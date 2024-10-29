package recordio

import (
	"errors"
	"fmt"
	"io"
	"os"

	pool "github.com/libp2p/go-buffer-pool"
)

type FileReader struct {
	open   bool
	closed bool

	currentOffset uint64
	file          *os.File
	header        *Header
	reader        ByteReaderResetCount
	bufferPool    *pool.BufferPool
}

func (r *FileReader) Open() error {
	if r.open {
		return fmt.Errorf("file reader for '%s' is already opened", r.file.Name())
	}

	if r.closed {
		return fmt.Errorf("file reader for '%s' is already closed", r.file.Name())
	}

	// try to read the file header
	bytes := make([]byte, FileHeaderSizeBytes)
	numRead, err := io.ReadFull(r.reader, bytes)
	if err != nil {
		return fmt.Errorf("error while reading header bytes of '%s': %w", r.file.Name(), err)
	}

	if numRead != len(bytes) {
		return fmt.Errorf("not enough bytes found in the header, expected %d but were %d", len(bytes), numRead)
	}

	r.header, err = readFileHeaderFromBuffer(bytes)
	if err != nil {
		return fmt.Errorf("error while parsing header of '%s': %w", r.file.Name(), err)
	}

	r.currentOffset = uint64(len(bytes))

	r.bufferPool = new(pool.BufferPool)
	r.open = true

	return nil
}

func (r *FileReader) ReadNext() ([]byte, error) {
	if !r.open || r.closed {
		return nil, fmt.Errorf("file reader for '%s' was either not opened yet or is closed already", r.file.Name())
	}

	if r.header.fileVersion == Version1 {
		return readNextV1(r)
	} else if r.header.fileVersion == Version2 {
		return readNextV2(r)
	} else {
		start := r.reader.Count()
		payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := readRecordHeaderV3(r.reader)
		if err != nil {
			// due to the use of blocked writes in DirectIO, we need to test whether the remainder of the file contains only zeros.
			// This would indicate a properly written file and the actual end - and not a malformed record.
			if errors.Is(err, MagicNumberMismatchErr) {
				remainder, err := io.ReadAll(r.reader)
				if err != nil {
					return nil, fmt.Errorf("error while parsing record header seeking for file end of '%s': %w", r.file.Name(), err)
				}
				for _, b := range remainder {
					if b != 0 {
						return nil, fmt.Errorf("error while parsing record header for zeros towards the file end of '%s': %w", r.file.Name(), MagicNumberMismatchErr)
					}
				}

				// no other bytes than zeros have been read so far, that must've been the valid end of the file.
				return nil, io.EOF
			}

			return nil, fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
		}

		if recordNil {
			r.currentOffset = r.currentOffset + (r.reader.Count() - start)
			return nil, nil
		}

		expectedBytesRead, pooledRecordBuffer := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
		defer r.bufferPool.Put(pooledRecordBuffer)

		numRead, err := io.ReadFull(r.reader, pooledRecordBuffer)
		if err != nil {
			return nil, fmt.Errorf("error while reading into record buffer of '%s': %w", r.file.Name(), err)
		}

		if uint64(numRead) != expectedBytesRead {
			return nil, fmt.Errorf("not enough bytes in the record of '%s' found, expected %d but were %d", r.file.Name(), expectedBytesRead, numRead)
		}

		var returnSlice []byte
		if r.header.compressor != nil {
			pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
			defer r.bufferPool.Put(pooledDecompressionBuffer)

			decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuffer, pooledDecompressionBuffer)
			if err != nil {
				return nil, err
			}
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(decompressedRecord))
			copy(returnSlice, decompressedRecord)
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuffer))
			copy(returnSlice, pooledRecordBuffer)
		}

		// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
		r.currentOffset = r.currentOffset + (r.reader.Count() - start)
		return returnSlice, nil
	}
}

func (r *FileReader) SkipNext() error {
	if !r.open || r.closed {
		return fmt.Errorf("file reader for '%s' was either not opened yet or is closed already", r.file.Name())
	}

	if r.header.fileVersion == Version1 {
		return SkipNextV1(r)
	} else if r.header.fileVersion == Version2 {
		return SkipNextV2(r)
	} else {
		start := r.reader.Count()
		payloadSizeUncompressed, payloadSizeCompressed, _, err := readRecordHeaderV3(r.reader)
		if err != nil {
			return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
		}

		expectedBytesSkipped := payloadSizeUncompressed
		if r.header.compressor != nil {
			expectedBytesSkipped = payloadSizeCompressed
		}

		// here we have to add the header to the offset too, otherwise we will seek not far enough
		expectedOffset := int64(r.currentOffset + expectedBytesSkipped + (r.reader.Count() - start))
		newOffset, err := r.file.Seek(expectedOffset, 0)
		if err != nil {
			return fmt.Errorf("error while seeking to offset %d in '%s': %w", expectedOffset, r.file.Name(), err)
		}

		if newOffset != expectedOffset {
			return fmt.Errorf("seeking in '%s' did not return expected offset %d, it was %d", r.file.Name(), expectedOffset, newOffset)
		}

		r.reader.Reset(r.file)
		r.currentOffset = uint64(newOffset)
	}

	return nil
}

// SkipNextV1 is legacy support path for non-vint compressed V1
func SkipNextV1(r *FileReader) error {
	headerBuf := r.bufferPool.Get(RecordHeaderSizeBytesV1V2)
	defer r.bufferPool.Put(headerBuf)

	numRead, err := io.ReadFull(r.reader, headerBuf)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if numRead != RecordHeaderSizeBytesV1V2 {
		return fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytesV1V2, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}

	expectedBytesSkipped := payloadSizeUncompressed
	if r.header.compressor != nil {
		expectedBytesSkipped = payloadSizeCompressed
	}

	expectedOffset := int64(r.currentOffset + expectedBytesSkipped)
	newOffset, err := r.file.Seek(expectedOffset, 0)
	if err != nil {
		return fmt.Errorf("error while seeking to offset %d in '%s': %w", expectedOffset, r.file.Name(), err)
	}

	if newOffset != expectedOffset {
		return fmt.Errorf("seeking in '%s' did not return expected offset %d, it was %d", r.file.Name(), expectedOffset, newOffset)
	}

	// reset the buffered reader after the seek
	r.reader.Reset(r.file)

	r.currentOffset = r.currentOffset + expectedBytesSkipped
	return nil
}

func SkipNextV2(r *FileReader) error {
	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	expectedBytesSkipped := payloadSizeUncompressed
	if r.header.compressor != nil {
		expectedBytesSkipped = payloadSizeCompressed
	}

	// here we have to add the header to the offset too, otherwise we will seek not far enough
	expectedOffset := int64(r.currentOffset + expectedBytesSkipped + (r.reader.Count() - start))
	newOffset, err := r.file.Seek(expectedOffset, 0)
	if err != nil {
		return fmt.Errorf("error while seeking to offset %d in '%s': %w", expectedOffset, r.file.Name(), err)
	}

	if newOffset != expectedOffset {
		return fmt.Errorf("seeking in '%s' did not return expected offset %d, it was %d", r.file.Name(), expectedOffset, newOffset)
	}

	r.reader.Reset(r.file)
	r.currentOffset = uint64(newOffset)
	return nil
}

func (r *FileReader) Close() error {
	r.closed = true
	r.open = false
	return r.file.Close()
}

// legacy support path for non-vint compressed V1
func readNextV1(r *FileReader) ([]byte, error) {
	headerBuf := r.bufferPool.Get(RecordHeaderSizeBytesV1V2)
	defer r.bufferPool.Put(headerBuf)

	numRead, err := io.ReadFull(r.reader, headerBuf)
	if err != nil {
		return nil, fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if numRead != RecordHeaderSizeBytesV1V2 {
		return nil, fmt.Errorf("not enough bytes in the record header of '%s' found, expected %d but were %d", r.file.Name(), RecordHeaderSizeBytesV1V2, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}

	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = io.ReadFull(r.reader, recordBuffer)
	if err != nil {
		return nil, fmt.Errorf("error while reading into record buffer of '%s': %w", r.file.Name(), err)
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record found of '%s', expected %d but were %d", r.file.Name(), expectedBytesRead, numRead)
	}

	if r.header.compressor != nil {
		recordBuffer, err = r.header.compressor.Decompress(recordBuffer)
		if err != nil {
			return nil, fmt.Errorf("error while decompressing record of '%s': %w", r.file.Name(), err)
		}
	}

	r.currentOffset = r.currentOffset + expectedBytesRead
	return recordBuffer, nil
}

func readNextV2(r *FileReader) ([]byte, error) {
	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
	if err != nil {
		// due to the use of blocked writes in DirectIO, we need to test whether the remainder of the file contains only zeros.
		// This would indicate a properly written file and the actual end - and not a malformed record.
		if errors.Is(err, MagicNumberMismatchErr) {
			remainder, err := io.ReadAll(r.reader)
			if err != nil {
				return nil, fmt.Errorf("error while parsing record header seeking for file end of '%s': %w", r.file.Name(), err)
			}
			for _, b := range remainder {
				if b != 0 {
					return nil, fmt.Errorf("error while parsing record header for zeros towards the file end of '%s': %w", r.file.Name(), MagicNumberMismatchErr)
				}
			}

			// no other bytes than zeros have been read so far, that must've been the valid end of the file.
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}

	expectedBytesRead, pooledRecordBuffer := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
	defer r.bufferPool.Put(pooledRecordBuffer)

	numRead, err := io.ReadFull(r.reader, pooledRecordBuffer)
	if err != nil {
		return nil, fmt.Errorf("error while reading into record buffer of '%s': %w", r.file.Name(), err)
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record of '%s' found, expected %d but were %d", r.file.Name(), expectedBytesRead, numRead)
	}

	var returnSlice []byte
	if r.header.compressor != nil {
		pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
		defer r.bufferPool.Put(pooledDecompressionBuffer)

		decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuffer, pooledDecompressionBuffer)
		if err != nil {
			return nil, err
		}
		if decompressedRecord == nil {
			returnSlice = nil
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(decompressedRecord))
			copy(returnSlice, decompressedRecord)
		}
	} else {
		if pooledRecordBuffer == nil {
			returnSlice = nil
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuffer))
			copy(returnSlice, pooledRecordBuffer)
		}
	}

	// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
	r.currentOffset = r.currentOffset + (r.reader.Count() - start)
	return returnSlice, nil
}

// TODO(thomas): we have to add an option pattern here as well

// NewFileReaderWithPath creates a new recordio file reader that can read RecordIO files at the given path.
func NewFileReaderWithPath(path string) (ReaderI, error) {
	return newFileReaderWithFactory(path, BufferedIOFactory{})
}

// NewFileReaderWithFile creates a new recordio file reader that can read RecordIO files with the given file.
// The file will be managed from here on out (ie closing).
func NewFileReaderWithFile(file *os.File) (ReaderI, error) {
	// we're closing the existing file, as it's being recreated by the factory below
	err := file.Close()
	if err != nil {
		return nil, fmt.Errorf("error while closing existing file handle at '%s': %w", file.Name(), err)
	}

	return newFileReaderWithFactory(file.Name(), BufferedIOFactory{})
}

// NewFileReaderWithFactory creates a new recordio file reader under a path and a given ReaderWriterCloserFactory.
func newFileReaderWithFactory(path string, factory ReaderWriterCloserFactory) (ReaderI, error) {
	f, r, err := factory.CreateNewReader(path, DefaultBufferSize)
	if err != nil {
		return nil, err
	}

	return &FileReader{
		file:          f,
		reader:        r,
		open:          false,
		closed:        false,
		currentOffset: 0,
	}, nil
}
