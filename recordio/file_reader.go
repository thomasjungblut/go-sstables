package recordio

import (
	"bufio"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"io"
	"os"
)

type FileReader struct {
	currentOffset uint64
	file          *os.File
	header        *Header
	open          bool
	closed        bool
	reader        CountingReaderResetComposite
	bufferPool    *pool.BufferPool
}

func (r *FileReader) Open() error {
	if r.open {
		return fmt.Errorf("file reader for '%s' is already opened", r.file.Name())
	}

	if r.closed {
		return fmt.Errorf("file reader for '%s' is already closed", r.file.Name())
	}

	// make sure we are at the start of the file
	newOffset, err := r.file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("error while seeking file '%s' to zero: %w", r.file.Name(), err)
	}

	if newOffset != 0 {
		return fmt.Errorf("seeking in '%s' did not return offset 0, it was %d", r.file.Name(), newOffset)
	}

	r.reader = NewCountingByteReader(bufio.NewReaderSize(r.file, DefaultBufferSize))

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
	} else {
		start := r.reader.Count()
		payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
		if err != nil {
			return nil, fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
		}

		expectedBytesRead, pooledRecordBuffer := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
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
			decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuffer, pooledDecompressionBuffer)
			if err != nil {
				return nil, err
			}
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(decompressedRecord))
			copy(returnSlice, decompressedRecord)
			r.bufferPool.Put(pooledRecordBuffer)
			r.bufferPool.Put(pooledDecompressionBuffer)
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuffer))
			copy(returnSlice, pooledRecordBuffer)
			r.bufferPool.Put(pooledRecordBuffer)
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
	} else {
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
	}

	return nil
}

// legacy support path for non-vint compressed V1
func SkipNextV1(r *FileReader) error {
	headerBuf := r.bufferPool.Get(RecordHeaderSizeBytes)
	numRead, err := io.ReadFull(r.reader, headerBuf)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if numRead != RecordHeaderSizeBytes {
		return fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytes, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}

	r.bufferPool.Put(headerBuf)

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

func (r *FileReader) Close() error {
	r.closed = true
	r.open = false
	return r.file.Close()
}

// legacy support path for non-vint compressed V1
func readNextV1(r *FileReader) ([]byte, error) {
	headerBuf := r.bufferPool.Get(RecordHeaderSizeBytes)
	numRead, err := io.ReadFull(r.reader, headerBuf)
	if err != nil {
		return nil, fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if numRead != RecordHeaderSizeBytes {
		return nil, fmt.Errorf("not enough bytes in the record header of '%s' found, expected %d but were %d", r.file.Name(), RecordHeaderSizeBytes, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}
	r.bufferPool.Put(headerBuf)

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

// NewFileReaderWithPath creates a new recordio file reader that can read RecordIO files at the given path.
func NewFileReaderWithPath(path string) (ReaderI, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error while opening recordio '%s': %w", path, err)
	}

	r, err := NewFileReaderWithFile(f)
	if err != nil {
		return nil, fmt.Errorf("error while creating new recordio '%s': %w", path, err)
	}

	return r, nil
}

// NewFileReaderWithPath creates a new recordio file reader that can read RecordIO files with the given file.
// The file will be managed from here on out (ie closing).
func NewFileReaderWithFile(file *os.File) (ReaderI, error) {
	return &FileReader{
		file:          file,
		open:          false,
		closed:        false,
		currentOffset: 0,
	}, nil
}
