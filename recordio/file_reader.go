package recordio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	pool "capnproto.org/go/capnp/v3/exp/bufferpool"
)

type FileReader struct {
	open   bool
	closed bool

	currentOffset uint64
	file          *os.File
	header        *Header
	reader        ByteReaderResetCount
	bufferPool    *pool.Pool

	recordHeaderCache      []byte
	recordHeaderByteReader *checksumByteReader
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

	r.bufferPool = pool.NewPool(1024, 20)
	r.recordHeaderCache = r.bufferPool.Get(RecordHeaderV4MaxSizeBytes)
	r.recordHeaderByteReader = newChecksumByteReader(r.reader, r.recordHeaderCache)
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
	} else if r.header.fileVersion == Version3 {
		return readNextV3(r)
	} else {
		start := r.reader.Count()
		payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := r.readRecordHeaderV4()
		if err != nil {
			// files written with DirectIO by earlier versions are padded with zeros, we need to test whether the remainder of the
			// file contains only zeros.
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

		var recordBuffer []byte
		if r.header.compressor == nil {
			// the buffer is returned to the caller directly, pooling would only add a zeroing and a defensive copy
			recordBuffer = make([]byte, payloadSizeUncompressed)
		} else {
			recordBuffer = r.bufferPool.Get(int(payloadSizeCompressed))
			defer r.bufferPool.Put(recordBuffer)
		}

		numRead, err := io.ReadFull(r.reader, recordBuffer)
		if err != nil {
			return nil, fmt.Errorf("error while reading into record buffer of '%s': %w", r.file.Name(), err)
		}

		if numRead != len(recordBuffer) {
			return nil, fmt.Errorf("not enough bytes in the record of '%s' found, expected %d but were %d", r.file.Name(), len(recordBuffer), numRead)
		}

		// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
		r.currentOffset = r.currentOffset + (r.reader.Count() - start)
		if r.header.compressor != nil {
			pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
			defer r.bufferPool.Put(pooledDecompressionBuffer)

			buf, err := r.header.compressor.DecompressWithBuf(recordBuffer, pooledDecompressionBuffer)
			if err != nil {
				return nil, err
			}

			return bytes.Clone(buf), nil
		}

		return recordBuffer, nil
	}
}

// readRecordHeaderV4 decodes the record header straight from the read buffer when possible. Headers that aren't fully
// buffered (e.g. across buffer boundaries or at the end of the file) are read byte by byte, which refills the buffer.
func (r *FileReader) readRecordHeaderV4() (payloadSizeUncompressed uint64, payloadSizeCompressed uint64, recordNil bool, err error) {
	buf := r.reader.PeekBuffered(RecordHeaderV4MaxSizeBytes)
	var n int
	payloadSizeUncompressed, payloadSizeCompressed, recordNil, n, err = readRecordHeaderV4FromSlice(buf)
	if err == nil || errors.Is(err, MagicNumberMismatchErr) {
		// consume exactly what the byte by byte path would have, the zero-trailer check relies on it
		r.reader.DiscardBuffered(n)
		return payloadSizeUncompressed, payloadSizeCompressed, recordNil, err
	}

	truncated := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if !truncated || len(buf) == RecordHeaderV4MaxSizeBytes {
		return 0, 0, false, err
	}

	return readRecordHeaderV4(r.recordHeaderByteReader)
}

func (r *FileReader) SkipNext() error {
	if !r.open || r.closed {
		return fmt.Errorf("file reader for '%s' was either not opened yet or is closed already", r.file.Name())
	}

	if r.header.fileVersion == Version1 {
		return SkipNextV1(r)
	} else if r.header.fileVersion == Version2 {
		return SkipNextV2(r)
	} else if r.header.fileVersion == Version3 {
		return SkipNextV3(r)
	} else {
		start := r.reader.Count()
		payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := r.readRecordHeaderV4()
		if err != nil {
			return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
		}

		if recordNil {
			// nil records have no payload, even though a compressed size may be recorded
			return r.skipPayload(start, 0, 0)
		}
		return r.skipPayload(start, payloadSizeUncompressed, payloadSizeCompressed)
	}
}

// skipPayload skips the payload of the record whose header was read starting at reader count start. Payloads that are
// fully buffered are discarded from the buffer, only larger ones seek in the file and reset the buffer.
func (r *FileReader) skipPayload(start uint64, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) error {
	expectedBytesSkipped := payloadSizeUncompressed
	if r.header.compressor != nil {
		expectedBytesSkipped = payloadSizeCompressed
	}

	// here we have to add the header to the offset too, otherwise we will seek not far enough
	headerBytes := r.reader.Count() - start
	if expectedBytesSkipped <= uint64(r.reader.Buffered()) {
		r.reader.DiscardBuffered(int(expectedBytesSkipped))
		r.currentOffset = r.currentOffset + headerBytes + expectedBytesSkipped
		return nil
	}

	expectedOffset := int64(r.currentOffset + expectedBytesSkipped + headerBytes)
	newOffset, err := r.file.Seek(expectedOffset, io.SeekStart)
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

// SkipNextV1 is legacy support path for non-vint compressed V1
func SkipNextV1(r *FileReader) error {
	start := r.reader.Count()
	headerBuf := r.bufferPool.Get(RecordHeaderSizeBytesV1V2)
	defer r.bufferPool.Put(headerBuf)

	numRead, err := io.ReadFull(r.reader, headerBuf)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if numRead != RecordHeaderSizeBytesV1V2 {
		return fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytesV1V2, numRead)
	}

	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return fmt.Errorf("error while parsing record header of '%s': %w", r.file.Name(), err)
	}

	return r.skipPayload(start, payloadSizeUncompressed, payloadSizeCompressed)
}

func SkipNextV2(r *FileReader) error {
	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	return r.skipPayload(start, payloadSizeUncompressed, payloadSizeCompressed)
}

func SkipNextV3(r *FileReader) error {
	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := readRecordHeaderV3(r.reader)
	if err != nil {
		return fmt.Errorf("error while reading record header of '%s': %w", r.file.Name(), err)
	}

	if recordNil {
		// nil records have no payload, even though a compressed size may be recorded
		return r.skipPayload(start, 0, 0)
	}
	return r.skipPayload(start, payloadSizeUncompressed, payloadSizeCompressed)
}

func (r *FileReader) Close() error {
	if r.recordHeaderCache != nil {
		r.bufferPool.Put(r.recordHeaderCache)
	}
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
		// files written with DirectIO by earlier versions are padded with zeros, we need to test whether the remainder of the
		// file contains only zeros.
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

func readNextV3(r *FileReader) ([]byte, error) {
	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := readRecordHeaderV3(r.reader)
	if err != nil {
		// files written with DirectIO by earlier versions are padded with zeros, we need to test whether the remainder of the
		// file contains only zeros.
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

	// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
	r.currentOffset = r.currentOffset + (r.reader.Count() - start)
	if r.header.compressor != nil {
		pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
		defer r.bufferPool.Put(pooledDecompressionBuffer)

		buf, err := r.header.compressor.DecompressWithBuf(pooledRecordBuffer, pooledDecompressionBuffer)
		if err != nil {
			return nil, err
		}

		return bytes.Clone(buf), nil
	}

	// TODO(thomas): copyBuf is a huge performance bottleneck, just returning the pooled buffer will
	// immediately unlock 1.5x-2x more throughput
	return bytes.Clone(pooledRecordBuffer), nil
}

// options
type FileReaderOptions struct {
	path            string
	file            *os.File
	bufferSizeBytes int
	factory         IOFactory
}

type FileReaderOption func(*FileReaderOptions)

// ReaderPath defines the file path where to read the recordio file
// Either this or File must be supplied.
func ReaderPath(p string) FileReaderOption {
	return func(args *FileReaderOptions) {
		args.path = p
	}
}

// ReaderFile uses the given os.File as the sink to write into. The code manages the given file lifecycle (ie closing).
// Either this or Path must be supplied
func ReaderFile(p *os.File) FileReaderOption {
	return func(args *FileReaderOptions) {
		args.file = p
	}
}

// BufferSizeBytes sets the IoFactory, by default it uses BufferedIOFactory.
func ReaderIoFactory(factory IOFactory) FileReaderOption {
	return func(args *FileReaderOptions) {
		args.factory = factory
	}
}

// set Factory , by default it uses DefaultBufferSize.
// This is the internal memory buffer before it's written to disk.
func ReaderBufferSizeBytes(p int) FileReaderOption {
	return func(args *FileReaderOptions) {
		args.bufferSizeBytes = p
	}
}

// NewFileReader creates a new reader with the given options, either Path or File must be supplied, compression is optional.
func NewFileReader(readerOptions ...FileReaderOption) (ReaderI, error) {
	opts := &FileReaderOptions{
		path:            "",
		file:            nil,
		bufferSizeBytes: DefaultBufferSize,
		factory:         BufferedIOFactory{},
	}

	for _, readOption := range readerOptions {
		readOption(opts)
	}

	if (opts.file == nil) == (opts.path == "") {
		return nil, errors.New("NewFileReader: either os.File or string path must be supplied, never both")
	}

	f, r, err := opts.factory.CreateNewReader(opts.path, opts.bufferSizeBytes)
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

// NewFileReaderWithPath creates a new recordio file reader that can read RecordIO files at the given path.
func NewFileReaderWithPath(path string) (ReaderI, error) {
	return NewFileReader(ReaderPath(path))
}

// NewFileReaderWithFile creates a new recordio file reader that can read RecordIO files with the given file.
// The file will be managed from here on out (ie closing).
func NewFileReaderWithFile(file *os.File) (ReaderI, error) {
	// we're closing the existing file, as it's being recreated by the factory below
	err := file.Close()
	if err != nil {
		return nil, fmt.Errorf("error while closing existing file handle at '%s': %w", file.Name(), err)
	}

	return NewFileReader(ReaderPath(file.Name()))
}
