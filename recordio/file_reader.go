package recordio

import (
	"bufio"
	"errors"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"io"
	"os"
	"strconv"
)

type FileReader struct {
	currentOffset uint64
	file          *os.File
	header        *Header
	open          bool
	closed        bool
	reader        *CountingBufferedReader
	bufferPool    *pool.BufferPool
}

func (r *FileReader) Open() error {
	if r.open {
		return errors.New("already opened")
	}

	if r.closed {
		return errors.New("already closed")
	}

	// make sure we are at the start of the file
	newOffset, err := r.file.Seek(0, 0)
	if err != nil {
		return err
	}

	if newOffset != 0 {
		return errors.New("seek did not return offset 0, it was: " + strconv.FormatInt(newOffset, 10))
	}

	r.reader = NewCountingByteReader(bufio.NewReaderSize(r.file, DefaultBufferSize))

	// try to read the file header
	bytes := make([]byte, FileHeaderSizeBytes)
	numRead, err := io.ReadFull(r.reader, bytes)
	if err != nil {
		return err
	}

	if numRead != len(bytes) {
		return fmt.Errorf("not enough bytes in the header found, expected %d but were %d", len(bytes), numRead)
	}

	r.header, err = readFileHeaderFromBuffer(bytes)
	if err != nil {
		return err
	}

	r.currentOffset = uint64(len(bytes))

	r.bufferPool = new(pool.BufferPool)
	r.open = true

	return nil
}

func (r *FileReader) ReadNext() ([]byte, error) {
	if !r.open || r.closed {
		return nil, errors.New("reader was either not opened yet or is closed already")
	}

	if r.header.fileVersion == Version1 {
		return readNextV1(r)
	} else {
		start := r.reader.count
		payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
		if err != nil {
			return nil, err
		}

		expectedBytesRead, pooledRecordBuffer := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
		numRead, err := io.ReadFull(r.reader, pooledRecordBuffer)
		if err != nil {
			return nil, err
		}

		if uint64(numRead) != expectedBytesRead {
			return nil, fmt.Errorf("not enough bytes in the record found, expected %d but were %d", expectedBytesRead, numRead)
		}

		var returnSlice []byte
		if r.header.compressor != nil {
			// TODO(thomas): with snappy we can also use a pool here
			returnSlice, err = r.header.compressor.Decompress(pooledRecordBuffer)
			if err != nil {
				return nil, err
			}
			r.bufferPool.Put(pooledRecordBuffer)
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuffer))
			copy(returnSlice, pooledRecordBuffer)
			r.bufferPool.Put(pooledRecordBuffer)
		}

		// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
		r.currentOffset = r.currentOffset + (r.reader.count - start)
		return returnSlice, nil
	}
}

func (r *FileReader) SkipNext() error {
	if !r.open || r.closed {
		return errors.New("reader was either not opened yet or is closed already")
	}

	if r.header.fileVersion == Version1 {
		return SkipNextV1(r)
	} else {
		start := r.reader.count
		payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(r.reader)
		if err != nil {
			return err
		}

		expectedBytesSkipped := payloadSizeUncompressed
		if r.header.compressor != nil {
			expectedBytesSkipped = payloadSizeCompressed
		}

		// here we have to add the header to the offset too, otherwise we will seek not far enough
		expectedOffset := int64(r.currentOffset + expectedBytesSkipped + (r.reader.count - start))
		newOffset, err := r.file.Seek(expectedOffset, 0)
		if err != nil {
			return err
		}

		if newOffset != expectedOffset {
			return errors.New("seek did not return expected offset, it was: " + strconv.FormatInt(newOffset, 10))
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
		return err
	}

	if numRead != RecordHeaderSizeBytes {
		return fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytes, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return nil
	}

	r.bufferPool.Put(headerBuf)

	expectedBytesSkipped := payloadSizeUncompressed
	if r.header.compressor != nil {
		expectedBytesSkipped = payloadSizeCompressed
	}

	expectedOffset := int64(r.currentOffset + expectedBytesSkipped)
	newOffset, err := r.file.Seek(expectedOffset, 0)
	if err != nil {
		return err
	}

	if newOffset != expectedOffset {
		return errors.New("seek did not return expected offset, it was: " + strconv.FormatInt(newOffset, 10))
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
		return nil, err
	}

	if numRead != RecordHeaderSizeBytes {
		return nil, fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytes, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBuf)
	if err != nil {
		return nil, err
	}
	r.bufferPool.Put(headerBuf)

	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = io.ReadFull(r.reader, recordBuffer)
	if err != nil {
		return nil, err
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record found, expected %d but were %d", expectedBytesRead, numRead)
	}

	if r.header.compressor != nil {
		recordBuffer, err = r.header.compressor.Decompress(recordBuffer)
		if err != nil {
			return nil, err
		}
	}

	r.currentOffset = r.currentOffset + expectedBytesRead
	return recordBuffer, nil
}

func NewFileReaderWithPath(path string) (*FileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewFileReaderWithFile(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewFileReaderWithFile(file *os.File) (*FileReader, error) {
	return &FileReader{
		file:          file,
		open:          false,
		closed:        false,
		currentOffset: 0,
	}, nil
}
