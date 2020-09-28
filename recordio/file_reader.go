package recordio

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type FileReader struct {
	recordHeaderBuffer []byte
	currentOffset      uint64
	file               *os.File
	header             *Header
	open               bool
	closed             bool
	reader             *CountingBufferedReader
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

	// try to read the file header
	bytes := make([]byte, FileHeaderSizeBytes)
	numRead, err := r.file.Read(bytes)
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
	r.recordHeaderBuffer = make([]byte, RecordHeaderSizeBytes)

	r.reader = NewCountingByteReader(bufio.NewReader(r.file))
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

		expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
		numRead, err := io.ReadFull(r.reader, recordBuffer)
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

		// why not just r.currentOffset = r.reader.count? we could've skipped something in between which makes the counts inconsistent
		r.currentOffset = r.currentOffset + (r.reader.count - start)
		return recordBuffer, nil
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
	numRead, err := r.file.Read(r.recordHeaderBuffer)
	if err != nil {
		return err
	}

	if numRead != RecordHeaderSizeBytes {
		return fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytes, numRead)
	}
	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(r.recordHeaderBuffer)
	if err != nil {
		return nil
	}

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
	numRead, err := r.file.Read(r.recordHeaderBuffer)
	if err != nil {
		return nil, err
	}

	if numRead != RecordHeaderSizeBytes {
		return nil, fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", RecordHeaderSizeBytes, numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(r.recordHeaderBuffer)
	if err != nil {
		return nil, err
	}

	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = r.file.Read(recordBuffer)
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
