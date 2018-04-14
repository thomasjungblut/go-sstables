package recordio

import (
	"os"
	"errors"
	"strconv"
	"fmt"
	"encoding/binary"
)

type FileReader struct {
	headerBuffer []byte

	open            bool
	closed          bool
	file            *os.File
	currentOffset   uint64
	compressionType int
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
	// 4 byte version number, 4 byte compression code = 8 bytes
	bytes := make([]byte, 8)
	numRead, err := r.file.Read(bytes)
	if err != nil {
		return err
	}

	if numRead != len(bytes) {
		return fmt.Errorf("not enough bytes in the header found, expected %d but were %d", len(bytes), numRead)
	}

	fileVersion := binary.LittleEndian.Uint32(bytes[0:4])
	if fileVersion != Version {
		return fmt.Errorf("version mismatch, expected %d but was %d", Version, fileVersion)
	}

	compressionType := binary.LittleEndian.Uint32(bytes[4:8])
	if compressionType > CompressionTypeSnappy {
		return fmt.Errorf("unknown compression type [%d]", compressionType)
	}

	if compressionType != CompressionTypeNone {
		return fmt.Errorf("compression type %d is unsupported", compressionType)
	}

	r.compressionType = int(compressionType)
	r.currentOffset = uint64(len(bytes))
	r.headerBuffer = make([]byte, HeaderSizeBytes)
	r.open = true

	return nil
}

func readHeader(r *FileReader) (uint64, uint64, error) {
	numRead, err := r.file.Read(r.headerBuffer)
	if err != nil {
		return 0, 0, err
	}

	if numRead != len(r.headerBuffer) {
		return 0, 0, fmt.Errorf("not enough bytes in the record header found, expected %d but were %d", len(r.headerBuffer), numRead)
	}

	r.currentOffset = r.currentOffset + uint64(numRead)

	magicNumber := binary.LittleEndian.Uint32(r.headerBuffer[0:4])
	if magicNumber != MagicNumberSeparator {
		return 0, 0, fmt.Errorf("magic number mismatch")
	}

	payloadSizeUncompressed := binary.LittleEndian.Uint64(r.headerBuffer[4:12])
	if err != nil {
		return 0, 0, err
	}

	payloadSizeCompressed := binary.LittleEndian.Uint64(r.headerBuffer[12:20])
	if err != nil {
		return 0, 0, err
	}

	return payloadSizeUncompressed, payloadSizeCompressed, nil
}

func (r *FileReader) ReadNext() ([]byte, error) {
	if !r.open || r.closed {
		return nil, errors.New("reader was either not opened yet or is closed already")
	}

	payloadSizeUncompressed, _, err := readHeader(r)

	if err != nil {
		return nil, err
	}

	recordBuffer := make([]byte, payloadSizeUncompressed)
	numRead, err := r.file.Read(recordBuffer)
	if err != nil {
		return nil, err
	}

	if numRead != len(recordBuffer) {
		return nil, fmt.Errorf("not enough bytes in the record found, expected %d but were %d", len(recordBuffer), numRead)
	}

	r.currentOffset = r.currentOffset + uint64(len(recordBuffer))

	return recordBuffer, nil
}

func (r *FileReader) SkipNext() error {
	if !r.open || r.closed {
		return errors.New("reader was either not opened yet or is closed already")
	}

	payloadSizeUncompressed, _, err := readHeader(r)
	if err != nil {
		return nil
	}

	expectedOffset := int64(r.currentOffset + uint64(payloadSizeUncompressed))
	newOffset, err := r.file.Seek(expectedOffset, 0)
	if err != nil {
		return err
	}

	if newOffset != expectedOffset {
		return errors.New("seek did not return expected offset, it was: " + strconv.FormatInt(newOffset, 10))
	}

	r.currentOffset = r.currentOffset + uint64(payloadSizeUncompressed)

	return nil
}

func (r *FileReader) Close() error {
	r.closed = true
	r.open = false
	return r.file.Close()
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
		file:            file,
		open:            false,
		closed:          false,
		compressionType: CompressionTypeNone,
		currentOffset:   0,
	}, nil
}
