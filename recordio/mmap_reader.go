package recordio

import (
	"errors"
	"golang.org/x/exp/mmap"
	"fmt"
)

type MMapReader struct {
	mmapReader *mmap.ReaderAt
	header     *Header
	open       bool
	closed     bool
}

func (r *MMapReader) Open() error {
	if r.open {
		return errors.New("already opened")
	}

	if r.closed {
		return errors.New("already closed")
	}

	buf := make([]byte, 8)
	numRead, err := r.mmapReader.ReadAt(buf, 0)
	if err != nil {
		return err
	}
	if numRead != len(buf) {
		return fmt.Errorf("not enough bytes in the header found, expected %d but were %d", len(buf), numRead)
	}

	header, err := readFileHeaderFromBuffer(buf)
	if err != nil {
		return err
	}

	r.header = header
	r.open = true
	return nil
}

func (r *MMapReader) ReadNextAt(offset uint64) ([]byte, error) {
	if !r.open || r.closed {
		return nil, errors.New("reader was either not opened yet or is closed already")
	}

	// TODO(thomas): here we can use a bpool of buffers (https://github.com/oxtoacart/bpool)
	buf := make([]byte, RecordHeaderSizeBytes)
	numRead, err := r.mmapReader.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}

	if numRead != len(buf) {
		return nil, fmt.Errorf("not enough bytes in the header found, expected %d but were %d", len(buf), numRead)
	}

	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeader(buf)
	if err != nil {
		return nil, err
	}

	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = r.mmapReader.ReadAt(recordBuffer, int64(offset+RecordHeaderSizeBytes))
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

	return recordBuffer, nil
}

func (r *MMapReader) Close() error {
	r.closed = true
	r.open = false
	return r.mmapReader.Close()
}

func NewMemoryMappedReaderWithPath(path string) (*MMapReader, error) {
	mmapReaderAt, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	return &MMapReader{mmapReader: mmapReaderAt}, nil
}
