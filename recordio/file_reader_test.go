package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	assert.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		assert.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordSnappyCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_SnappyWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		assert.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			assert.Nil(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			assert.Nil(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecordCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_SnappyWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			assert.Nil(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			assert.Nil(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipAllMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		assert.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderVersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 0"))
}

func TestReaderVersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 256"))
}

func TestReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderCompressionUnknown(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp3", t)
	expectErrorOnOpen(t, reader, errors.New("unknown compression type [3]"))
}

func TestReaderMagicNumberMismatch(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	assert.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), err)
}

func TestReaderForbidsClosedReader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	assert.Nil(t, err)
	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("reader was either not opened yet or is closed already"), err)
	err = reader.SkipNext()
	assert.Equal(t, errors.New("reader was either not opened yet or is closed already"), err)
	err = reader.Open()
	assert.Equal(t, errors.New("already closed"), err)
}

func TestReaderForbidsDoubleOpens(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	assert.Nil(t, err)
	expectErrorOnOpen(t, reader, errors.New("already opened"))
}

func expectErrorOnOpen(t *testing.T, reader OpenClosableI, expectedError error) {
	err := reader.Open()
	defer closeOpenClosable(t, reader)
	assert.Equal(t, expectedError, err)
}

func newOpenedTestReader(t *testing.T, file string) (*FileReader, error) {
	reader := newTestReader(file, t)
	err := reader.Open()
	assert.Nil(t, err)
	return reader, err
}

func newTestReader(file string, t *testing.T) *FileReader {
	r, err := NewFileReaderWithPath(file)
	assert.Nil(t, err)
	return r
}
