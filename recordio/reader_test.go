package recordio

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"errors"
)

func TestReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer reader.Close()

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	assert.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		assert.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordSnappyCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/recordio_SnappyWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		assert.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

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
	reader, err := newOpenedTestReader(t, "test_files/recordio_SnappyWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

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
	reader, err := newOpenedTestReader(t, "test_files/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		assert.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderVersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_v0", t)
	err := reader.Open()
	defer reader.Close()
	assert.Equal(t, errors.New("version mismatch, expected 1 but was 0"), err)
}

func TestReaderVersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_v256", t)
	err := reader.Open()
	defer reader.Close()
	assert.Equal(t, errors.New("version mismatch, expected 1 but was 256"), err)
}

func TestReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, 1, reader.compressionType)
}

func TestReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, 2, reader.compressionType)
}

func TestReaderCompressionUnknown(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_comp3", t)
	err := reader.Open()
	defer reader.Close()
	assert.Equal(t, errors.New("unknown compression type [3]"), err)
}

func TestReaderMagicNumberMismatch(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer reader.Close()
	assert.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), err)
}

func TestReaderForbidsClosedReader(t *testing.T) {
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord", t)
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
	reader := newTestReader("test_files/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	assert.Nil(t, err)
	err = reader.Open()
	assert.Equal(t, errors.New("already opened"), err)
}

func newOpenedTestReader(t *testing.T, file string) (*FileReader, error) {
	reader := newTestReader(file, t)
	err := reader.Open()
	assert.Nil(t, err)
	return reader, err
}

func newTestReader(file string, t *testing.T) (*FileReader) {
	r, err := NewFileReaderWithPath(file)
	assert.Nil(t, err)
	return r
}
