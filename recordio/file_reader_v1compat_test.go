// this file exists for backward compatibility with the V1 files
// is basically a 1:1 copy of file_reader_test, which has additional tests and goes to the different folder
package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReaderHappyPathSingleRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer reader.Close()

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	assert.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathMultiRecordSnappyCompressedV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordCompressedV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipAllMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
	assert.Nil(t, err)
	defer reader.Close()

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		assert.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderV1VersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 0"))
}

func TestReaderV1VersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 256"))
}

func TestReaderCompressionGzipHeaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderCompressionUnknownV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp3", t)
	expectErrorOnOpen(t, reader, errors.New("unknown compression type [3]"))
}

func TestReaderMagicNumberMismatchV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer reader.Close()
	assert.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), err)
}

func TestReaderForbidsClosedReaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	assert.Nil(t, err)
	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("reader was either not opened yet or is closed already"), err)
	err = reader.SkipNext()
	assert.Equal(t, errors.New("reader was either not opened yet or is closed already"), err)
	err = reader.Open()
	assert.Equal(t, errors.New("already closed"), err)
}

func TestReaderForbidsDoubleOpensV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	assert.Nil(t, err)
	expectErrorOnOpen(t, reader, errors.New("already opened"))
}
