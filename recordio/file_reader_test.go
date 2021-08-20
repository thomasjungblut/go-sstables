package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		require.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordSnappyCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_SnappyWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		require.Nil(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			require.Nil(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			require.Nil(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecordCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_SnappyWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			require.Nil(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			require.Nil(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipAllMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		require.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderVersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 2 but was 0")
}

func TestReaderVersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 2 but was 256")
}

func TestReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderCompressionUnknown(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp3", t)
	expectErrorStringOnOpen(t, reader, "unknown compression type [3]")
}

func TestReaderMagicNumberMismatch(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestReaderForbidsClosedReader(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.Nil(t, err)
	_, err = reader.ReadNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.SkipNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "is already closed")
}

func TestReaderForbidsDoubleOpens(t *testing.T) {
	reader := newTestReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.Nil(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}

func expectErrorStringOnOpen(t *testing.T, reader OpenClosableI, expectedError string) {
	err := reader.Open()
	defer closeOpenClosable(t, reader)
	assert.Contains(t, err.Error(), expectedError)
}

func newOpenedTestReader(t *testing.T, file string) (*FileReader, error) {
	reader := newTestReader(file, t)
	err := reader.Open()
	require.Nil(t, err)
	return reader, err
}

func newTestReader(file string, t *testing.T) *FileReader {
	r, err := NewFileReaderWithPath(file)
	require.Nil(t, err)
	return r.(*FileReader)
}
