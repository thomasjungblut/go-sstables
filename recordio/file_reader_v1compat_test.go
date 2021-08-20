// this file exists for backward compatibility with the V1 files
// is basically a 1:1 copy of file_reader_test, which has additional tests and goes to the different folder
package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReaderHappyPathSingleRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathMultiRecordSnappyCompressedV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordCompressedV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipAllMultiRecordV1(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		require.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderV1VersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 2 but was 0")
}

func TestReaderV1VersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 2 but was 256")
}

func TestReaderCompressionGzipHeaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderCompressionUnknownV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp3", t)
	expectErrorStringOnOpen(t, reader, "unknown compression type [3]")
}

func TestReaderMagicNumberMismatchV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestReaderForbidsClosedReaderV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.Nil(t, err)
	_, err = reader.ReadNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.SkipNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "already closed")
}

func TestReaderForbidsDoubleOpensV1(t *testing.T) {
	reader := newTestReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.Nil(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}
