// this file exists for backward compatibility with the V2 files
// is basically a 1:1 copy of file_reader_test, which has additional tests and goes to the different folder
package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestReaderHappyPathSingleRecordV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathMultiRecordSnappyCompressedV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipMultiRecordCompressedV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_SnappyWriterMultiRecord_asc")
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

func TestReaderHappyPathSkipAllMultiRecordV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.Nil(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		require.Nil(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderV3VersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 4 but was 0")
}

func TestReaderV3VersionMismatchV356(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 4 but was 256")
}

func TestReaderCompressionGzipHeaderV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeaderV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderMagicNumberMismatchV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.Nil(t, err)

	_, err = reader.ReadNext()
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestReaderDirectIOV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_directio", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.NoError(t, err)

	record, err := reader.ReadNext()
	require.NoError(t, err)
	assert.Equal(t, []byte{13, 06, 29, 07}, record)

	_, err = reader.ReadNext()
	require.ErrorIs(t, err, io.EOF)
}

func TestReaderDirectIOTrailerV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_directio_trailer", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.NoError(t, err)

	record, err := reader.ReadNext()
	require.NoError(t, err)
	assert.Equal(t, []byte{13, 06, 29, 07}, record)

	_, err = reader.ReadNext()
	assert.ErrorIs(t, err, MagicNumberMismatchErr)
}

func TestReaderHappyPathMagicNumberContentV3(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedMagicNumberContent")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	buf, err := reader.ReadNext()
	require.NoError(t, err)
	require.Equal(t, MagicNumberSeparatorLongBytes, buf)
	buf, err = reader.ReadNext()
	require.NoError(t, err)
	require.Equal(t, []byte{21, 8, 23}, buf)
	buf, err = reader.ReadNext()
	require.NoError(t, err)
	require.Equal(t, MagicNumberSeparatorLongBytes, buf)

	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderForbidsClosedReaderV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.Nil(t, err)
	_, err = reader.ReadNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.SkipNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "already closed")
}

func TestReaderForbidsDoubleOpensV3(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.Nil(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}
