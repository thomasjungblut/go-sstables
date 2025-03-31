package recordio

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNext()
	require.NoError(t, err)
	assertAscendingBytes(t, buf, 13)
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		require.NoError(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathMultiRecordSnappyCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_SnappyWriterMultiRecord_asc")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		buf, err := reader.ReadNext()
		require.NoError(t, err)
		assertAscendingBytes(t, buf, expectedLen)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			require.NoError(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			require.NoError(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipMultiRecordCompressed(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_SnappyWriterMultiRecord_asc")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		if expectedLen%2 == 0 {
			buf, err := reader.ReadNext()
			require.NoError(t, err)
			assertAscendingBytes(t, buf, expectedLen)
		} else {
			err = reader.SkipNext()
			require.NoError(t, err)
		}
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderHappyPathSkipAllMultiRecord(t *testing.T) {
	reader, err := newOpenedTestReader(t, "test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc")
	require.NoError(t, err)
	defer closeFileReader(t, reader)

	for expectedLen := 0; expectedLen < 255; expectedLen++ {
		err = reader.SkipNext()
		require.NoError(t, err)
	}
	// next read should yield EOF
	readNextExpectEOF(t, reader)
}

func TestReaderVersionMismatchV0(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 0")
}

func TestReaderVersionMismatchV256(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 256")
}

func TestReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.NoError(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.NoError(t, err)
	defer closeFileReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestReaderCompressionUnknown(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp300", t)
	expectErrorStringOnOpen(t, reader, "unknown compression type [300]")
}

func TestReaderMagicNumberMismatch(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord_mnm", t)
	err := reader.Open()
	defer closeFileReader(t, reader)
	require.NoError(t, err)

	_, err = reader.ReadNext()
	assert.ErrorIs(t, err, MagicNumberMismatchErr)
}

func TestReaderDirectIO(t *testing.T) {
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

func TestReaderDirectIOTrailer(t *testing.T) {
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

func TestReaderHappyPathMagicNumberContent(t *testing.T) {
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

func TestReaderForbidsClosedReader(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.NoError(t, err)
	_, err = reader.ReadNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.SkipNext()
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "is already closed")
}

func TestReaderForbidsDoubleOpens(t *testing.T) {
	reader := newTestReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.NoError(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}

func TestReaderInitNoPath(t *testing.T) {
	_, err := NewFileReader()
	assert.Equal(t, errors.New("NewFileReader: either os.File or string path must be supplied, never both"), err)
}

func TestReaderInitPathAndFile(t *testing.T) {
	f, err := os.OpenFile("test_files/readerTestFile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	assert.NoError(t, err)
	defer os.Remove("test_files/readerTestFile")
	defer f.Close()
	reader, err := NewFileReader(ReaderFile(f), ReaderPath("test_files/readerTestFile2"))
	assert.Equal(t, errors.New("NewFileReader: either os.File or string path must be supplied, never both"), err)
	assert.Nil(t, reader)
}

func expectErrorStringOnOpen(t *testing.T, reader OpenClosableI, expectedError string) {
	err := reader.Open()
	defer closeOpenClosable(t, reader)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), expectedError)
}

func newOpenedTestReader(t *testing.T, file string) (*FileReader, error) {
	reader := newTestReader(file, t)
	err := reader.Open()
	require.NoError(t, err)
	return reader, err
}

func newTestReader(file string, t *testing.T) *FileReader {
	r, err := NewFileReaderWithPath(file)
	require.NoError(t, err)
	return r.(*FileReader)
}
