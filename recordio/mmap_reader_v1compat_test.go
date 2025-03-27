// this file exists for backward compatibility with the V1 files
// is basically a 1:1 copy of mmap_reader_test, which has additional tests and goes to the different folder
package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMMapReaderHappyPathSingleRecordV1(t *testing.T) {
	reader := newOpenedTestMMapReader(t, "test_files/v1_compat/recordio_UncompressedSingleRecord")
	defer closeMMapReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNextAt(FileHeaderSizeBytes)
	require.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
}

func TestMMapReaderSingleRecordMisalignedOffsetV1(t *testing.T) {
	reader := newOpenedTestMMapReader(t, "test_files/v1_compat/recordio_UncompressedSingleRecord")
	defer closeMMapReader(t, reader)

	_, err := reader.ReadNextAt(FileHeaderSizeBytes + 1)
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestMMapReaderSingleRecordOffsetBiggerThanFileV1(t *testing.T) {
	reader := newOpenedTestMMapReader(t, "test_files/v1_compat/recordio_UncompressedSingleRecord")
	defer closeMMapReader(t, reader)

	_, err := reader.ReadNextAt(42000)
	assert.Equal(t, errors.New("mmap: invalid ReadAt offset 42000"), errors.Unwrap(err))
}

func TestMMapReaderV1VersionMismatchV0(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 0")
}

func TestMMapReaderV1VersionMismatchV256(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 256")
}

func TestMMapReaderV1CompressionGzipHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestMMapReaderV1CompressionSnappyHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestMMapReaderForbidsClosedReaderV1(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.Nil(t, err)
	_, err = reader.ReadNextAt(100)
	assert.Contains(t, err.Error(), "opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "already closed")
}

func TestMMapReaderForbidsDoubleOpensV1(t *testing.T) {
	reader := newTestMMapReader("test_files/v1_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.Nil(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}
