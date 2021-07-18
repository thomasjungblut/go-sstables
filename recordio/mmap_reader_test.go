package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestMMapReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer closeMMapReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNextAt(FileHeaderSizeBytes)
	assert.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
}

func TestMMapReaderSingleRecordMisalignedOffset(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer closeMMapReader(t, reader)

	_, err = reader.ReadNextAt(FileHeaderSizeBytes + 1)
	assert.Equal(t, errors.New("magic number mismatch"), err)
}

func TestMMapReaderSingleRecordOffsetBiggerThanFile(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	defer closeMMapReader(t, reader)

	_, err = reader.ReadNextAt(42000)
	assert.Equal(t, errors.New("mmap: invalid ReadAt offset 42000"), err)
}

func TestMMapReaderVersionMismatchV0(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 0"))
}

func TestMMapReaderVersionMismatchV256(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorOnOpen(t, reader, errors.New("version mismatch, expected a value from 1 to 2 but was 256"))
}

func TestMMapReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestMMapReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	assert.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestMMapReaderCompressionUnknown(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord_comp3", t)
	expectErrorOnOpen(t, reader, errors.New("unknown compression type [3]"))
}

func TestMMapReaderForbidsClosedReader(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	assert.Nil(t, err)
	_, err = reader.ReadNextAt(100)
	assert.Equal(t, errors.New("reader was either not opened yet or is closed already"), err)
	err = reader.Open()
	assert.Equal(t, errors.New("already closed"), err)
}

func TestMMapReaderForbidsDoubleOpens(t *testing.T) {
	reader := newTestMMapReader("test_files/v2_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	assert.Nil(t, err)
	expectErrorOnOpen(t, reader, errors.New("already opened"))
}

// this is explicitly testing the difference in mmap semantics, where we would get an EOF error due to the following:
// * record header is very small (5 bytes)
// * record itself is smaller than the remainder of the buffer (RecordHeaderV2MaxSizeBytes - 5 bytes of the header = 15 bytes)
// * only the EOF follows
// this basically triggers the mmap.ReaderAt to fill a buffer of RecordHeaderV2MaxSizeBytes size (up until the EOF) AND return the io.EOF as an error.
// that caused some failed tests in the sstable reader, so it makes sense to have an explicit test for it
func TestMMapReaderReadsSmallVarIntHeaderEOFCorrectly(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v2_compat/recordio_UncompressedSingleRecord")
	assert.Nil(t, err)
	bytes, err := reader.ReadNextAt(FileHeaderSizeBytes)
	assert.Nil(t, err)
	assertAscendingBytes(t, bytes, 13)
	bytes, err = reader.ReadNextAt(uint64(FileHeaderSizeBytes + 5 + len(bytes)))
	assert.Nil(t, bytes)
	assert.Equal(t, io.EOF, err)

	// testing the boundaries around, which should give us a magic number mismatch
	bytes, err = reader.ReadNextAt(uint64(FileHeaderSizeBytes + 4 + len(bytes)))
	assert.Nil(t, bytes)
	assert.Equal(t, errors.New("magic number mismatch"), err)
}

func newOpenedTestMMapReader(t *testing.T, file string) (*MMapReader, error) {
	reader := newTestMMapReader(file, t)
	err := reader.Open()
	assert.Nil(t, err)
	return reader, err
}

func newTestMMapReader(file string, t *testing.T) *MMapReader {
	r, err := NewMemoryMappedReaderWithPath(file)
	assert.Nil(t, err)
	return r.(*MMapReader)
}
