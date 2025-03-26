package recordio

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math"
	"math/rand"
	"testing"
)

func TestMMapReaderHappyPathSingleRecord(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeMMapReader(t, reader)

	// should contain an ascending 13 byte buffer
	buf, err := reader.ReadNextAt(FileHeaderSizeBytes)
	require.Nil(t, err)
	assertAscendingBytes(t, buf, 13)
}

func TestMMapReaderSingleRecordMisalignedOffset(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeMMapReader(t, reader)

	_, err = reader.ReadNextAt(FileHeaderSizeBytes + 1)
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestMMapReaderSingleRecordOffsetBiggerThanFile(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	defer closeMMapReader(t, reader)

	_, err = reader.ReadNextAt(42000)
	assert.Equal(t, errors.New("mmap: invalid ReadAt offset 42000"), errors.Unwrap(err))
}

func TestMMapReaderVersionMismatchV0(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v0", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 0")
}

func TestMMapReaderVersionMismatchV256(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord_v256", t)
	expectErrorStringOnOpen(t, reader, "version mismatch, expected a value from 1 to 3 but was 256")
}

func TestMMapReaderCompressionGzipHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp1", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 1, reader.header.compressionType)
}

func TestMMapReaderCompressionSnappyHeader(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp2", t)
	err := reader.Open()
	require.Nil(t, err)
	defer closeMMapReader(t, reader)
	assert.Equal(t, 2, reader.header.compressionType)
}

func TestMMapReaderCompressionUnknown(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord_comp300", t)
	expectErrorStringOnOpen(t, reader, "unknown compression type [300]")
}

func TestMMapReaderForbidsClosedReader(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Close()
	require.Nil(t, err)
	_, err = reader.ReadNextAt(100)
	assert.Contains(t, err.Error(), "was either not opened yet or is closed already")
	err = reader.Open()
	assert.Contains(t, err.Error(), "already closed")
}

func TestMMapReaderForbidsDoubleOpens(t *testing.T) {
	reader := newTestMMapReader("test_files/v3_compat/recordio_UncompressedSingleRecord", t)
	err := reader.Open()
	require.Nil(t, err)
	expectErrorStringOnOpen(t, reader, "already opened")
}

// this is explicitly testing the difference in mmap semantics, where we would get an EOF error due to the following:
// * record header is very small (5 bytes)
// * record itself is smaller than the remainder of the buffer (RecordHeaderV2MaxSizeBytes - 5 bytes of the header = 15 bytes)
// * only the EOF follows
// this basically triggers the mmap.ReaderAt to fill a buffer of RecordHeaderV2MaxSizeBytes size (up until the EOF) AND return the io.EOF as an error.
// that caused some failed tests in the sstable reader, so it makes sense to have an explicit test for it
func TestMMapReaderReadsSmallVarIntHeaderEOFCorrectly(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v3_compat/recordio_UncompressedSingleRecord")
	require.Nil(t, err)
	bytes, err := reader.ReadNextAt(FileHeaderSizeBytes)
	require.Nil(t, err)
	assertAscendingBytes(t, bytes, 13)
	bytes, err = reader.ReadNextAt(uint64(FileHeaderSizeBytes + 6 + len(bytes)))
	require.Nil(t, bytes)
	assert.Equal(t, io.EOF, err)

	// testing the boundaries around, which should give us a magic number mismatch
	bytes, err = reader.ReadNextAt(uint64(FileHeaderSizeBytes + 5 + len(bytes)))
	require.Nil(t, bytes)
	assert.Equal(t, errors.New("magic number mismatch"), errors.Unwrap(err))
}

func TestMMapReaderReadsNilAndEmpties(t *testing.T) {
	reader, err := newOpenedTestMMapReader(t, "test_files/v3_compat/recordio_UncompressedNilAndEmptyRecord")
	require.Nil(t, err)
	bytes, err := reader.ReadNextAt(FileHeaderSizeBytes)
	require.Nil(t, err)
	require.Nil(t, bytes)

	bytes, err = reader.ReadNextAt(uint64(14))
	require.Nil(t, err)
	require.Equal(t, []byte{}, bytes)
}

func TestMMApReaderReadSequencedWrites(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	var offsets []uint64
	for i := 0; i < math.MaxInt8; i++ {
		offset, err := writer.Write([]byte{byte(i)})
		require.NoError(t, err)
		offsets = append(offsets, offset)
	}

	require.NoError(t, writer.Close())
	reader, err := newOpenedTestMMapReader(t, writer.file.Name())
	require.NoError(t, err)

	require.Equal(t, uint64(0x381), reader.Size())
	for i, offset := range offsets {
		at, err := reader.ReadNextAt(offset)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, at)

		// SeekNext on the exact offset should yield the same record
		at, err = reader.SeekNext(offset)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, at)
	}

	// reads that seek each individual byte up until length
	j := 0
	for i := uint64(0); i < reader.Size(); i++ {
		next, err := reader.SeekNext(i)
		if j == len(offsets) {
			require.ErrorIs(t, err, io.EOF)
		} else {
			require.NoError(t, err)
			require.Equal(t, []byte{byte(j)}, next)
			if i >= offsets[j] {
				j++
			}
		}
	}
}

func TestMMApReaderReadShuffled(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	var values []byte
	var offsets []uint64
	for i := 0; i < math.MaxInt8; i++ {
		offset, err := writer.Write([]byte{byte(i)})
		require.NoError(t, err)
		offsets = append(offsets, offset)
		values = append(values, byte(i))
	}

	require.NoError(t, writer.Close())

	rand.Shuffle(len(offsets), func(i, j int) {
		values[i], values[j] = values[j], values[i]
		offsets[i], offsets[j] = offsets[j], offsets[i]
	})

	reader, err := newOpenedTestMMapReader(t, writer.file.Name())
	require.NoError(t, err)

	require.Equal(t, uint64(0x381), reader.Size())
	for i, offset := range offsets {
		at, err := reader.ReadNextAt(offset)
		require.NoError(t, err)
		require.Equal(t, []byte{values[i]}, at)

		// SeekNext on the exact offset should yield the same record
		at, err = reader.SeekNext(offset)
		require.NoError(t, err)
		require.Equal(t, []byte{values[i]}, at)
	}
}

func TestMMApReaderReadSequencedWritesSeeksSmallBuf(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	var offsets []uint64
	for i := 0; i < math.MaxInt8; i++ {
		offset, err := writer.Write([]byte{byte(i)})
		require.NoError(t, err)
		offsets = append(offsets, offset)
	}

	require.NoError(t, writer.Close())
	reader, err := newOpenedTestMMapReader(t, writer.file.Name())
	// this test reduces the seek buffer to be very small, so we can test the boundaries better
	reader.seekLen = 10
	require.NoError(t, err)

	require.Equal(t, uint64(0x381), reader.Size())
	for i, offset := range offsets {
		at, err := reader.SeekNext(offset)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, at)
	}

	// reads that seek each individual byte up until length
	j := 0
	for i := uint64(0); i < reader.Size(); i++ {
		next, err := reader.SeekNext(i)
		if j == len(offsets) {
			require.ErrorIs(t, err, io.EOF)
		} else {
			require.NoError(t, err)
			require.Equal(t, []byte{byte(j)}, next)
			if i >= offsets[j] {
				j++
			}
		}
	}
}

func newOpenedTestMMapReader(t *testing.T, file string) (*MMapReader, error) {
	reader := newTestMMapReader(file, t)
	err := reader.Open()
	require.Nil(t, err)
	return reader, err
}

func newTestMMapReader(file string, t *testing.T) *MMapReader {
	r, err := NewMemoryMappedReaderWithPath(file)
	require.Nil(t, err)
	return r.(*MMapReader)
}
