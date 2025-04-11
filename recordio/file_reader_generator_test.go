// this does not really test anything, it generates the test_files that can be used to test the file_reader
// you can switch it on by setting the "generate_compatfiles" env variable to something non-empty
package recordio

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestGenerateTestFiles(t *testing.T) {
	if os.Getenv("generate_compatfiles") == "" {
		t.Skip("not requested to generate compatibility files")
		return
	}

	prefix := "test_files/v4_compat/"
	writeUncompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord")
	writeUncompressedMultiRecordAscending(t, prefix+"recordio_UncompressedWriterMultiRecord_asc")
	writeCompressedMultiRecordAscending(t, prefix+"recordio_SnappyWriterMultiRecord_asc")
	writeVersionMismatchAugmented(t, prefix+"recordio_UncompressedSingleRecord_v0", 0)
	writeVersionMismatchAugmented(t, prefix+"recordio_UncompressedSingleRecord_v256", 256)
	writeCompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord_comp1", CompressionTypeGZIP)
	writeCompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord_comp2", CompressionTypeSnappy)
	writeCompressedSingleRecordAugmented(t, prefix+"recordio_UncompressedSingleRecord_comp300", 300) //unknown compression type
	writeUncompressedSingleRecordAugmentedMagicNumber(t, prefix+"recordio_UncompressedSingleRecord_mnm")
	writeUncompressedNilAndEmptyRecords(t, prefix+"recordio_UncompressedNilAndEmptyRecord")
	writeMagicNumberIntoContent(t, prefix+"recordio_UncompressedMagicNumberContent")
	writeCrcFailure(t, prefix+"recordio_UncompressedCrcFailure")

	writeDirectIOUncompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord_directio")
	writeDirectIOUncompressedSingleRecordRandomTrailer(t, prefix+"recordio_UncompressedSingleRecord_directio_trailer")
}

func writeUncompressedNilAndEmptyRecords(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer closeFileWriter(t, writer)
	require.NoError(t, err)
	_, err = writer.Write(nil)
	require.NoError(t, err)
	_, err = writer.Write([]byte{})
	require.NoError(t, err)
}

func writeMagicNumberIntoContent(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer closeFileWriter(t, writer)
	_, err = writer.Write(MagicNumberSeparatorLongBytes)
	require.NoError(t, err)

	_, err = writer.Write([]byte{21, 8, 23})
	require.NoError(t, err)

	_, err = writer.Write(MagicNumberSeparatorLongBytes)
	require.NoError(t, err)
}

func writeDirectIOUncompressedSingleRecord(t *testing.T, path string) {
	_ = os.Remove(path)
	w, err := NewFileWriter(Path(path), BufferSizeBytes(4096), DirectIO())
	require.NoError(t, err)
	require.NoError(t, w.Open())

	// this should produce a zeroed overhang, as directIO flushes the whole block
	_, err = w.Write([]byte{13, 06, 29, 07})
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func writeDirectIOUncompressedSingleRecordRandomTrailer(t *testing.T, path string) {
	writeDirectIOUncompressedSingleRecord(t, path)
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	// write some garbled data in between, so we know this file might be corrupted instead of properly written by directIO
	binary.PutUvarint(bytes[1024:1028], 1337)
	err = os.WriteFile(path, bytes, 0666)
	require.NoError(t, err)
}

func writeUncompressedSingleRecordAugmentedMagicNumber(t *testing.T, path string) {
	writeUncompressedSingleRecord(t, path)
	bytes, err := os.ReadFile(path)
	binary.PutUvarint(bytes[8:12], MagicNumberSeparatorLong+1)
	require.NoError(t, err)
	err = os.WriteFile(path, bytes, 0666)
	require.NoError(t, err)
}

func writeCompressedSingleRecordAugmented(t *testing.T, path string, compType int) {
	writeCompressedSingleRecord(t, path, CompressionTypeGZIP)
	bytes, err := os.ReadFile(path)

	binary.LittleEndian.PutUint32(bytes[4:8], uint32(compType))

	require.NoError(t, err)
	err = os.WriteFile(path, bytes, 0666)
	require.NoError(t, err)
}

func writeCompressedSingleRecord(t *testing.T, path string, compType int) {
	writer, err := newCompressedOpenedWriterAtPath(path, compType)
	defer closeFileWriter(t, writer)
	require.NoError(t, err)
	_, err = writer.Write(ascendingBytes(1337))
	require.NoError(t, err)
}

func writeVersionMismatchAugmented(t *testing.T, path string, augmentedVersion uint32) {
	// we're writing an empty file, that should always go to CurrentVersion, but we'll change the version retrospectively
	// to mock the error on reading side
	writer, err := newUncompressedOpenedWriterAtPath(path)
	require.NoError(t, err)
	assert.Nil(t, writer.Close())
	bytes, err := os.ReadFile(path)

	binary.LittleEndian.PutUint32(bytes[0:4], augmentedVersion)

	require.NoError(t, err)
	err = os.WriteFile(path, bytes, 0666)
	require.NoError(t, err)
}

func writeCrcFailure(t *testing.T, path string) {
	writeUncompressedSingleRecord(t, path)
	bytes, err := os.ReadFile(path)

	binary.LittleEndian.PutUint32(bytes[0x11:0x15], uint32(42))

	require.NoError(t, err)
	err = os.WriteFile(path, bytes, 0666)
	require.NoError(t, err)
}

func writeCompressedMultiRecordAscending(t *testing.T, path string) {
	writer, err := newCompressedOpenedWriterAtPath(path, CompressionTypeSnappy)
	defer closeFileWriter(t, writer)
	require.NoError(t, err)
	for i := 0; i < 255; i++ {
		_, err = writer.Write(ascendingBytes(i))
		require.NoError(t, err)
	}
}

func writeUncompressedMultiRecordAscending(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer closeFileWriter(t, writer)
	require.NoError(t, err)
	for i := 0; i < 255; i++ {
		_, err = writer.Write(ascendingBytes(i))
		require.NoError(t, err)
	}
}

func writeUncompressedSingleRecord(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer closeFileWriter(t, writer)
	require.NoError(t, err)
	_, err = writer.Write(ascendingBytes(13))
	require.NoError(t, err)
}

func newUncompressedOpenedWriterAtPath(path string) (*FileWriter, error) {
	_ = os.Remove(path)
	r, err := NewFileWriter(Path(path))
	if err != nil {
		return nil, err
	}
	return r.(*FileWriter), r.Open()
}

func newCompressedOpenedWriterAtPath(path string, compType int) (*FileWriter, error) {
	_ = os.Remove(path)
	r, err := NewFileWriter(Path(path), CompressionType(compType))
	if err != nil {
		return nil, err
	}
	return r.(*FileWriter), r.Open()
}
