// this does not really test anything, it generates the test_files that can be used to test the file_reader
// you can switch it on by setting the "generate_compatfiles" env variable to something non-empty
package recordio

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestGenerateTestFiles(t *testing.T) {
	if os.Getenv("generate_compatfiles") == "" {
		t.Skip("not requested to generate compatibility files")
		return
	}

	prefix := "test_files/v2_compat/"
	writeUncompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord")
	writeUncompressedMultiRecordAscending(t, prefix+"recordio_UncompressedWriterMultiRecord_asc")
	writeCompressedMultiRecordAscending(t, prefix+"recordio_SnappyWriterMultiRecord_asc")
	writeVersionMismatchAugmented(t, prefix+"recordio_UncompressedSingleRecord_v0", 0)
	writeVersionMismatchAugmented(t, prefix+"recordio_UncompressedSingleRecord_v256", 256)
	writeCompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord_comp1", CompressionTypeGZIP)
	writeCompressedSingleRecord(t, prefix+"recordio_UncompressedSingleRecord_comp2", CompressionTypeSnappy)
	writeCompressedSingleRecordAugmented(t, prefix+"recordio_UncompressedSingleRecord_comp3", 3) //unknown compression type
	writeUncompressedSingleRecordAugmentedMagicNumber(t, prefix+"recordio_UncompressedSingleRecord_mnm")
}

func writeUncompressedSingleRecordAugmentedMagicNumber(t *testing.T, path string) {
	writeUncompressedSingleRecord(t, path)
	bytes, err := ioutil.ReadFile(path)
	binary.PutUvarint(bytes[8:12], MagicNumberSeparatorLong+1)
	assert.Nil(t, err)
	err = ioutil.WriteFile(path, bytes, 0666)
	assert.Nil(t, err)
}

func writeCompressedSingleRecordAugmented(t *testing.T, path string, compType int) {
	writeCompressedSingleRecord(t, path, CompressionTypeGZIP)
	bytes, err := ioutil.ReadFile(path)

	binary.LittleEndian.PutUint32(bytes[4:8], uint32(compType))

	assert.Nil(t, err)
	err = ioutil.WriteFile(path, bytes, 0666)
	assert.Nil(t, err)
}

func writeCompressedSingleRecord(t *testing.T, path string, compType int) {
	writer, err := newCompressedOpenedWriterAtPath(path, compType)
	defer writer.Close()
	assert.Nil(t, err)
	_, err = writer.Write(ascendingBytes(1337))
	assert.Nil(t, err)
}

func writeVersionMismatchAugmented(t *testing.T, path string, augmentedVersion uint32) {
	// we're writing an empty file, that should always go to CurrentVersion, but we'll change the version retrospectively
	// to mock the error on reading side
	writer, err := newUncompressedOpenedWriterAtPath(path)
	assert.Nil(t, err)
	assert.Nil(t, writer.Close())
	bytes, err := ioutil.ReadFile(path)

	binary.LittleEndian.PutUint32(bytes[0:4], augmentedVersion)

	assert.Nil(t, err)
	err = ioutil.WriteFile(path, bytes, 0666)
	assert.Nil(t, err)
}

func writeCompressedMultiRecordAscending(t *testing.T, path string) {
	writer, err := newCompressedOpenedWriterAtPath(path, CompressionTypeSnappy)
	defer writer.Close()
	assert.Nil(t, err)
	for i := 0; i < 255; i++ {
		_, err = writer.Write(ascendingBytes(i))
		assert.Nil(t, err)
	}
}

func writeUncompressedMultiRecordAscending(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer writer.Close()
	assert.Nil(t, err)
	for i := 0; i < 255; i++ {
		_, err = writer.Write(ascendingBytes(i))
		assert.Nil(t, err)
	}
}

func writeUncompressedSingleRecord(t *testing.T, path string) {
	writer, err := newUncompressedOpenedWriterAtPath(path)
	defer writer.Close()
	assert.Nil(t, err)
	_, err = writer.Write(ascendingBytes(13))
	assert.Nil(t, err)
}

func newUncompressedOpenedWriterAtPath(path string) (*FileWriter, error) {
	_ = os.Remove(path)
	r, err := NewFileWriterWithPath(path)
	if err != nil {
		return nil, err
	}
	return r, r.Open()
}

func newCompressedOpenedWriterAtPath(path string, compType int) (*FileWriter, error) {
	_ = os.Remove(path)
	r, err := NewCompressedFileWriterWithPath(path, compType)
	if err != nil {
		return nil, err
	}
	return r, r.Open()
}
