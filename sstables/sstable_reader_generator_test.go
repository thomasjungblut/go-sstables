// this does not really test anything, it generates the test_files that can be used to test the file_reader
// you can switch it on by setting the "generate_compatfiles" env variable to something non-empty
package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"os"
	"path"
	"testing"
)

func TestGenerateTestFiles(t *testing.T) {
	if os.Getenv("generate_compatfiles") == "" {
		t.Skip("not requested to generate compatibility files")
		return
	}

	prefix := "test_files/"
	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableRecordIOV2")
	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableWithBloom")
	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableWithMetaData")
	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableWithCRCHashes")
	writeHappyPathSSTableWithEmptyValues(t, prefix+"SimpleWriteHappyPathSSTableWithCRCHashesEmptyValues")

	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableWithCRCHashesMismatch")
	imputeError(t, prefix+"SimpleWriteHappyPathSSTableWithCRCHashesMismatch")
}

// this will change a byte at a specific offset for crc hash test cases
func imputeError(t *testing.T, p string) {
	f, err := os.OpenFile(path.Join(p, DataFileName), os.O_RDWR, 0655)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()

	_, err = f.WriteAt([]byte{0x15}, 51)
	require.NoError(t, err)
}

func writeHappyPathSSTable(t *testing.T, path string) {
	writer := newSimpleBytesWriterAt(t, path)
	list := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	err := writer.WriteSkipListMap(list)
	assert.Nil(t, err)
}

func writeHappyPathSSTableWithEmptyValues(t *testing.T, path string) {
	writer := newSimpleBytesWriterAt(t, path)
	list := skiplist.NewSkipListMap[[]byte, []byte](skiplist.BytesComparator{})
	list.Insert(intToByteSlice(42), intToByteSlice(0))
	list.Insert(intToByteSlice(45), []byte{})
	err := writer.WriteSkipListMap(list)
	assert.Nil(t, err)
}

func newSimpleBytesWriterAt(t *testing.T, path string) *SSTableSimpleWriter {
	_ = os.RemoveAll(path)
	_ = os.MkdirAll(path, 0666)
	writer, e := NewSSTableSimpleWriter(WriteBasePath(path), WithKeyComparator(skiplist.BytesComparator{}))
	assert.Nil(t, e)
	return writer
}
