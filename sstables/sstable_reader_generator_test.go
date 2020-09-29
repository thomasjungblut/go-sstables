// this does not really test anything, it generates the test_files that can be used to test the file_reader
// you can switch it on by setting the "generate_compatfiles" env variable to something non-empty
package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"os"
	"testing"
)

func TestGenerateTestFiles(t *testing.T) {
	if os.Getenv("generate_compatfiles") == "" {
		t.Skip("not requested to generate compatibility files")
		return
	}

	prefix := "test_files/"
	writeHappyPathSSTable(t, prefix+"SimpleWriteHappyPathSSTableRecordIOV2")
}

func writeHappyPathSSTable(t *testing.T, path string) {
	writer := newSimpleBytesWriterAt(t, path)
	list := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7,})
	err := writer.WriteSkipListMap(list)
	assert.Nil(t, err)
}

func newSimpleBytesWriterAt(t *testing.T, path string) *SSTableSimpleWriter {
	_ = os.RemoveAll(path)
	_ = os.MkdirAll(path, 0666)
	writer, e := NewSSTableSimpleWriter(WriteBasePath(path), WithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, e)
	return writer
}
