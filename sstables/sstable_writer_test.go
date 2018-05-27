package sstables

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"io/ioutil"
	"os"
	"encoding/binary"
	"errors"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

func TestSkipListWriteHappyPath(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	defer os.RemoveAll(writer.streamWriter.opts.basePath)
	assert.Nil(t, err)
	list := newSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7,})
	err = writer.WriteSkipListMap(list)
	assert.Nil(t, err)
	// TODO read it back and see if it matches
}

func TestDirectoryDoesNotExist(t *testing.T) {
	writer := NewSSTableSimpleWriter(WriteBasePath(""))
	err := writer.WriteSkipListMap(newSkipListMapWithElements([]int{}))
	assert.Equal(t, errors.New("basePath was not supplied"), err)
}

func TestCompressionTypeDoesNotExist(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	defer os.RemoveAll(tmpDir)
	assert.Nil(t, err)
	writer := NewSSTableSimpleWriter(WriteBasePath(tmpDir), DataCompressionType(25))
	err = writer.WriteSkipListMap(newSkipListMapWithElements([]int{}))
	assert.Equal(t, errors.New("unsupported compression type 25"), err)
}

func newTestSSTableSimpleWriter() (*SSTableSimpleWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableSimpleWriter(WriteBasePath(tmpDir)), nil
}

func newSkipListMapWithElements(toInsert []int) *skiplist.SkipListMap {
	list := skiplist.NewSkipList(skiplist.BytesComparator)
	for _, e := range toInsert {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(e))
		value := make([]byte, 4)
		binary.BigEndian.PutUint32(value, uint32(e+1))
		list.Insert(key, value)
	}
	return list
}
