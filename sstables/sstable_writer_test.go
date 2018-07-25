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

func TestSkipListSimpleWriteHappyPath(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	defer os.RemoveAll(writer.streamWriter.opts.basePath)
	assert.Nil(t, err)
	list := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7,})
	err = writer.WriteSkipListMap(list)
	assert.Nil(t, err)

	reader, err := NewSSTableReader(
		ReadBasePath(writer.streamWriter.opts.basePath),
		ReadWithKeyComparator(writer.streamWriter.opts.keyComparator))
	assert.Nil(t, err)
	defer reader.Close()
	assertContentMatches(t, reader, list)
}

func TestSkipListStreamedWriteFailsOnKeyComparison(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	defer writer.Close()
	defer os.RemoveAll(writer.opts.basePath)
	assert.Nil(t, err)

	err = writer.Open()
	assert.Nil(t, err)

	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	assert.Nil(t, err)

	binary.BigEndian.PutUint32(key, uint32(6))
	err = writer.WriteNext(key, key)
	assert.Equal(t, errors.New("non-ascending key cannot be written"), err)

	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	assert.Equal(t, errors.New("the same key cannot be written more than once"), err)
}

func TestSkipListStreamedWriteKeyComparisonAdjustsBufferSize(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	defer writer.Close()
	defer os.RemoveAll(writer.opts.basePath)
	assert.Nil(t, err)

	err = writer.Open()
	assert.Nil(t, err)

	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	assert.Nil(t, err)

	key = make([]byte, 5)
	binary.BigEndian.PutUint32(key, uint32(14))
	err = writer.WriteNext(key, key)
	assert.Nil(t, err)

	key = make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(12))
	err = writer.WriteNext(key, key)
	assert.Equal(t, errors.New("non-ascending key cannot be written"), err)
}

func TestComparatorNotSupplied(t *testing.T) {
	_, err := NewSSTableSimpleWriter(WriteBasePath("abc"))
	assert.Equal(t, errors.New("no key comparator supplied"), err)
}

func TestDirectoryDoesNotExist(t *testing.T) {
	_, err := NewSSTableSimpleWriter(WithKeyComparator(skiplist.BytesComparator))
	assert.Equal(t, errors.New("basePath was not supplied"), err)
}

func TestCompressionTypeDoesNotExist(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	defer os.RemoveAll(tmpDir)
	assert.Nil(t, err)
	writer, err := NewSSTableSimpleWriter(WriteBasePath(tmpDir), DataCompressionType(25), WithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	err = writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements([]int{}))
	assert.Equal(t, errors.New("unsupported compression type 25"), err)
}

func newTestSSTableSimpleWriter() (*SSTableSimpleWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableSimpleWriter(WriteBasePath(tmpDir), WithKeyComparator(skiplist.BytesComparator))
}

func newTestSSTableStreamWriter() (*SSTableStreamWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableStreamWriter(WriteBasePath(tmpDir), WithKeyComparator(skiplist.BytesComparator))
}

//noinspection GoSnakeCaseUsage
func TEST_ONLY_NewSkipListMapWithElements(toInsert []int) *skiplist.SkipListMap {
	list := skiplist.NewSkipListMap(skiplist.BytesComparator)
	for _, e := range toInsert {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(e))
		value := make([]byte, 4)
		binary.BigEndian.PutUint32(value, uint32(e+1))
		list.Insert(key, value)
	}
	return list
}
