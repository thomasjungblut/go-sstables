package sstables

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"errors"
)

func TestSimpleHappyPathRead(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7,})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSimpleHappyPathBloomRead(t *testing.T) {

	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7,})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestNegativeContainsHappyPath(t *testing.T) {

	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	assertNegativeContains(t, reader)
}

func TestNegativeContainsHappyPathBloom(t *testing.T) {

	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	assertNegativeContains(t, reader)
}

func assertNegativeContains(t *testing.T, reader *SSTableReader) {
	assert.False(t, reader.Contains([]byte{}))
	assert.False(t, reader.Contains([]byte{1}))
	assert.False(t, reader.Contains([]byte{1, 2, 3}))
	_, err := reader.Get([]byte{})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1, 2, 3})
	assert.Equal(t, errors.New("key was not found"), err)
}
