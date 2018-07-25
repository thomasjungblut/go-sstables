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
	assertContentMatches(t, reader, skipListMap)
}

func TestNegativeContains(t *testing.T) {

	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	assert.False(t, reader.Contains([]byte{}))
	assert.False(t, reader.Contains([]byte{1}))
	assert.False(t, reader.Contains([]byte{1, 2, 3}))

	_, err = reader.Get([]byte{})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1, 2, 3})
	assert.Equal(t, errors.New("key was not found"), err)
}

func assertContentMatches(t *testing.T, reader *SSTableReader, expectedSkipListMap *skiplist.SkipListMap) {
	it := expectedSkipListMap.Iterator()
	numRead := 0
	for {
		expectedKey, expectedValue, err := it.Next()
		if err == skiplist.Done {
			break
		}
		assert.Nil(t, err)

		assert.True(t, reader.Contains(expectedKey.([]byte)))
		actualValue, err := reader.Get(expectedKey.([]byte))
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, actualValue)
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, expectedSkipListMap.Size(), numRead)
}
