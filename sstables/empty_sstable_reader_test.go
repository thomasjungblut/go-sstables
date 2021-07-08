package sstables

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContains(t *testing.T) {
	reader := EmptySStableReader{}
	assert.False(t, reader.Contains([]byte{}), "contains returned true")
}

func TestGet(t *testing.T) {
	reader := EmptySStableReader{}
	_, err := reader.Get([]byte{})
	assert.Equal(t, NotFound, err)
}

func TestMetaData(t *testing.T) {
	reader := EmptySStableReader{}
	assert.Equal(t, 0, int(reader.MetaData().NumRecords))
	assert.Nil(t, reader.MetaData().MinKey)
	assert.Nil(t, reader.MetaData().MaxKey)
}

func TestScan(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.Scan()
	assert.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func TestEmptyScanStartingAt(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.ScanStartingAt([]byte{1, 2, 3})
	assert.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func TestEmptyScanRange(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.ScanRange([]byte{1, 2, 3}, []byte{1, 2, 5})
	assert.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func testConsumeEmptyIterator(t *testing.T, it SSTableIteratorI) {
	_, _, err := it.Next()
	assert.Equal(t, Done, err)
}
