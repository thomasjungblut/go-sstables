package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestContains(t *testing.T) {
	reader := EmptySStableReader{}
	contains, err := reader.Contains([]byte{})
	require.NoError(t, err)
	assert.False(t, contains, "contains returned true")
}

func TestGet(t *testing.T) {
	reader := EmptySStableReader{}
	_, err := reader.Get([]byte{})
	assert.Equal(t, NotFound, err)
}

func TestMetaData(t *testing.T) {
	reader := EmptySStableReader{}
	assert.Equal(t, 0, int(reader.MetaData().NumRecords))
	require.Nil(t, reader.MetaData().MinKey)
	require.Nil(t, reader.MetaData().MaxKey)
}

func TestScan(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.Scan()
	require.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func TestEmptyScanStartingAt(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.ScanStartingAt([]byte{1, 2, 3})
	require.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func TestEmptyScanRange(t *testing.T) {
	reader := EmptySStableReader{}
	it, err := reader.ScanRange([]byte{1, 2, 3}, []byte{1, 2, 5})
	require.Nil(t, err)
	testConsumeEmptyIterator(t, it)
}

func testConsumeEmptyIterator(t *testing.T, it SSTableIteratorI) {
	_, _, err := it.Next()
	assert.Equal(t, Done, err)
}
