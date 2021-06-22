package memstore

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"os"
	"testing"
)

func TestMemStoreAddHappyPath(t *testing.T) {
	m := NewMemStore()
	assert.False(t, m.Contains([]byte("a")))
	val, err := m.Get([]byte("a"))
	assert.Nil(t, val)
	assert.Equal(t, KeyNotFound, err)
	err = m.Add([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	val, err = m.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal"), val)
}

func TestMemStoreAddFailOnNilKeyValue(t *testing.T) {
	m := NewMemStore()
	err := m.Add(nil, nil)
	assert.Equal(t, errors.New("key was nil"), err)
	err = m.Add([]byte("a"), nil)
	assert.Equal(t, errors.New("value was nil"), err)
}

func TestMemStoreAddFailsOnExist(t *testing.T) {
	m := NewMemStore()
	err := m.Add([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	err = m.Add([]byte("a"), []byte("aVal"))
	assert.Equal(t, KeyAlreadyExists, err)
}

func TestMemStoreUpsertBehavesLikeAdd(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	val, err := m.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal"), val)
}

func TestMemStoreUpsertUpdatesOnExist(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	// make sure the value is set correctly
	kv, err := m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal"), *kv.(ValueStruct).value)

	err = m.Upsert([]byte("a"), []byte("aVal2"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	// make sure that the value was changed under the hood
	kv, err = m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal2"), *kv.(ValueStruct).value)
}

func TestMemStoreDeleteTombstones(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	// make sure the value is set correctly
	kv, err := m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal"), *kv.(ValueStruct).value)

	err = m.Delete([]byte("a"))
	assert.False(t, m.Contains([]byte("a")))
	// make sure that the value was changed under the hood
	kv, err = m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)

	// make sure that Get will return the new error message
	assert.Nil(t, *kv.(ValueStruct).value)
	_, err = m.Get([]byte("a"))
	assert.Equal(t, KeyTombstoned, err)
}

func TestMemStoreAddTombStonedKeyAgain(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.Equal(t, 1, m.Size())

	err = m.Delete([]byte("a"))
	assert.False(t, m.Contains([]byte("a")))
	// size should stay the same, as it's a tombstone
	assert.Equal(t, 1, m.Size())

	err = m.Add([]byte("a"), []byte("aVal2"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
	assert.Equal(t, 1, m.Size())
	kv, err := m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aVal2"), *kv.(ValueStruct).value)
}

func TestMemStoreDeleteSemantics(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))

	err = m.Delete([]byte("b"))
	assert.Equal(t, KeyNotFound, err)
	err = m.DeleteIfExists([]byte("b"))
	assert.Nil(t, err)
	err = m.DeleteIfExists([]byte("a"))
	assert.Nil(t, err)

	assert.False(t, m.Contains([]byte("a")))
	// make sure that the value was changed under the hood
	kv, err := m.skipListMap.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Nil(t, *kv.(ValueStruct).value)
}

func TestMemStoreSizeEstimates(t *testing.T) {
	m := NewMemStore()
	assert.Equal(t, 0, m.Size())
	err := m.Upsert(make([]byte, 20), make([]byte, 50))
	assert.Nil(t, err)
	assert.Equal(t, uint64(80), m.EstimatedSizeInBytes())
	assert.Equal(t, 1, m.Size())

	err = m.Upsert(make([]byte, 20), make([]byte, 100))
	assert.Nil(t, err)
	assert.Equal(t, uint64(138), m.EstimatedSizeInBytes())
	assert.Equal(t, 1, m.Size())

	err = m.Delete(make([]byte, 20))
	assert.Nil(t, err)
	assert.Equal(t, uint64(23), m.EstimatedSizeInBytes())
	assert.Equal(t, 1, m.Size())

	err = m.Upsert(make([]byte, 20), make([]byte, 200))
	assert.Nil(t, err)
	assert.Equal(t, uint64(253), m.EstimatedSizeInBytes())
	assert.Equal(t, 1, m.Size())
}

func TestMemStoreFlush(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("akey"), []byte("aval"))
	assert.Nil(t, err)
	err = m.Upsert([]byte("bkey"), []byte("bval"))
	assert.Nil(t, err)

	tmpDir, err := ioutil.TempDir("", "memstore_flush")
	assert.Nil(t, err)
	defer func() { assert.Nil(t, os.RemoveAll(tmpDir)) }()

	err = m.Flush(sstables.WriteBasePath(tmpDir))
	assert.Nil(t, err)

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(tmpDir),
		sstables.ReadWithKeyComparator(m.comparator))
	assert.Nil(t, err)
	defer closeReader(t, reader)

	val, err := reader.Get([]byte("akey"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("aval"), val)

	val, err = reader.Get([]byte("bkey"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("bval"), val)

	// negative test
	_, err = reader.Get([]byte("ckey"))
	assert.Equal(t, sstables.NotFound, err)
}

func TestMemStoreSStableIteratorUpsertOnly(t *testing.T) {
	m := NewMemStore()
	assert.Nil(t, m.Upsert([]byte("akey"), []byte("aval")))
	assert.Nil(t, m.Upsert([]byte("bkey"), []byte("bval")))
	assert.Nil(t, m.Upsert([]byte("ckey"), []byte("cval")))
	// assert.Nil(t, m.Delete([]byte("bkey")))

	actualCount := 0
	prefix := []string{"a", "b", "c"}
	it := m.SStableIterator()
	for _, e := range prefix {
		actualKey, actualValue, err := it.Next()
		assert.Nil(t, err)
		assert.Equal(t, e+"key", string(actualKey))
		assert.Equal(t, e+"val", string(actualValue))
		actualCount++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(prefix), actualCount)
	// iterator must be in Done state too
	_, _, err := it.Next()
	assert.Equal(t, sstables.Done, err)

}

func TestMemStoreSStableIteratorWithTombstones(t *testing.T) {
	m := NewMemStore()
	assert.Nil(t, m.Upsert([]byte("akey"), []byte("aval")))
	assert.Nil(t, m.Upsert([]byte("bkey"), []byte("bval")))
	assert.Nil(t, m.Upsert([]byte("ckey"), []byte("cval")))

	assert.Nil(t, m.Delete([]byte("bkey")))

	actualCount := 0
	prefix := []string{"a", "b", "c"}
	it := m.SStableIterator()
	for _, e := range prefix {
		actualKey, actualValue, err := it.Next()
		assert.Nil(t, err)
		assert.Equal(t, e+"key", string(actualKey))
		// on tombstones we expect the key to be returned, but the value being nil
		if e == "b" {
			assert.Nil(t, actualValue)
		} else {
			assert.Equal(t, e+"val", string(actualValue))
		}
		actualCount++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(prefix), actualCount)
	// iterator must be in Done state too
	_, _, err := it.Next()
	assert.Equal(t, sstables.Done, err)
}

func closeReader(t *testing.T, reader sstables.SSTableReaderI) {
	func() { assert.Nil(t, reader.Close()) }()
}
