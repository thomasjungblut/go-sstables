package memstore

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"errors"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"os"
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
	assert.Nil(t, *kv.(ValueStruct).value)
}

func TestMemStoreAddTombStonedKeyAgain(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)

	err = m.Delete([]byte("a"))
	assert.False(t, m.Contains([]byte("a")))

	err = m.Add([]byte("a"), []byte("aVal2"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
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
	err := m.Upsert(make([]byte, 20), make([]byte, 50))
	assert.Nil(t, err)
	assert.Equal(t, uint64(80), m.EstimatedSizeInBytes())

	err = m.Upsert(make([]byte, 20), make([]byte, 100))
	assert.Nil(t, err)
	assert.Equal(t, uint64(138), m.EstimatedSizeInBytes())

	err = m.Delete(make([]byte, 20))
	assert.Nil(t, err)
	assert.Equal(t, uint64(23), m.EstimatedSizeInBytes())

	err = m.Upsert(make([]byte, 20), make([]byte, 200))
	assert.Nil(t, err)
	assert.Equal(t, uint64(253), m.EstimatedSizeInBytes())
}

func TestMemStoreFlush(t *testing.T) {
	m := NewMemStore()
	err := m.Upsert([]byte("akey"), []byte("aval"))
	assert.Nil(t, err)
	err = m.Upsert([]byte("bkey"), []byte("bval"))
	assert.Nil(t, err)

	tmpDir, err := ioutil.TempDir("", "memstore_flush")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	err = m.Flush(sstables.WriteBasePath(tmpDir))
	assert.Nil(t, err)

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(tmpDir),
		sstables.ReadWithKeyComparator(m.comparator))
	assert.Nil(t, err)
	defer reader.Close()

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
