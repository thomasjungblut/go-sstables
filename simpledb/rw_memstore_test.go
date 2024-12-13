package simpledb

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
)

func TestRWMemstoreAddShouldGoToWriteStore(t *testing.T) {
	rw := setupPrefilledRWMemstore(t)
	k := asStringBytes(42)
	assert.Nil(t, rw.Add(k, k))
	_, err := rw.readStore.Get(k)
	assert.Equal(t, memstore.KeyNotFound, err)
	v, err := rw.writeStore.Get(k)
	assert.Equal(t, k, v)
}

func TestRWMemstoreContains(t *testing.T) {
	rw := setupPrefilledRWMemstore(t)
	for i := 0; i < 15; i++ {
		assert.True(t, rw.Contains(asStringBytes(i)))
	}

	assert.False(t, rw.Contains(asStringBytes(-1)))
	assert.False(t, rw.Contains(asStringBytes(16)))
}

func TestRWMemstoreGet(t *testing.T) {
	rw := setupPrefilledRWMemstore(t)
	for i := 0; i < 15; i++ {
		k := asStringBytes(i)
		v, err := rw.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, k, v)
	}

	_, err := rw.Get(asStringBytes(-1))
	assert.Equal(t, memstore.KeyNotFound, err)

	_, err = rw.Get(asStringBytes(16))
	assert.Equal(t, memstore.KeyNotFound, err)

	_, err = rw.Get(asStringBytes(42))
	assert.Equal(t, memstore.KeyNotFound, err)
}

func TestRWMemstoreTombstoning(t *testing.T) {
	rw := setupPrefilledRWMemstore(t)

	for i := 0; i < 15; i++ {
		k := asStringBytes(i)
		assert.Nil(t, rw.Tombstone(k))

		_, err := rw.Get(k)
		assert.Equal(t, memstore.KeyTombstoned, err)
	}
}

func TestRWMemstoreDelete(t *testing.T) {
	rw := setupPrefilledRWMemstore(t)

	for i := 0; i < 10; i++ {
		k := asStringBytes(i)
		assert.Nil(t, rw.Delete(k))

		_, err := rw.Get(k)
		assert.Equal(t, memstore.KeyTombstoned, err)
	}

	for i := 10; i < 20; i++ {
		k := asStringBytes(i)
		assert.Nil(t, rw.DeleteIfExists(k))

		_, err := rw.Get(k)
		assert.Equal(t, memstore.KeyTombstoned, err)
	}
}

func setupPrefilledRWMemstore(t *testing.T) *RWMemstore {
	rw := &RWMemstore{
		readStore:  memstore.NewMemStore(),
		writeStore: memstore.NewMemStore(),
	}

	for i := 0; i < 10; i++ {
		is := asStringBytes(i)
		assert.Nil(t, rw.readStore.Add(is, is))
	}

	for i := 5; i < 15; i++ {
		is := asStringBytes(i)
		assert.Nil(t, rw.writeStore.Add(is, is))
	}

	return rw
}

func asStringBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}
