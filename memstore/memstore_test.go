package memstore

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"errors"
)

func TestMemStoreAddHappyPath(t *testing.T) {
	m := NewMemStore()
	assert.False(t, m.Contains([]byte("a")))
	err := m.Add([]byte("a"), []byte("aVal"))
	assert.Nil(t, err)
	assert.True(t, m.Contains([]byte("a")))
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
