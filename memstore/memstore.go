package memstore

import (
	"github.com/thomasjungblut/go-sstables/sstables"
	"errors"
)

var KeyAlreadyExists = errors.New("key already exists")

//noinspection GoNameStartsWithPackageName
type MemStoreI interface {
	// inserts when the key does not exist yet, returns an error when the key exists.
	Add(key []byte, value []byte) error
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// inserts when the key does not exist yet, updates the current value if the key exists.
	Upsert(key []byte, value []byte) error
	// deletes the key from the MemStore, returns an error if the key does not exist
	Delete(key []byte) error
	// TODO(thomas): flush to an sstable
	// TODO(thomas): add some notion of memory size to determine when to flush
}

type ValueStruct struct {
	// when deleting, we're simply tomb-stoning the key by setting value = nil, which also saves memory
	value *[]byte
}

type MemStore struct {
	skipListMap   *sstables.SkipListMap
	estimatedSize uint64
}

func (m *MemStore) Add(key []byte, value []byte) error {
	return upsertInternal(m, key, value, true)
}

func (m *MemStore) Contains(key []byte) bool {
	element, err := m.skipListMap.Get(key)
	// we can return false if we didn't find it by error, or when the key is tomb-stoned
	if err == sstables.NotFound {
		return false
	}
	if *element.(ValueStruct).value == nil {
		return false
	}
	return true
}

func (m *MemStore) Upsert(key []byte, value []byte) error {
	return upsertInternal(m, key, value, false)
}

func upsertInternal(m *MemStore, key []byte, value []byte, errorIfKeyExist bool) error {
	if key == nil {
		return errors.New("key was nil")
	}

	if value == nil {
		return errors.New("value was nil")
	}

	element, err := m.skipListMap.Get(key)

	if err != sstables.NotFound {
		if *element.(ValueStruct).value != nil && errorIfKeyExist {
			return KeyAlreadyExists
		}
		*element.(ValueStruct).value = value
	} else {
		m.skipListMap.Insert(key, ValueStruct{value: &value})
	}
	return nil
}

func (m *MemStore) Delete(key []byte) error {
	element, err := m.skipListMap.Get(key)
	if err == sstables.NotFound {
		return KeyAlreadyExists
	} else {
		*element.(ValueStruct).value = nil
	}

	return nil
}

func NewMemStore() (*MemStore) {
	return &MemStore{skipListMap: sstables.NewSkipList(sstables.BytesComparator)}
}
