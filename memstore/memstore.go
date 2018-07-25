package memstore

import (
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"errors"
)

var KeyAlreadyExists = errors.New("key already exists")
var KeyNotFound = errors.New("key not found")

//noinspection GoNameStartsWithPackageName
type MemStoreI interface {
	// inserts when the key does not exist yet, returns a KeyAlreadyExists error when the key exists.
	Add(key []byte, value []byte) error
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// inserts when the key does not exist yet, updates the current value if the key exists.
	Upsert(key []byte, value []byte) error
	// deletes the key from the MemStore, returns a KeyNotFound error if the key does not exist
	Delete(key []byte) error
	// deletes the key from the MemStore
	DeleteIfExists(key []byte) error
	// returns a rough estimate of size in bytes of this MemStore
	EstimatedSizeInBytes() uint64
	// flushes the current memstore to disk as an SSTable, error if unsuccessful
	Flush(opts ...sstables.WriterOption) error
}

type ValueStruct struct {
	// when deleting, we're simply tomb-stoning the key by setting value = nil, which also saves memory
	value *[]byte
}

func (v ValueStruct) GetValue() []byte {
	return *v.value
}

type MemStore struct {
	skipListMap   *skiplist.SkipListMap
	estimatedSize uint64
	comparator    skiplist.KeyComparator
}

func (m *MemStore) Add(key []byte, value []byte) error {
	return upsertInternal(m, key, value, true)
}

func (m *MemStore) Contains(key []byte) bool {
	element, err := m.skipListMap.Get(key)
	// we can return false if we didn't find it by error, or when the key is tomb-stoned
	if err == skiplist.NotFound {
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
	if err != skiplist.NotFound {
		if *element.(ValueStruct).value != nil && errorIfKeyExist {
			return KeyAlreadyExists
		}
		prevLen := len(*element.(ValueStruct).value)
		*element.(ValueStruct).value = value
		m.estimatedSize = m.estimatedSize - uint64(prevLen) + uint64(len(value))
	} else {
		m.skipListMap.Insert(key, ValueStruct{value: &value})
		m.estimatedSize += uint64(len(key)) + uint64(len(value))
	}
	return nil
}

func (m *MemStore) Delete(key []byte) error {
	return deleteInternal(m, key, true)
}

func (m *MemStore) DeleteIfExists(key []byte) error {
	return deleteInternal(m, key, false)
}

func deleteInternal(m *MemStore, key []byte, errorIfKeyNotFound bool) error {
	element, err := m.skipListMap.Get(key)
	if err == skiplist.NotFound {
		if errorIfKeyNotFound {
			return KeyNotFound
		}
	} else {
		m.estimatedSize -= uint64(len(*element.(ValueStruct).value))
		*element.(ValueStruct).value = nil
	}

	return nil
}

func (m *MemStore) EstimatedSizeInBytes() uint64 {
	// we account for ~15% overhead
	return uint64(1.15 * float32(m.estimatedSize))
}

func (m *MemStore) Flush(writerOptions ...sstables.WriterOption) error {
	writerOptions = append(writerOptions, sstables.WithKeyComparator(m.comparator))
	writer, err := sstables.NewSSTableStreamWriter(writerOptions...)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	it := m.skipListMap.Iterator()
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		if err != nil {
			return err
		}

		kBytes := k.([]byte)
		vBytes := v.(ValueStruct)

		// do not write tombstones to the final file
		if vBytes.value != nil {
			writer.WriteNext(kBytes, *vBytes.value)
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

func NewMemStore() (*MemStore) {
	cmp := skiplist.BytesComparator
	return &MemStore{skipListMap: skiplist.NewSkipListMap(cmp), comparator: cmp}
}
