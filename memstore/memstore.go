package memstore

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
)

var KeyAlreadyExists = errors.New("key already exists")
var KeyNotFound = errors.New("key not found")
var KeyTombstoned = errors.New("key was tombstoned")
var KeyNil = errors.New("key was nil")
var ValueNil = errors.New("value was nil")

//noinspection GoNameStartsWithPackageName
type MemStoreI interface {
	// inserts when the key does not exist yet, returns a KeyAlreadyExists error when the key exists.
	// Neither nil key nor values are allowed, KeyNil and ValueNil will be returned accordingly.
	Add(key []byte, value []byte) error
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// returns the values for the given key, if not exists returns a KeyNotFound error
	// if the key exists (meaning it was added and deleted) it will return KeyTombstoned as an error
	Get(key []byte) ([]byte, error)
	// inserts when the key does not exist yet, updates the current value if the key exists.
	// Neither nil key nor values are allowed, KeyNil and ValueNil will be returned accordingly.
	Upsert(key []byte, value []byte) error
	// deletes the key from the MemStore, returns a KeyNotFound error if the key does not exist.
	// Effectively this will set a tombstone for the given key and set its value to be nil.
	Delete(key []byte) error
	// deletes the key from the MemStore. The semantic is the same as in Delete, however when there is no
	// key in the memstore it will also not set a tombstone for it and there will be no error when the key isn't there.
	// That can be problematic in several database constellation, where you can use Tombstone.
	DeleteIfExists(key []byte) error
	// Tombstone records the given key as if it were deleted. When a given key does not exist it will insert it,
	// when it does it will do a Delete
	Tombstone(key []byte) error
	// returns a rough estimate of size in bytes of this MemStore
	EstimatedSizeInBytes() uint64
	// flushes the current memstore to disk as an SSTable, error if unsuccessful
	Flush(opts ...sstables.WriterOption) error
	// returns the current memstore as an sstables.SStableIteratorI to iterate the table in memory.
	// if there is a tombstoned record, the key will be returned but the value will be nil.
	// this is especially useful when you want to merge on-disk sstables with in memory memstores
	SStableIterator() sstables.SSTableIteratorI
	// Returns how many elements are in this memstore. This also includes tombstoned keys.
	Size() int
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

func (m *MemStore) Get(key []byte) ([]byte, error) {
	element, err := m.skipListMap.Get(key)
	// we can return false if we didn't find it by error, or when the key is tomb-stoned
	if err == skiplist.NotFound {
		return nil, KeyNotFound
	}
	val := *element.(ValueStruct).value
	if val == nil {
		return nil, KeyTombstoned
	}
	return val, nil
}

func (m *MemStore) Upsert(key []byte, value []byte) error {
	return upsertInternal(m, key, value, false)
}

func upsertInternal(m *MemStore, key []byte, value []byte, errorIfKeyExist bool) error {
	if key == nil {
		return KeyNil
	}

	if value == nil {
		return ValueNil
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

func (m *MemStore) Tombstone(key []byte) error {
	element, err := m.skipListMap.Get(key)
	if err != skiplist.NotFound {
		prevLen := len(*element.(ValueStruct).value)
		*element.(ValueStruct).value = nil
		m.estimatedSize = m.estimatedSize - uint64(prevLen)
	} else {
		var vByte []byte
		v := ValueStruct{value: &vByte}
		m.skipListMap.Insert(key, v)
		m.estimatedSize += uint64(len(key))
	}
	return nil
}

func (m *MemStore) EstimatedSizeInBytes() uint64 {
	// we account for ~15% overhead
	return uint64(1.15 * float32(m.estimatedSize))
}

func (m *MemStore) Size() int {
	return m.skipListMap.Size()
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

	it, _ := m.skipListMap.Iterator()
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
			err = writer.WriteNext(kBytes, *vBytes.value)
			if err != nil {
				return err
			}
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

type SkipListSStableIterator struct {
	iterator skiplist.SkipListIteratorI
}

func (s SkipListSStableIterator) Next() ([]byte, []byte, error) {
	key, val, err := s.iterator.Next()
	if err != nil {
		if err == skiplist.Done {
			return nil, nil, sstables.Done
		} else {
			return nil, nil, err
		}
	}
	valStruct := val.(ValueStruct)
	return key.([]byte), *valStruct.value, nil
}

func (m *MemStore) SStableIterator() sstables.SSTableIteratorI {
	it, _ := m.skipListMap.Iterator()
	return &SkipListSStableIterator{iterator: it}
}

func NewMemStore() *MemStore {
	cmp := skiplist.BytesComparator
	return &MemStore{skipListMap: skiplist.NewSkipListMap(cmp), comparator: cmp}
}
