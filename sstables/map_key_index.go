package sstables

import (
	"errors"
	"fmt"
	"io"

	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

type ByteKeyMapper[T comparable] interface {
	MapBytes(data []byte) T
}

type Byte4KeyMapper struct {
}

func (s *Byte4KeyMapper) MapBytes(data []byte) [4]byte {
	if len(data) > 4 {
		panic(fmt.Sprintf("data length is too large, found %d but expected 4", len(data)))
	}
	var result [4]byte
	copy(result[:], data[:])
	return result
}

type Byte20KeyMapper struct {
}

func (s *Byte20KeyMapper) MapBytes(data []byte) [20]byte {
	if len(data) > 20 {
		panic(fmt.Sprintf("data length is too large, found %d but expected 20", len(data)))
	}
	var result [20]byte
	copy(result[:], data[:])
	return result
}

// MapKeyIndex is keeping the entire index as a slice and a map in memory and uses binary search to
// find the given keys for range lookups. This is useful for fast Contains/Get lookups.
type MapKeyIndex[T comparable] struct {
	SliceKeyIndex
	index  map[T]IndexVal
	mapper ByteKeyMapper[T]
}

func (s *MapKeyIndex[T]) Contains(key []byte) (bool, error) {
	idxval, found := s.index[s.mapper.MapBytes(key)]
	return found && !idxval.Tombstoned, nil
}

func (s *MapKeyIndex[T]) Get(key []byte) (IndexVal, error) {
	val, found := s.index[s.mapper.MapBytes(key)]
	if found {
		return val, nil
	}

	return IndexVal{}, skiplist.NotFound
}

type MapKeyIndexLoader[T comparable] struct {
	ReadBufferSize int
	Mapper         ByteKeyMapper[T]
}

func (s *MapKeyIndexLoader[T]) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
	if s.Mapper == nil {
		return nil, fmt.Errorf("error loader need a Mapper for sstable '%s'", indexPath)
	}

	reader, err := rProto.NewReader(
		rProto.ReaderPath(indexPath),
		rProto.ReadBufferSizeBytes(s.ReadBufferSize),
	)
	if err != nil {
		return nil, fmt.Errorf("error while creating index reader of sstable in '%s': %w", indexPath, err)
	}

	err = reader.Open()
	if err != nil {
		return nil, fmt.Errorf("error while opening index reader of sstable in '%s': %w", indexPath, err)
	}

	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	capacity := uint64(0)
	if metadata != nil {
		capacity = metadata.NumRecords
	}

	smap := make(map[T]IndexVal, capacity)
	sx := make([]sliceKey, 0, capacity)

	record := &proto.IndexEntry{}
	var i = 0
	for {
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		kBytes := s.Mapper.MapBytes(record.Key)
		smap[kBytes] = IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum, Tombstoned: record.Tombstoned}
		sx = append(sx, sliceKey{smap[kBytes], record.Key})

		i++
	}

	return &MapKeyIndex[T]{
		SliceKeyIndex: SliceKeyIndex{index: sx},
		index:         smap,
		mapper:        s.Mapper,
	}, nil
}
