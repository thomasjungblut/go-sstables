package sstables

import (
	"errors"
	"fmt"
	"io"
	"os"

	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// SortedMapIndex is keeping the entire index as a map and the ordered keys as a slice in memory
// Use binary search to iter over the index.
type ByteKeyMapper[T comparable] interface {
	MapBytes(data []byte) T
}

// use this key Mapper for sha1 keys
type Byte20KeyMapper[T comparable] struct{}

func (m Byte20KeyMapper[T]) MapBytes(data []byte) T {
	var result [20]byte

	if len(data) > 20 {
		data = data[:20]
	}
	copy(result[:], data)
	return any(result).(T)
}

type SortedMapIndex[T comparable] struct {
	SliceKeyIndex
	index  map[T]IndexVal
	mapper ByteKeyMapper[T]
}

func (s *SortedMapIndex[T]) Contains(key []byte) bool {
	_, found := s.index[s.mapper.MapBytes(key)]
	return found
}
func (s *SortedMapIndex[T]) Get(key []byte) (IndexVal, error) {
	val, found := s.index[s.mapper.MapBytes(key)]
	if found {
		return val, nil
	}

	return IndexVal{}, skiplist.NotFound
}

type SortedMapIndexLoader[T comparable] struct {
	ReadBufferSize int
	Binary         bool
	Mapper         ByteKeyMapper[T]
}

func (s *SortedMapIndexLoader[T]) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
	if s.Mapper == nil {
		return nil, fmt.Errorf("error loader need a Mapper '%s': %w", indexPath)
	}

	if s.Binary {
		return s.loadBinary(indexPath, metadata)
	}
	return s.loadProtoBuf(indexPath, metadata)
}

func (s *SortedMapIndexLoader[T]) loadProtoBuf(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {

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
		smap[s.Mapper.MapBytes(record.Key)] = IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}
		sx = append(sx, sliceKey{IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}, record.Key})

		i++
	}

	ski := SliceKeyIndex{index: sx}
	return &SortedMapIndex[T]{index: smap, sliceKeyIndex: ski, mapper: s.Mapper}, nil
}

func (s *SortedMapIndexLoader[T]) loadBinary(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
	binaryFile, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error while opening binary index reader of sstable in '%s': %w", indexPath, err)
	}

	defer binaryFile.Close()

	capacity := uint64(0)
	if metadata != nil {
		capacity = metadata.NumRecords
	}

	smap := make(map[T]IndexVal, capacity)
	sx := make([]sliceKey, 0, capacity)

	record := &FastIndexEntry{}
	var i = 0
	for {
		err := record.unmarshal(binaryFile)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}
		smap[s.Mapper.MapBytes(record.Key)] = IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}
		sx = append(sx, sliceKey{IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}, record.Key})

		i++
	}
	ski := SliceKeyIndex{index: sx}
	keymapper := Byte20KeyMapper[T]{}
	return &SortedMapIndex[T]{index: smap, sliceKeyIndex: ski, mapper: keymapper}, nil
}
