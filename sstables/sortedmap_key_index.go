package sstables

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"github.com/wk8/go-ordered-map/v2"
)

// SortedMapIndex is keeping the entire index as a slice in memory and uses binary search to find the given keys.
type SortedMapIndex struct {
	index *orderedmap.OrderedMap[string, IndexVal]
}

func KeyToString(key []byte) string {
	return hex.EncodeToString(key)
}
func (s *SortedMapIndex) Get(key []byte) (IndexVal, error) {
	val, found := s.index.Get(KeyToString(key))
	if found {
		return val, nil
	}

	return IndexVal{}, skiplist.NotFound
}

func (s *SortedMapIndex) Contains(key []byte) bool {
	_, found := s.index.Get(KeyToString(key))
	return found
}

func (s *SortedMapIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	start := s.index.Newest()
	end := s.index.Oldest()

	return s.IteratorBetween([]byte(start.Key), []byte(end.Key))
}

func (s *SortedMapIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	end := s.index.Oldest()
	return s.IteratorBetween(key, []byte(end.Key))
}

func (s *SortedMapIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	if bytes.Compare(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}

	start := s.index.GetPair(KeyToString(keyLower))
	if start == nil {
		return nil, errors.New("keyLower is not found")
	}
	end := s.index.GetPair(KeyToString(keyHigher))
	if end == nil {
		return nil, errors.New("keyHigher is not found")
	}
	return &SortedMapIndexIterator{index: s.index, currentIndex: nil, startIndex: start, endIndex: end}, nil
}

type SortedMapIndexIterator struct {
	index        *orderedmap.OrderedMap[string, IndexVal]
	endIndex     *orderedmap.Pair[string, IndexVal]
	startIndex   *orderedmap.Pair[string, IndexVal]
	currentIndex *orderedmap.Pair[string, IndexVal]
}

func (s *SortedMapIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.currentIndex != nil && s.currentIndex.Key >= s.endIndex.Key {
		return nil, IndexVal{}, skiplist.Done
	}
	if s.currentIndex == nil {
		if s.startIndex == nil {
			s.startIndex = s.index.Oldest()
		}
		s.currentIndex = s.startIndex

	} else {
		s.currentIndex = s.currentIndex.Next()
	}
	key, err := hex.DecodeString(s.currentIndex.Key)
	if err != nil {
		return nil, IndexVal{}, err
	}
	return key, s.currentIndex.Value, nil
}

type SortedMapIndexLoader struct {
	ReadBufferSize int
}

func (s *SortedMapIndexLoader) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
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

	sx := orderedmap.New[string, IndexVal](orderedmap.WithCapacity[string, IndexVal](int(capacity)))

	record := &proto.IndexEntry{}
	for {
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		sx.Set(KeyToString(record.Key), IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum})
	}

	return &SortedMapIndex{sx}, nil
}
