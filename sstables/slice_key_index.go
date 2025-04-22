package sstables

import (
	"bytes"
	"errors"
	"fmt"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"golang.org/x/exp/slices"
	"io"
)

type sliceKey struct {
	IndexVal
	key []byte
}

// SliceKeyIndex is keeping the entire index as a slice in memory and uses binary search to find the given keys.
type SliceKeyIndex struct {
	NoOpOpenClose
	index []sliceKey
}

func (s *SliceKeyIndex) search(key []byte) (int, bool) {
	return slices.BinarySearchFunc(s.index, key, func(entry sliceKey, k []byte) int {
		return bytes.Compare(entry.key, k)
	})
}

func (s *SliceKeyIndex) Get(key []byte) (IndexVal, error) {
	idx, found := s.search(key)
	if found {
		return s.index[idx].IndexVal, nil
	}

	return IndexVal{}, skiplist.NotFound
}

func (s *SliceKeyIndex) Contains(key []byte) (bool, error) {
	_, found := s.search(key)
	return found, nil
}

func (s *SliceKeyIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	return &SliceKeyIndexIterator{index: s.index, endIndexExcl: len(s.index)}, nil
}

func (s *SliceKeyIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	idx, _ := s.search(key)
	return &SliceKeyIndexIterator{index: s.index, currentIndex: idx, endIndexExcl: len(s.index)}, nil
}

func (s *SliceKeyIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	if bytes.Compare(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}

	startIdx, _ := s.search(keyLower)
	endIdx, _ := s.search(keyHigher)

	// we need to adjust the ending a bit, because our iterator always includes the keyHigher in the result
	if endIdx >= 0 && endIdx < len(s.index) {
		if bytes.Compare(s.index[endIdx].key, keyHigher) <= 0 {
			endIdx = endIdx + 1
		}
	}

	return &SliceKeyIndexIterator{index: s.index, currentIndex: startIdx, endIndexExcl: endIdx}, nil
}

type SliceKeyIndexIterator struct {
	index        []sliceKey
	endIndexExcl int
	currentIndex int
}

func (s *SliceKeyIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.currentIndex >= s.endIndexExcl {
		return nil, IndexVal{}, skiplist.Done
	}
	cx := s.index[s.currentIndex]
	s.currentIndex += 1
	return cx.key, cx.IndexVal, nil
}

type SliceKeyIndexLoader struct {
	ReadBufferSize int
}

func (s *SliceKeyIndexLoader) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
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

	sx := make([]sliceKey, 0, capacity)

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

		sx = append(sx, sliceKey{IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}, record.Key})
	}

	return &SliceKeyIndex{NoOpOpenClose{}, sx}, nil
}
