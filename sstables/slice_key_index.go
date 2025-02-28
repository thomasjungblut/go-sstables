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

// SliceKeyIndex is keeping the entire index as a slice in memory and uses binary search to find the given keys.
type SliceKeyIndex struct {
	index []*proto.IndexEntry
}

func (s *SliceKeyIndex) search(key []byte) (int, bool) {
	return slices.BinarySearchFunc(s.index, key, func(entry *proto.IndexEntry, k []byte) int {
		return bytes.Compare(entry.Key, k)
	})
}

func (s *SliceKeyIndex) Get(key []byte) (IndexVal, error) {
	idx, found := s.search(key)
	if found {
		entry := s.index[idx]
		return IndexVal{
			Offset:   entry.ValueOffset,
			Checksum: entry.Checksum,
		}, nil
	}

	return IndexVal{}, skiplist.NotFound
}

func (s *SliceKeyIndex) Contains(key []byte) bool {
	_, found := s.search(key)
	return found
}

func (s *SliceKeyIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	return &SliceKeyIndexIterator{index: s.index, endIndexExcl: len(s.index)}, nil
}

func (s *SliceKeyIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	idx, _ := s.search(key)
	return &SliceKeyIndexIterator{index: s.index, currentIndex: idx, endIndexExcl: len(s.index)}, nil
}

func (s *SliceKeyIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	startIdx, _ := s.search(keyLower)
	endIdx, _ := s.search(keyHigher)
	return &SliceKeyIndexIterator{index: s.index, currentIndex: startIdx, endIndexExcl: endIdx}, nil
}

type SliceKeyIndexIterator struct {
	index        []*proto.IndexEntry
	endIndexExcl int
	currentIndex int
}

func (s *SliceKeyIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.currentIndex >= s.endIndexExcl {
		return nil, IndexVal{}, skiplist.Done
	}

	defer func() {
		s.currentIndex += 1
	}()

	cx := s.index[s.currentIndex]
	return cx.Key, IndexVal{
		Offset:   cx.ValueOffset,
		Checksum: cx.Checksum,
	}, nil
}

type SliceKeyIndexLoader struct {
	ReadBufferSize int
}

func (s SliceKeyIndexLoader) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
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

	sx := make([]*proto.IndexEntry, 0, metadata.NumRecords)

	for {
		record := &proto.IndexEntry{}
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		sx = append(sx, record)
	}

	return &SliceKeyIndex{sx}, nil
}
