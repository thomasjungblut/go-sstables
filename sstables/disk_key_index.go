package sstables

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"io"
)

// DiskKeyIndex is doing lookups on disk to find the value for a given key using binary search
type DiskKeyIndex struct {
	reader rProto.ReadAtI
	// offsetCacheMaxSize defines how many entries are saved in the offsetCache
	offsetCacheMaxSize int
	// offsetCache caches the result of a given offset for the binary search
	offsetCache map[uint64]*proto.IndexEntry
}

func (s *DiskKeyIndex) Close() error {
	return s.reader.Close()
}

func (s *DiskKeyIndex) Open() error {
	return s.reader.Open()
}

func (s *DiskKeyIndex) Contains(key []byte) (bool, error) {
	_, _, exists, err := s.binarySearch(key)
	return exists, err
}

func (s *DiskKeyIndex) Get(key []byte) (IndexVal, error) {
	_, v, exists, err := s.binarySearch(key)
	if err != nil {
		return IndexVal{}, err
	}
	if !exists {
		return IndexVal{}, skiplist.NotFound
	}

	return IndexVal{
		Offset:   v.ValueOffset,
		Checksum: v.Checksum,
	}, nil
}

func (s *DiskKeyIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	return s.newIterator(recordio.FileHeaderSizeBytes, s.reader.Size()), nil
}

func (s *DiskKeyIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	offset, _, _, err := s.binarySearch(key)
	if err != nil {
		return nil, err
	}
	return s.newIterator(offset, s.reader.Size()), nil
}

func (s *DiskKeyIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	if bytes.Compare(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}

	startOffset, _, _, err := s.binarySearch(keyLower)
	if err != nil {
		return nil, err
	}

	endOffset, _, found, err := s.binarySearch(keyHigher)
	if err != nil {
		return nil, err
	}

	// due to the inclusivity of keyHigher, we want to exclude the next item if it's not an exact match
	if !found {
		endOffset = endOffset - 1
	}

	return s.newIterator(startOffset, endOffset), nil
}

// adjusted version of sort.BinarySearchFunc, returning a file offset instead of an index
func (s *DiskKeyIndex) binarySearch(target []byte) (uint64, *proto.IndexEntry, bool, error) {
	n := s.reader.Size()
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	i, j := uint64(0), n
	for i < j {
		h := (i + j) >> 1 // avoid overflow when computing h
		// i ≤ h < j
		at, err := s.findAt(h)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, nil, false, nil
			}
			return 0, nil, false, err
		}
		if bytes.Compare(at.Key, target) < 0 {
			i = h + 1 // preserves cmp(x[i - 1], target) < 0
		} else {
			j = h // preserves cmp(x[j], target) >= 0
		}
	}
	// i == j, cmp(x[i-1], target) < 0, and cmp(x[j], target) (= cmp(x[i], target)) >= 0  =>  answer is i.
	at, err := s.findAt(i)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, nil, false, nil
		}
		return 0, nil, false, err
	}
	return i, at, i < n && bytes.Compare(at.Key, target) == 0, nil
}

func (s *DiskKeyIndex) findAt(off uint64) (*proto.IndexEntry, error) {
	if val, ok := s.offsetCache[off]; ok {
		return val, nil
	}

	record := &proto.IndexEntry{}
	_, _, err := s.reader.SeekNext(record, off)
	if len(s.offsetCache) < s.offsetCacheMaxSize {
		s.offsetCache[off] = record
	}

	return record, err
}

func (s *DiskKeyIndex) newIterator(offset, endOffset uint64) *DiskKeyIndexIterator {
	return &DiskKeyIndexIterator{
		reader:        s.reader,
		entry:         &proto.IndexEntry{},
		currentOffset: offset,
		endOffset:     endOffset,
	}
}

type DiskKeyIndexIterator struct {
	reader        rProto.ReadAtI
	entry         *proto.IndexEntry
	currentOffset uint64
	endOffset     uint64
}

func (s *DiskKeyIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.currentOffset > s.endOffset {
		return nil, IndexVal{}, skiplist.Done
	}

	offset, _, err := s.reader.SeekNext(s.entry, s.currentOffset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, IndexVal{}, skiplist.Done
		}
		return nil, IndexVal{}, err
	}

	s.currentOffset = offset + 1
	return s.entry.Key, IndexVal{
		Offset:   s.entry.ValueOffset,
		Checksum: s.entry.Checksum,
	}, nil
}

type DiskIndexLoader struct {
}

func (l *DiskIndexLoader) Load(indexPath string, _ *proto.MetaData) (_ SortedKeyIndex, err error) {
	reader, err := rProto.NewMMapProtoReaderWithPath(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error while creating index reader of sstable in '%s': %w", indexPath, err)
	}

	idx := &DiskKeyIndex{
		reader:             reader,
		offsetCacheMaxSize: 128,
		offsetCache:        make(map[uint64]*proto.IndexEntry),
	}
	return idx, nil
}
