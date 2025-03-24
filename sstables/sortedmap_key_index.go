package sstables

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/exp/slices"

	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// SortedMapIndex is keeping the entire index as a map and the ordered keys as a slice in memory
// Use binary search to iter over the index.
type SortedMapIndex struct {
	index map[[20]byte]IndexVal
	keys  [][20]byte
}

func (s *SortedMapIndex) Contains(key []byte) bool {
	_, found := s.index[[20]byte(key)]
	return found
}
func (s *SortedMapIndex) Get(key []byte) (IndexVal, error) {
	val, found := s.index[[20]byte(key)]
	if found {
		return val, nil
	}

	return IndexVal{}, skiplist.NotFound
}

func (s *SortedMapIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	return &SortedMapIndexIterator{index: s.index, keys: s.keys, endIndexExcl: len(s.index)}, nil
}

func (s *SortedMapIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	idx, _ := s.search([20]byte(key))
	return &SortedMapIndexIterator{index: s.index, keys: s.keys, currentIndex: idx, endIndexExcl: len(s.index)}, nil
}

func (s *SortedMapIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	if bytes.Compare(keyLower[:], keyHigher[:]) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}

	startIdx, _ := s.search([20]byte(keyLower))
	fx, _ := s.search([20]byte(keyHigher))
	// we have slightly different opinions on the inclusivity of the end of the range scans here
	endIdx := min(fx+1, len(s.index))
	return &SortedMapIndexIterator{index: s.index, currentIndex: startIdx, endIndexExcl: endIdx}, nil
}

func (s *SortedMapIndex) search(key [20]byte) (int, bool) {
	return slices.BinarySearchFunc(s.keys, key, func(entry [20]byte, k [20]byte) int {
		return bytes.Compare(entry[:], k[:])
	})
}

type SortedMapIndexIterator struct {
	index        map[[20]byte]IndexVal
	keys         [][20]byte
	endIndexExcl int
	currentIndex int
}

func (s *SortedMapIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.currentIndex >= s.endIndexExcl {
		return nil, IndexVal{}, skiplist.Done
	}
	cx := s.keys[s.currentIndex]
	s.currentIndex += 1
	v := s.index[cx]
	return cx[:], v, nil
}

type SortedMapIndexLoader struct {
	ReadBufferSize int
	Binary         bool
}

func (s *SortedMapIndexLoader) Load(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
	if s.Binary {
		return s.loadBinary(indexPath, metadata)
	}
	return s.loadProtoBuf(indexPath, metadata)
}

func (s *SortedMapIndexLoader) loadProtoBuf(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {

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

	smap := make(map[[20]byte]IndexVal, capacity)
	skeys := make([][20]byte, capacity)

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
		smap[[20]byte(record.Key)] = IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}
		skeys[i] = [20]byte(record.Key)
		i++
	}

	return &SortedMapIndex{index: smap, keys: skeys}, nil
}

func (s *SortedMapIndexLoader) loadBinary(indexPath string, metadata *proto.MetaData) (SortedKeyIndex, error) {
	binaryFile, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error while opening binary index reader of sstable in '%s': %w", indexPath, err)
	}

	defer binaryFile.Close()

	capacity := uint64(0)
	if metadata != nil {
		capacity = metadata.NumRecords
	}

	smap := make(map[[20]byte]IndexVal, capacity)
	skeys := make([][20]byte, capacity)

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
		smap[[20]byte(record.Key)] = IndexVal{Offset: record.ValueOffset, Checksum: record.Checksum}
		skeys[i] = [20]byte(record.Key)
		i++
	}

	return &SortedMapIndex{index: smap, keys: skeys}, nil
}
