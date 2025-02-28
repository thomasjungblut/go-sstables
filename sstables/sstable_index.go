package sstables

import (
	"errors"
	"fmt"
	"io"

	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

type IndexVal struct {
	Offset   uint64
	Checksum uint64
}

type SortedKeyIndex interface {
	// Contains returns true if the given key can be found in the index
	Contains(key []byte) bool
	// Get returns the IndexVal that compares equal to the key supplied or returns skiplist.NotFound if it does not exist.
	Get(key []byte) (IndexVal, error)
	// Iterator returns an iterator over the entire sorted sequence
	Iterator() (skiplist.IteratorI[[]byte, IndexVal], error)
	// IteratorStartingAt returns an iterator over the sorted sequence starting at the given key (inclusive if key is in the index).
	// Using a key that is out of the sequence range will result in either an empty iterator or the full sequence.
	IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error)
	// IteratorBetween Returns an iterator over the sorted sequence starting at the given keyLower (inclusive if key is in the index)
	// and until the given keyHigher was reached (inclusive if key is in the index).
	// Using keys that are out of the sequence range will result in either an empty iterator or the full sequence.
	// If keyHigher is lower than keyLower an error will be returned
	IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error)
}

type IndexLoader interface {
	// Load is creating a SortedKeyIndex from the given path.
	Load(path string, metadata *proto.MetaData) (SortedKeyIndex, error)
}

type SkipListIndexLoader struct {
	KeyComparator  skiplist.Comparator[[]byte]
	ReadBufferSize int
}

func (l *SkipListIndexLoader) Load(indexPath string, _ *proto.MetaData) (_ SortedKeyIndex, err error) {
	reader, err := rProto.NewReader(
		rProto.ReaderPath(indexPath),
		rProto.ReadBufferSizeBytes(l.ReadBufferSize),
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

	indexMap := skiplist.NewSkipListMap[[]byte, IndexVal](l.KeyComparator)
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

		indexMap.Insert(record.Key, IndexVal{
			Offset:   record.ValueOffset,
			Checksum: record.Checksum,
		})
	}

	return indexMap, nil
}
