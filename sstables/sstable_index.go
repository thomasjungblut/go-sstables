package sstables

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

type IndexVal struct {
	Offset   uint64
	Checksum uint64
}

type NoOpOpenClose struct {
}

func (s *NoOpOpenClose) Open() error {
	return nil
}

func (s *NoOpOpenClose) Close() error {
	return nil
}

type SortedKeyIndex interface {
	recordio.OpenClosableI

	// Contains returns true if the given key can be found in the index
	Contains(key []byte) (bool, error)
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
