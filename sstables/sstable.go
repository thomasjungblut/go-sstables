package sstables

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

var IndexFileName = "index.rio"
var DataFileName = "data.rio"
var BloomFileName = "bloom.bf.gz"
var MetaFileName = "meta.pb.bin"

// iterator pattern as described in https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
var Done = errors.New("no more items in iterator")
var NotFound = errors.New("key was not found")

type SSTableIteratorI interface {
	// returns the next key, value in sequence
	// returns Done as the error when the iterator is exhausted
	Next() ([]byte, []byte, error)
}

type SSTableReaderI interface {
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// returns the value associated with the given key, NotFound as the error otherwise
	Get(key []byte) ([]byte, error)
	// Returns an iterator over the sorted sequence starting at the given key (inclusive if key is in the list).
	// Using a key that is out of the sequence range will result in either an empty iterator or the full sequence.
	ScanStartingAt(key []byte) (SSTableIteratorI, error)
	// Returns an iterator over the sorted sequence starting at the given keyLower (inclusive if key is in the list)
	// and until the given keyHigher was reached (inclusive if key is in the list).
	// Using keys that are out of the sequence range will result in either an empty iterator or the full sequence.
	// If keyHigher is lower than keyLower an error will be returned.
	ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error)
	// closes this sstable reader
	Close() error
}

type SSTableSimpleWriterI interface {
	// writes all records of that SkipList to an sstable disk structure, expects []byte as key and value
	WriteSkipList(skipListMap *skiplist.SkipListMap) error
}

type SSTableStreamWriterI interface {
	// opens the sstable files.
	Open() error
	// writes the next record to an sstable disk structure, expects keys to be ordered.
	WriteNext(key []byte, value []byte) error
	// closes the sstable files.
	Close() error
}
