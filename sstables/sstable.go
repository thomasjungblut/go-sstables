package sstables

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

var IndexFileName = "index.rio"
var DataFileName = "data.rio"
var BloomFileName = "bloom.bf.gz"
var MetaFileName = "meta.pb.bin"
var NotFound = errors.New("key was not found")

type SSTableReaderI interface {
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// returns the value associated with the given key, NotFound as the error otherwise
	Get(key []byte) ([]byte, error)
	// closes this sstable reader
	Close() error
	// TODO(thomas): range scan
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
