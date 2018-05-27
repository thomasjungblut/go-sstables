package sstables

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
)

var IndexFileName = "index.rio"
var DataFileName = "data.rio"
var BloomFileName = "bloom.bf.gz"
// TODO(thomas): we need to store metadata too (min key, max key, sequences, num records)

const (
	// never reorder, always append
	SSTableReaderNoMemory          = iota
	SSTableReaderIndexInMemory     = iota
	SSTableReaderFullTableInMemory = iota
)

type SSTableReaderI interface {
	// returns true when the given key exists, false otherwise
	Contains(key []byte) bool
	// returns the value associated with the given key, NotFound as the error otherwise
	Get(key []byte) ([]byte, error)
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

// read/write options
type ReadOptions struct {
	basePath string
	readMode int
}

type ReadOption func(*ReadOptions)

func ReadBasePath(p string) ReadOption {
	return func(args *ReadOptions) {
		args.basePath = p
	}
}

func ReadMode(p int) ReadOption {
	return func(args *ReadOptions) {
		args.readMode = p
	}
}
