package sstables

import (
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// SuperSSTableReader unifies several sstables under one single reader with the same interface.
// The ordering of the readers matters, it is assumed the older reader comes before the newer (ascending order).
// All lookups are done concurrently, despite scans being done sequentially but using a heap.
type SuperSSTableReader struct {
	readers []SSTableReaderI
}

func (s SuperSSTableReader) Contains(key []byte) bool {
	panic("implement me")
}

func (s SuperSSTableReader) Get(key []byte) ([]byte, error) {
	panic("implement me")
}

func (s SuperSSTableReader) Scan() (SSTableIteratorI, error) {
	panic("implement me")
}

func (s SuperSSTableReader) ScanStartingAt(key []byte) (SSTableIteratorI, error) {
	panic("implement me")
}

func (s SuperSSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error) {
	panic("implement me")
}

func (s SuperSSTableReader) Close() error {
	panic("implement me")
}

func (s SuperSSTableReader) MetaData() *proto.MetaData {
	panic("implement me")
}
