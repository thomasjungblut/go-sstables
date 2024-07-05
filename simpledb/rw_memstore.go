package simpledb

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
)

// RWMemstore the RW memstore contains two memstores, one for reading, one for writing.
// the one that writes takes precedence over the read store (for the same key).
type RWMemstore struct {
	readStore  memstore.MemStoreI
	writeStore memstore.MemStoreI
}

// read paths

func (c *RWMemstore) Contains(key []byte) bool {
	return c.writeStore.Contains(key) || c.readStore.Contains(key)
}

func (c *RWMemstore) Get(key []byte) ([]byte, error) {
	// the write memstore always wins here, if the key is not found
	// in the writeStore the readStore is the source of truth
	writeVal, writeErr := c.writeStore.Get(key)
	if writeErr != nil {
		if errors.Is(writeErr, memstore.KeyNotFound) {
			return c.readStore.Get(key)
		}

		// that also includes the tombstones
		return nil, writeErr
	}

	return writeVal, nil
}

// write paths, just proxy to the writeStore

func (c *RWMemstore) Add(key []byte, value []byte) error {
	return c.writeStore.Add(key, value)
}

func (c *RWMemstore) Upsert(key []byte, value []byte) error {
	return c.writeStore.Upsert(key, value)
}

func (c *RWMemstore) Delete(key []byte) error {
	err := c.writeStore.Delete(key)
	if errors.Is(err, memstore.KeyNotFound) {
		return c.Tombstone(key)
	}

	return err
}

func (c *RWMemstore) DeleteIfExists(key []byte) error {
	return c.Delete(key)
}

func (c *RWMemstore) Tombstone(key []byte) error {
	return c.writeStore.Tombstone(key)
}

func (c *RWMemstore) EstimatedSizeInBytes() uint64 {
	return c.writeStore.EstimatedSizeInBytes()
}

func (c *RWMemstore) SStableIterator() sstables.SSTableIteratorI {
	return c.writeStore.SStableIterator()
}

func (c *RWMemstore) Flush(opts ...sstables.WriterOption) error {
	return c.writeStore.Flush(opts...)
}

func (c *RWMemstore) Size() int {
	return c.writeStore.Size()
}
