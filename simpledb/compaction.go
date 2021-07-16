package simpledb

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"log"
)

const (
	sstableIteratorId  = iota
	memstoreIteratorId = iota
)

func compactionFunc(key []byte, values [][]byte, context []interface{}) ([]byte, []byte) {
	l := len(values)
	if l == 0 || l > 2 {
		log.Panicf("unexpected number of values getting merged, len=%d but expected 2", l)
	}

	valToWrite := values[0]
	// the larger value of the context wins, that would be the memstore write
	if l == 2 && context[1].(int) > context[0].(int) {
		valToWrite = values[1]
	}

	// if the winning value is a tombstone (nil), then we will tell the merger to ignore it altogether
	// this is the actual compaction of the merged sstable
	if valToWrite == nil {
		return nil, nil
	}
	return key, valToWrite
}

func merge(numMemstoreElements int, writePath string, memStoreIterator sstables.SSTableIteratorI,
	sstableIterator sstables.SSTableIteratorI) error {

	writer, err := sstables.NewSSTableStreamWriter(
		sstables.WriteBasePath(writePath),
		sstables.WithKeyComparator(skiplist.BytesComparator),
		sstables.BloomExpectedNumberOfElements(uint64(numMemstoreElements)))
	if err != nil {
		return err
	}

	err = sstables.NewSSTableMerger(skiplist.BytesComparator).
		MergeCompact(sstables.MergeContext{
			Iterators: []sstables.SSTableIteratorI{
				memStoreIterator,
				sstableIterator,
			}, IteratorContext: []interface{}{
				memstoreIteratorId,
				sstableIteratorId,
			},
		}, writer, compactionFunc)
	if err != nil {
		return err
	}

	return nil
}

func swapMemstore(db *DB) *memstore.MemStoreI {
	storeToFlush := db.memStore.writeStore
	db.memStore = &RWMemstore{
		readStore:  storeToFlush,
		writeStore: memstore.NewMemStore(),
	}
	return &storeToFlush
}

func allocateNewSSTableFolders(db *DB) (string, string, error) {
	readPath := db.currentSSTablePath
	writePath, err := ioutil.TempDir(db.basePath, "sstable")
	if err != nil {
		return "", "", err
	}
	db.currentSSTablePath = writePath
	return readPath, writePath, err
}
