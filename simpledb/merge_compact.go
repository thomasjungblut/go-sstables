package simpledb

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"log"
	"os"
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

func flushMemstoreAndMergeSSTablesAsync(db *DB) {
	err := func(db *DB) error {
		defer func() { db.doneFlushChannel <- true }()
		for storeToFlush := range db.storeFlushChannel {
			readPath, writePath, err := allocateNewSSTableFolders(db)
			if err != nil {
				return err
			}

			memStoreIterator := (*storeToFlush).SStableIterator()
			sstableIterator, err := db.mainSSTableReader.Scan()
			if err != nil {
				return err
			}

			err = merge((*storeToFlush).Size(), writePath, memStoreIterator, sstableIterator)
			if err != nil {
				return err
			}

			err = swapSSTableReader(db, err, writePath, readPath)
			if err != nil {
				return err
			}
		}

		return nil
	}(db)

	if err != nil {
		log.Panicf("error while merging sstable at %s, error was %v", db.currentSSTablePath, err)
	}
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

// swapSSTableReader exchanges the old reader with a newly created one over the writePath.
// the readPath will be deleted completely.
func swapSSTableReader(db *DB, err error, writePath string, readPath string) error {
	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(writePath),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
	)
	if err != nil {
		return err
	}

	db.rwLock.Lock()
	return func(db *DB) error {
		defer db.rwLock.Unlock()

		err = db.mainSSTableReader.Close()
		if err != nil {
			return err
		}

		// at the very start we only have the memstore, thus check if there's a path first
		if readPath != "" {
			err = os.RemoveAll(readPath)
			if err != nil {
				return err
			}
		}
		// finally swap the pointer over to the new reader
		db.mainSSTableReader = reader

		return nil
	}(db)
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
