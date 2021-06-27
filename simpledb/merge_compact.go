package simpledb

import (
	"context"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
	"path"
	"strconv"
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

func flushMemstoreAndMergeSSTablesAsync(db *DB, storeToFlush *memstore.MemStoreI) {
	// ignoring the semaphore error since we want to block and do not cancel any context
	_ = db.mergeSemaphore.Acquire(context.Background(), 1)
	go func() {
		defer db.mergeSemaphore.Release(1)
		err := func(db *DB, storeToFlush *memstore.MemStoreI) error {
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

			// swap the sstable we just merged under the writer lock
			db.rwLock.Lock()
			err = func() error {
				defer db.rwLock.Unlock()

				reader, err := swapSSTableReader(db, err, writePath, readPath)
				if err != nil {
					return err
				}
				db.mainSSTableReader = reader

				return nil
			}()

			return nil
		}(db, storeToFlush)
		if err != nil {
			// TODO is panicking the best we can do?
			log.Panicf("error while async merging sstable at %s/%s, error was %v",
				db.basePath, db.currentSSTablePath, err)
		}
	}()
}

// this is supposed to be called under the db.rwLock
// this is the synchronous version of flushMemstoreAndMergeSSTablesAsync
func simpleSyncMergeCompaction(db *DB) error {
	// normally we would flush the memstore to disk, for simplicity sake we can directly merge it with the
	// sstable that we already have and swap the reader out.
	memStoreIterator := db.memStore.SStableIterator()
	sstableIterator, err := db.mainSSTableReader.Scan()
	if err != nil {
		return err
	}

	readPath, writePath, err := allocateNewSSTableFolders(db)
	if err != nil {
		return err
	}

	err = merge(db.memStore.Size(), writePath, memStoreIterator, sstableIterator)
	if err != nil {
		return err
	}

	reader, err := swapSSTableReader(db, err, writePath, readPath)
	if err != nil {
		return err
	}
	db.mainSSTableReader = reader
	mStore := memstore.NewMemStore()
	db.memStore = &RWMemstore{
		readStore:  mStore,
		writeStore: mStore,
	}
	return nil
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
func swapSSTableReader(db *DB, err error, writePath string, readPath string) (sstables.SSTableReaderI, error) {
	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(writePath),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
	)
	if err != nil {
		return nil, err
	}

	err = db.mainSSTableReader.Close()
	if err != nil {
		return nil, err
	}
	err = os.RemoveAll(readPath)
	if err != nil {
		return nil, err
	}
	return reader, nil
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
	readPath := path.Join(db.basePath, db.currentSSTablePath)
	i, _ := strconv.Atoi(db.currentSSTablePath)
	db.currentSSTablePath = strconv.Itoa(i + 1)
	writePath := path.Join(db.basePath, db.currentSSTablePath)
	err := os.MkdirAll(writePath, 0700)
	if err != nil {
		return "", "", nil
	}
	return readPath, writePath, err
}
