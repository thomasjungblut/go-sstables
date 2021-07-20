package simpledb

import (
	"fmt"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"
)

func flushMemstoreContinuously(db *DB) {
	defer func() { db.doneFlushChannel <- true }()
	err := func(db *DB) error {
		for flushAction := range db.storeFlushChannel {
			err := executeFlush(db, flushAction)
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

func executeFlush(db *DB, flushAction memStoreFlushAction) error {
	walPath := flushAction.walPath
	memStoreToFlush := flushAction.memStore
	numElements := uint64((*memStoreToFlush).Size())
	// we can skip if there is nothing to write, usually that indicates a proper "close" was done.
	if (*memStoreToFlush).Size() == 0 {
		log.Printf("no memstore flush necessary due to empty store, skipping\n")
		return nil
	}

	start := time.Now()

	gen := atomic.AddInt64(&db.currentGeneration, 1)
	writePath := path.Join(db.basePath, fmt.Sprintf(SSTablePattern, gen))
	err := os.MkdirAll(writePath, 0700)
	if err != nil {
		return err
	}

	err = (*memStoreToFlush).FlushWithTombstones(
		sstables.WriteBasePath(writePath),
		sstables.WithKeyComparator(db.cmp),
		sstables.BloomExpectedNumberOfElements(numElements))
	if err != nil {
		return err
	}

	if walPath != "" {
		err = os.Remove(walPath)
		if err != nil {
			return err
		}
	}

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(writePath),
		sstables.ReadWithKeyComparator(db.cmp),
	)
	if err != nil {
		return err
	}

	elapsedDuration := time.Since(start)
	totalBytes := reader.MetaData().TotalBytes
	throughput := float64(totalBytes) / 1024 / 1024 / elapsedDuration.Seconds()
	log.Printf("done flushing memstore to sstable of size %d bytes (%2.f mb/s) in %v. Path: [%s]\n",
		totalBytes, throughput, elapsedDuration, writePath)

	// add the newly created reader into the rotation
	// note that this CAN block here waiting on a current compaction to finish
	db.sstableManager.addReader(reader)

	return nil
}

func (db *DB) rotateWalAndFlushMemstore() error {
	walPath, err := db.wal.Rotate()
	if err != nil {
		return err
	}
	db.storeFlushChannel <- memStoreFlushAction{
		memStore: swapMemstore(db),
		walPath:  walPath,
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
