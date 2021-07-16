package simpledb

import (
	"fmt"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"
)

func flushMemstoreContinuously(db *DB) {
	err := func(db *DB) error {
		defer func() { db.doneFlushChannel <- true }()
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

	gen := atomic.AddInt32(&db.currentGeneration, 1)
	writePath := path.Join(db.basePath, fmt.Sprintf(SSTablePattern, gen))
	err := os.MkdirAll(writePath, 0700)
	if err != nil {
		return err
	}

	err = (*memStoreToFlush).FlushWithTombstones(
		sstables.WriteBasePath(writePath),
		sstables.WithKeyComparator(skiplist.BytesComparator),
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
		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
	)
	if err != nil {
		return err
	}

	db.sstableManager.addNewReader(reader)

	elapsedDuration := time.Since(start)
	totalBytes := reader.MetaData().TotalBytes
	throughput := float64(totalBytes) / 1024 / 1024 / elapsedDuration.Seconds()
	log.Printf("done flushing memstore to sstable of size %d bytes (%2.f mb/s) in %v. Path: [%s]\n",
		totalBytes, throughput, elapsedDuration, writePath)
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
