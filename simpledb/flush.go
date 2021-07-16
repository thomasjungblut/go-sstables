package simpledb

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func flushMemstoreContinuously(db *DB) {
	err := func(db *DB) error {
		defer func() { db.doneFlushChannel <- true }()
		for flushAction := range db.storeFlushChannel {
			start := time.Now()
			writePath, err := ioutil.TempDir(db.basePath, "sstable")
			if err != nil {
				return err
			}

			walPath := flushAction.walPath
			memStoreToFlush := flushAction.memStore

			err = (*memStoreToFlush).FlushWithTombstones(
				sstables.WriteBasePath(writePath),
				sstables.WithKeyComparator(skiplist.BytesComparator),
				sstables.BloomExpectedNumberOfElements(uint64((*memStoreToFlush).Size())))
			if err != nil {
				return err
			}

			err = os.Remove(walPath)
			if err != nil {
				return err
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
		}

		return nil
	}(db)

	if err != nil {
		log.Panicf("error while merging sstable at %s, error was %v", db.currentSSTablePath, err)
	}
}
