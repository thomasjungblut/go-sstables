package simpledb

import (
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
		log.Panicf("unexpected number of values getting merged, len=%d", l)
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

// TODO below should be done in a goroutine to not affect write latency
// TODO here we should already merge both sstables for ease of use
// TODO some of the parts can be atomic swaps instead of being under the rwLock
// have a channel with capacity 1 block on this operation if the existing didn't finish yet
func flushMemstoreAndMergeSStables(db *DB) error {
	// normally we would flush the memstore to disk, for simplicity sake we can directly merge it with the
	// sstable that we already have and swap the reader out.
	memStoreIterator := db.memStore.SStableIterator()
	sstableIterator, err := db.mainSSTableReader.Scan()
	if err != nil {
		return err
	}

	readPath := path.Join(db.basePath, db.currentSSTablePath)
	i, _ := strconv.Atoi(db.currentSSTablePath)
	db.currentSSTablePath = strconv.Itoa(i + 1)
	writePath := path.Join(db.basePath, db.currentSSTablePath)
	err = os.MkdirAll(writePath, 0700)
	if err != nil {
		return err
	}

	writer, err := sstables.NewSSTableStreamWriter(
		sstables.WriteBasePath(writePath),
		sstables.WithKeyComparator(skiplist.BytesComparator),
		sstables.BloomExpectedNumberOfElements(uint64(db.memStore.Size())))
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

	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath(writePath),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator),
	)
	if err != nil {
		return err
	}

	err = db.mainSSTableReader.Close()
	if err != nil {
		return err
	}
	err = os.RemoveAll(readPath)
	if err != nil {
		return err
	}

	db.mainSSTableReader = reader
	db.memStore = memstore.NewMemStore()
	return nil
}
