package simpledb

import (
	"errors"

	"github.com/thomasjungblut/go-sstables/wal"
)

// pendingWrite is a write with a synced WAL, concurrent writes are committed together by a wal.Batcher (group commit)
// so that they share a single fsync.
type pendingWrite struct {
	walBytes []byte
	// apply mutates the memstore, it's called by the leader under the database lock after the WAL is durable
	apply func() error
	// err is the result of apply, set by the leader
	err error
}

// syncedWrite durably appends walBytes to the WAL and afterwards applies the mutation, batched with concurrent writes.
func (db *DB) syncedWrite(walBytes []byte, apply func() error) error {
	w := &pendingWrite{walBytes: walBytes, apply: apply}
	// errors of the whole batch are returned as is, in that case no write of the batch was applied
	if err := db.writeBatcher.Submit(w); err != nil {
		return err
	}
	return w.err
}

func (db *DB) commitWrites(batch []*pendingWrite) error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	if !db.open {
		return ErrNotOpenedYet
	}

	if db.closed {
		return ErrAlreadyClosed
	}

	records := make([][]byte, len(batch))
	for i, w := range batch {
		records[i] = w.walBytes
	}
	if err := wal.AppendSyncBatch(db.wal, records); err != nil {
		return err
	}

	// the memstore is only changed once the whole batch is durable, and in the same order as the WAL records, so that
	// a recovery replays into the same state
	for _, w := range batch {
		w.err = w.apply()
	}

	if db.memStore.EstimatedSizeInBytes() > db.memstoreMaxSize {
		// all writes are durable and applied at this point, a failed rotation is only reported to the leader
		if err := db.rotateWalAndFlushMemstore(); err != nil {
			batch[0].err = errors.Join(batch[0].err, err)
		}
	}
	return nil
}
