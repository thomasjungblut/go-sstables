package simpledb

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/wal"
)

// slowSyncWAL makes every sync slow so that concurrent writers queue up behind the leader, and counts the syncs.
type slowSyncWAL struct {
	wal.WriteAheadLogI
	syncs   atomic.Int64
	syncErr error
}

func (w *slowSyncWAL) AppendSync(record []byte) error {
	w.syncs.Add(1)
	time.Sleep(20 * time.Millisecond)
	if w.syncErr != nil {
		return w.syncErr
	}
	return w.WriteAheadLogI.AppendSync(record)
}

func TestGroupCommitBatchesConcurrentWrites(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_groupCommitBatches")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)
	slowWal := &slowSyncWAL{WriteAheadLogI: db.wal}
	db.wal = slowWal

	numWriters := 50
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, db.Put(strconv.Itoa(i), "value_"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()

	// the first writer syncs alone, all others queue up behind it and are committed in few batches
	assert.Less(t, slowWal.syncs.Load(), int64(numWriters/2))
	for i := 0; i < numWriters; i++ {
		v, err := db.Get(strconv.Itoa(i))
		require.NoError(t, err)
		assert.Equal(t, "value_"+strconv.Itoa(i), v)
	}
}

func TestGroupCommitWALErrorFailsWholeBatch(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_groupCommitWALError")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)
	walErr := errors.New("fsync failed")
	slowWal := &slowSyncWAL{WriteAheadLogI: db.wal, syncErr: walErr}
	db.wal = slowWal

	numWriters := 10
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.ErrorIs(t, db.Put(strconv.Itoa(i), "value"), walErr)
		}(i)
	}
	wg.Wait()

	// nothing that wasn't durable may become visible
	for i := 0; i < numWriters; i++ {
		_, err := db.Get(strconv.Itoa(i))
		assert.Equal(t, ErrNotFound, err)
	}
}

func TestGroupCommitAppliesInWALOrder(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_groupCommitOrder")
	defer cleanDatabaseFolder(t, db)
	slowWal := &slowSyncWAL{WriteAheadLogI: db.wal}
	db.wal = slowWal

	// all writers overwrite the same key, the value in the memstore must match what a WAL replay produces
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, db.Put("key", strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	before, err := db.Get("key")
	require.NoError(t, err)

	crashDatabaseInternally(t, db)
	db, err = NewSimpleDB(db.basePath)
	require.NoError(t, err)
	require.NoError(t, db.Open())
	defer closeDatabase(t, db)

	after, err := db.Get("key")
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestGroupCommitMaxBatchSizeOption(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpleDB_groupCommitMaxBatchSize")
	require.NoError(t, err)
	db, err := NewSimpleDB(tmpDir, MemstoreSizeBytes(1024*1024*2), GroupCommitMaxBatchSize(1))
	require.NoError(t, err)
	require.NoError(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)
	slowWal := &slowSyncWAL{WriteAheadLogI: db.wal}
	db.wal = slowWal

	numWriters := 20
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, db.Put(strconv.Itoa(i), "value"))
		}(i)
	}
	wg.Wait()

	// batches of a single write can't share any fsync
	assert.Equal(t, int64(numWriters), slowWal.syncs.Load())
}

// newGroupCommitTestDB opens a database whose WAL syncs are counted and slowed down, see slowSyncWAL.
func newGroupCommitTestDB(t *testing.T, name string, opts ...ExtraOption) (*DB, *slowSyncWAL) {
	tmpDir, err := os.MkdirTemp("", name)
	require.NoError(t, err)
	db, err := NewSimpleDB(tmpDir, append([]ExtraOption{MemstoreSizeBytes(1024 * 1024 * 2)}, opts...)...)
	require.NoError(t, err)
	require.NoError(t, db.Open())
	t.Cleanup(func() { cleanDatabaseFolder(t, db) })
	slowWal := &slowSyncWAL{WriteAheadLogI: db.wal}
	db.wal = slowWal
	return db, slowWal
}

// putStaggered puts two keys from two goroutines, the second one after the given gap
func putStaggered(t *testing.T, db *DB, gap time.Duration) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, db.Put("first", "value"))
	}()
	go func() {
		defer wg.Done()
		time.Sleep(gap)
		assert.NoError(t, db.Put("second", "value"))
	}()
	wg.Wait()
}

func TestGroupCommitDefaults(t *testing.T) {
	opts := defaultExtraOptions()
	assert.Equal(t, time.Duration(0), opts.groupCommitMaxWait, "writes must not be delayed by default")
	assert.Equal(t, wal.DefaultMaxGroupCommitBatchSize, opts.groupCommitMaxBatchSize)

	for _, opt := range []ExtraOption{GroupCommitMaxWait(time.Millisecond), GroupCommitMaxBatchSize(7)} {
		opt(opts)
	}
	assert.Equal(t, time.Millisecond, opts.groupCommitMaxWait)
	assert.Equal(t, 7, opts.groupCommitMaxBatchSize)
}

func TestGroupCommitMaxWaitBatchesStaggeredWrites(t *testing.T) {
	db, slowWal := newGroupCommitTestDB(t, "simpleDB_groupCommitWaitStaggered", GroupCommitMaxWait(500*time.Millisecond))
	defer closeDatabase(t, db)

	// the first write waits for the second one, both share a single sync
	putStaggered(t, db, 50*time.Millisecond)
	assert.Equal(t, int64(1), slowWal.syncs.Load())
	for _, k := range []string{"first", "second"} {
		v, err := db.Get(k)
		require.NoError(t, err)
		assert.Equal(t, "value", v)
	}
}

func TestGroupCommitCloseWhileWriteIsWaiting(t *testing.T) {
	db, slowWal := newGroupCommitTestDB(t, "simpleDB_groupCommitCloseWaiting", GroupCommitMaxWait(500*time.Millisecond))

	putErr := make(chan error)
	go func() {
		putErr <- db.Put("key", "value")
	}()
	// let the write start waiting for more writers, then close the database underneath it
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, db.Close())

	select {
	case err := <-putErr:
		assert.Equal(t, ErrAlreadyClosed, err)
	case <-time.After(5 * time.Second):
		t.Fatal("delayed write didn't return after the database was closed")
	}
	assert.Equal(t, int64(0), slowWal.syncs.Load())
}
