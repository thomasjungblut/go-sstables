package wal

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingAppender counts the syncs of the wrapped appender and makes them slow, so that concurrent writers queue up.
type countingAppender struct {
	WriteAheadLogAppendI
	syncs atomic.Int64
}

func (c *countingAppender) AppendSync(record []byte) error {
	c.syncs.Add(1)
	time.Sleep(10 * time.Millisecond)
	return c.WriteAheadLogAppendI.AppendSync(record)
}

func TestGroupCommitAppenderConcurrentAppendSync(t *testing.T) {
	wal := newTestWal(t, "wal_group_commit_concurrent")
	counting := &countingAppender{WriteAheadLogAppendI: wal.WriteAheadLogAppendI}
	appender := NewGroupCommitAppender(counting)

	numRecords := 200
	var wg sync.WaitGroup
	for i := 0; i < numRecords; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			record := make([]byte, 8)
			binary.BigEndian.PutUint64(record, uint64(i))
			assert.NoError(t, appender.AppendSync(record))
		}(i)
	}
	wg.Wait()
	require.NoError(t, appender.Close())

	assert.Less(t, counting.syncs.Load(), int64(numRecords/2))
	seen := make(map[uint64]int)
	require.NoError(t, wal.Replay(func(record []byte) error {
		seen[binary.BigEndian.Uint64(record)]++
		return nil
	}))
	require.Len(t, seen, numRecords)
	for i := 0; i < numRecords; i++ {
		assert.Equal(t, 1, seen[uint64(i)], "record %d must be in the WAL exactly once", i)
	}
}

func TestGroupCommitAppenderMixedOperations(t *testing.T) {
	wal := newTestWal(t, "wal_group_commit_mixed")
	appender := NewGroupCommitAppender(wal.WriteAheadLogAppendI)

	require.NoError(t, appender.Append([]byte{1}))
	require.NoError(t, appender.AppendSync([]byte{2}))
	_, err := appender.Rotate()
	require.NoError(t, err)
	require.NoError(t, appender.AppendSync([]byte{3}))
	require.NoError(t, appender.Close())

	var replayed [][]byte
	require.NoError(t, wal.Replay(func(record []byte) error {
		replayed = append(replayed, record)
		return nil
	}))
	assert.Equal(t, [][]byte{{1}, {2}, {3}}, replayed)
}

func TestAppendSyncBatchSyncsOnce(t *testing.T) {
	wal := newTestWal(t, "wal_append_sync_batch")
	counting := &countingAppender{WriteAheadLogAppendI: wal.WriteAheadLogAppendI}

	require.NoError(t, AppendSyncBatch(counting, [][]byte{{1}, {2}, {3}}))
	assert.Equal(t, int64(1), counting.syncs.Load())
}

func TestGroupCommitAppenderNoWaitByDefault(t *testing.T) {
	appender := NewGroupCommitAppender(&countingAppender{})
	assert.Equal(t, time.Duration(0), appender.batcher.opts.maxBatchWait)
	assert.Equal(t, DefaultMaxGroupCommitBatchSize, appender.batcher.opts.maxBatchSize)
}

func TestGroupCommitAppenderMaxBatchWait(t *testing.T) {
	wal := newTestWal(t, "wal_group_commit_wait")
	counting := &countingAppender{WriteAheadLogAppendI: wal.WriteAheadLogAppendI}
	appender := NewGroupCommitAppender(counting, MaxBatchWait(500*time.Millisecond))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, appender.AppendSync([]byte{1}))
	}()
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		assert.NoError(t, appender.AppendSync([]byte{2}))
	}()
	wg.Wait()

	// the first record waited for the second one, both were synced together
	assert.Equal(t, int64(1), counting.syncs.Load())
}

func TestGroupCommitAppenderCloseWhileWaiting(t *testing.T) {
	wal := newTestWal(t, "wal_group_commit_close_waiting")
	appender := NewGroupCommitAppender(wal.WriteAheadLogAppendI, MaxBatchWait(500*time.Millisecond))

	appendErr := make(chan error)
	go func() {
		appendErr <- appender.AppendSync([]byte{1})
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, appender.Close())

	select {
	case err := <-appendErr:
		assert.Error(t, err, "a record committed after close must not be reported as durable")
	case <-time.After(5 * time.Second):
		t.Fatal("delayed AppendSync didn't return after the appender was closed")
	}
}
