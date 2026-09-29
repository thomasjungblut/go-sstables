package wal

import (
	"sync"
)

// DefaultMaxGroupCommitBatchSize is the number of records the GroupCommitAppender syncs at most with a single fsync.
const DefaultMaxGroupCommitBatchSize = 1024

// GroupCommitAppender makes a WriteAheadLogAppendI safe for concurrent use and batches the fsyncs of concurrent
// AppendSync calls (group commit): all records that were appended by concurrent callers are made durable by a single
// fsync. Every AppendSync call still only returns once its own record is durable.
type GroupCommitAppender struct {
	mu       sync.Mutex
	appender WriteAheadLogAppendI
	batcher  *Batcher[[]byte]
}

// NewGroupCommitAppender wraps the given appender, which must not be used directly anymore. The batches are limited
// to DefaultMaxGroupCommitBatchSize records and not delayed by default, both can be changed with the given options,
// see MaxBatchSize and MaxBatchWait.
func NewGroupCommitAppender(appender WriteAheadLogAppendI, opts ...BatcherOption) *GroupCommitAppender {
	g := &GroupCommitAppender{appender: appender}
	g.batcher = NewBatcher(g.commit, append([]BatcherOption{MaxBatchSize(DefaultMaxGroupCommitBatchSize)}, opts...)...)
	return g
}

func (g *GroupCommitAppender) commit(records [][]byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return AppendSyncBatch(g.appender, records)
}

// AppendSyncBatch appends all records and makes them durable with a single fsync: all but the last record are
// appended without sync, syncing the last record flushes and fsyncs all records before it. On errors, a part of the
// records may have been appended already.
func AppendSyncBatch(appender WriteAheadLogAppendI, records [][]byte) error {
	for i, record := range records {
		var err error
		if i == len(records)-1 {
			err = appender.AppendSync(record)
		} else {
			err = appender.Append(record)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Append a given record and does NOT execute fsync to guarantee the persistence of the record.
func (g *GroupCommitAppender) Append(record []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.appender.Append(record)
}

// AppendSync appends the given record and returns once it's durable, the fsync is shared with concurrent callers.
func (g *GroupCommitAppender) AppendSync(record []byte) error {
	return g.batcher.Submit(record)
}

// Rotate forces the rotation of the underlying WAL, see WriteAheadLogAppendI.Rotate.
func (g *GroupCommitAppender) Rotate() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.appender.Rotate()
}

// Close closes the underlying WAL.
func (g *GroupCommitAppender) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.appender.Close()
}
