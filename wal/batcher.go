package wal

import (
	"slices"
	"sync"
	"time"
)

// Batcher implements group commit, similar to the write queue in LevelDB: concurrent callers submit items and the
// first caller in the queue becomes the leader. The leader commits all queued items (up to the maximum batch size) in
// a single call of the commit function, while the others wait for it. After the commit, all items of the batch return
// the result of the commit and the next caller in the queue becomes the leader.
//
// This amortizes expensive operations like an fsync over all concurrent callers: with a WAL, the commit function
// would append all records of the batch and fsync only once. While a leader commits, the next batch queues up behind
// it, so the batches grow with the load without any waiting. Optionally, a leader can wait for more items before
// committing, see MaxBatchWait.
//
// The commit function is only ever called by one leader at a time, with the items in the order they were submitted.
// The first item of a batch is always the item of the leader.
type Batcher[T any] struct {
	commit func(batch []T) error
	opts   batcherOptions

	mu      sync.Mutex
	cond    *sync.Cond
	pending []*batchItem[T]
	// leaderWaiting is closed to wake up a leader that waits for more items, once the batch is full
	leaderWaiting chan struct{}
}

type batchItem[T any] struct {
	item T
	err  error
	done bool
}

type batcherOptions struct {
	maxBatchSize int
	maxBatchWait time.Duration
}

type BatcherOption func(*batcherOptions)

// MaxBatchSize limits the number of items that are committed together, zero or less doesn't limit the batch size.
func MaxBatchSize(n int) BatcherOption {
	return func(o *batcherOptions) {
		o.maxBatchSize = n
	}
}

// MaxBatchWait is the timeout for which a leader waits for more items before committing. It commits earlier once the
// batch is full, but never waits longer than the timeout, even if no other item arrives. This can increase the batch
// sizes when the items arrive slower than a commit takes, but also delays every commit, even of a single item.
// Zero or less disables waiting, which is the default.
func MaxBatchWait(d time.Duration) BatcherOption {
	return func(o *batcherOptions) {
		o.maxBatchWait = d
	}
}

// NewBatcher creates a new Batcher that commits the batches with the given function. By default, batches are not
// limited in size and not delayed.
func NewBatcher[T any](commit func(batch []T) error, opts ...BatcherOption) *Batcher[T] {
	b := &Batcher[T]{commit: commit}
	for _, opt := range opts {
		opt(&b.opts)
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Submit queues the item for the next batch and returns once its batch was committed, with the error of the commit.
func (b *Batcher[T]) Submit(item T) error {
	self := b.enqueue(item)
	if leader := b.awaitTurn(self); !leader {
		// another leader committed the item already
		return self.err
	}

	batch := b.collectBatch()
	err := b.commit(itemsOf(batch))
	b.complete(batch, err)
	return err
}

// enqueue appends the item to the queue, and wakes up a leader that waits for more items once the batch is full.
func (b *Batcher[T]) enqueue(item T) *batchItem[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	self := &batchItem[T]{item: item}
	b.pending = append(b.pending, self)
	if b.leaderWaiting != nil && b.batchFull() {
		close(b.leaderWaiting)
		b.leaderWaiting = nil
	}
	return self
}

// awaitTurn blocks until the item was committed by another leader (false) or it becomes the leader itself (true).
func (b *Batcher[T]) awaitTurn(self *batchItem[T]) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for !self.done && b.pending[0] != self {
		b.cond.Wait()
	}
	return !self.done
}

// collectBatch takes the items of the next batch from the front of the queue, after waiting for more items if a delay
// is configured. They stay queued until complete, so that later items can't become leaders in the meantime.
func (b *Batcher[T]) collectBatch() []*batchItem[T] {
	if b.opts.maxBatchWait > 0 {
		b.waitForMoreItems()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(b.pending)
	if b.opts.maxBatchSize > 0 {
		n = min(n, b.opts.maxBatchSize)
	}
	return slices.Clone(b.pending[:n])
}

// waitForMoreItems waits until the batch is full or the maximum delay passed, whatever comes first.
func (b *Batcher[T]) waitForMoreItems() {
	b.mu.Lock()
	if b.batchFull() {
		b.mu.Unlock()
		return
	}
	waiting := make(chan struct{})
	b.leaderWaiting = waiting
	b.mu.Unlock()

	timer := time.NewTimer(b.opts.maxBatchWait)
	defer timer.Stop()
	select {
	case <-waiting:
	case <-timer.C:
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.leaderWaiting == waiting {
		b.leaderWaiting = nil
	}
}

// complete marks the items of the batch as done with the result of the commit, removes them from the queue and wakes
// up the others: the waiting items of the batch return and the next item in the queue becomes the leader.
func (b *Batcher[T]) complete(batch []*batchItem[T], err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range batch {
		p.err = err
		p.done = true
	}
	// drop the references to the committed items, the backing array is reused by later appends
	clear(b.pending[:len(batch)])
	b.pending = b.pending[len(batch):]
	b.cond.Broadcast()
}

func (b *Batcher[T]) batchFull() bool {
	return b.opts.maxBatchSize > 0 && len(b.pending) >= b.opts.maxBatchSize
}

func itemsOf[T any](batch []*batchItem[T]) []T {
	items := make([]T, len(batch))
	for i, p := range batch {
		items[i] = p.item
	}
	return items
}
