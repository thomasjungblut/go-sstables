package wal

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchRecorder records the committed batches, optionally blocking the commits until released.
type batchRecorder struct {
	mu      sync.Mutex
	batches [][]int
	delay   time.Duration
	release chan struct{}
	errs    map[int]error // error to return for the batch at the given index
}

func (r *batchRecorder) commit(batch []int) error {
	if r.release != nil {
		<-r.release
	}
	time.Sleep(r.delay)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, append([]int(nil), batch...))
	return r.errs[len(r.batches)-1]
}

func (r *batchRecorder) recorded() [][]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.batches
}

// queueInOrder submits the items one after another in separate goroutines, waiting until each one is queued.
func queueInOrder(t *testing.T, b *Batcher[int], items []int, results map[int]error, resultsMu *sync.Mutex, wg *sync.WaitGroup) {
	for _, item := range items {
		wg.Add(1)
		go func(item int) {
			defer wg.Done()
			err := b.Submit(item)
			resultsMu.Lock()
			results[item] = err
			resultsMu.Unlock()
		}(item)

		require.Eventually(t, func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			for _, p := range b.pending {
				if p.item == item {
					return true
				}
			}
			return false
		}, time.Second, time.Millisecond)
	}
}

func TestBatcherBatchesConcurrentSubmits(t *testing.T) {
	recorder := &batchRecorder{delay: 20 * time.Millisecond}
	b := NewBatcher(recorder.commit)

	numItems := 50
	var wg sync.WaitGroup
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, b.Submit(i))
		}(i)
	}
	wg.Wait()

	batches := recorder.recorded()
	assert.Less(t, len(batches), numItems/2)
	var committed []int
	for _, batch := range batches {
		committed = append(committed, batch...)
	}
	sort.Ints(committed)
	for i := 0; i < numItems; i++ {
		assert.Equal(t, i, committed[i], "every item must be committed exactly once")
	}
}

func TestBatcherCommitsInSubmitOrderWithLeaderFirst(t *testing.T) {
	recorder := &batchRecorder{release: make(chan struct{})}
	b := NewBatcher(recorder.commit)

	results := map[int]error{}
	var resultsMu sync.Mutex
	var wg sync.WaitGroup
	// 0 becomes the leader and blocks in its commit, 1 to 4 queue up behind it
	queueInOrder(t, b, []int{0, 1, 2, 3, 4}, results, &resultsMu, &wg)
	close(recorder.release)
	wg.Wait()

	assert.Equal(t, [][]int{{0}, {1, 2, 3, 4}}, recorder.recorded())
}

func TestBatcherMaxBatchSize(t *testing.T) {
	recorder := &batchRecorder{release: make(chan struct{})}
	b := NewBatcher(recorder.commit, MaxBatchSize(2))

	results := map[int]error{}
	var resultsMu sync.Mutex
	var wg sync.WaitGroup
	queueInOrder(t, b, []int{0, 1, 2, 3, 4, 5}, results, &resultsMu, &wg)
	close(recorder.release)
	wg.Wait()

	assert.Equal(t, [][]int{{0}, {1, 2}, {3, 4}, {5}}, recorder.recorded())
}

func TestBatcherErrorOnlyReachesItsBatch(t *testing.T) {
	commitErr := errors.New("fsync failed")
	recorder := &batchRecorder{release: make(chan struct{}), errs: map[int]error{1: commitErr}}
	b := NewBatcher(recorder.commit, MaxBatchSize(2))

	results := map[int]error{}
	var resultsMu sync.Mutex
	var wg sync.WaitGroup
	queueInOrder(t, b, []int{0, 1, 2, 3}, results, &resultsMu, &wg)
	close(recorder.release)
	wg.Wait()

	require.Equal(t, [][]int{{0}, {1, 2}, {3}}, recorder.recorded())
	assert.NoError(t, results[0])
	assert.ErrorIs(t, results[1], commitErr)
	assert.ErrorIs(t, results[2], commitErr)
	assert.NoError(t, results[3])
}

// submitStaggered submits 0 and, after the given gap, 1 from two goroutines
func submitStaggered(t *testing.T, b *Batcher[int], gap time.Duration) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, b.Submit(0))
	}()
	go func() {
		defer wg.Done()
		time.Sleep(gap)
		assert.NoError(t, b.Submit(1))
	}()
	wg.Wait()
}

func TestBatcherMaxBatchWaitWaitsForMoreItems(t *testing.T) {
	withoutWait := &batchRecorder{}
	submitStaggered(t, NewBatcher(withoutWait.commit), 20*time.Millisecond)
	assert.Equal(t, [][]int{{0}, {1}}, withoutWait.recorded())

	withWait := &batchRecorder{}
	submitStaggered(t, NewBatcher(withWait.commit, MaxBatchWait(time.Second)), 20*time.Millisecond)
	assert.Equal(t, [][]int{{0, 1}}, withWait.recorded())
}

func TestBatcherMaxBatchWaitStopsWaitingWhenFull(t *testing.T) {
	recorder := &batchRecorder{}
	b := NewBatcher(recorder.commit, MaxBatchSize(2), MaxBatchWait(10*time.Second))

	start := time.Now()
	submitStaggered(t, b, 20*time.Millisecond)
	assert.Less(t, time.Since(start), 5*time.Second, "a full batch must not wait for the delay")
	assert.Equal(t, [][]int{{0, 1}}, recorder.recorded())
}

func TestBatcherMaxBatchWaitDelaysSingleItem(t *testing.T) {
	recorder := &batchRecorder{}
	delay := 100 * time.Millisecond
	b := NewBatcher(recorder.commit, MaxBatchWait(delay))

	start := time.Now()
	require.NoError(t, b.Submit(0))
	assert.GreaterOrEqual(t, time.Since(start), delay, "a single item waits the whole delay for more items")
	assert.Equal(t, [][]int{{0}}, recorder.recorded())
}

func TestBatcherNonPositiveMaxBatchWaitDisablesWaiting(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		recorder := &batchRecorder{}
		submitStaggered(t, NewBatcher(recorder.commit, MaxBatchWait(d)), 20*time.Millisecond)
		assert.Equal(t, [][]int{{0}, {1}}, recorder.recorded(), "delay %v", d)
	}
}
