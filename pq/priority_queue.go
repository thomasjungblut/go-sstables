package pq

import (
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

// Done indicates an iterator has returned all items.
// https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
var Done = errors.New("no more items in iterator")

type IteratorWithContext[K any, V any, CTX any] interface {
	// Next returns the next key, value in sequence.
	// Returns Done as the error when the iterator is exhausted.
	Next() (K, V, error)
	// Context returns the context to identify the given iterator.
	Context() CTX
}

type PriorityQueueI[K any, V any, CTX any] interface {
	// Next returns the next key, value and context in sequence.
	// Returns Done as the error when the iterator is exhausted.
	Next() (K, V, CTX, error)
}

type Element[K any, V any, CTX any] struct {
	heapIndex int
	key       K
	value     V
	iterator  IteratorWithContext[K, V, CTX]
}

type PriorityQueue[K any, V any, CTX any] struct {
	size int
	heap []*Element[K, V, CTX]
	comp skiplist.Comparator[K]
}

func (pq *PriorityQueue[K, V, CTX]) lessThan(i, j *Element[K, V, CTX]) bool {
	return pq.comp.Compare(i.key, j.key) < 0
}

func (pq *PriorityQueue[K, V, CTX]) swap(i, j int) {
	pq.heap[i], pq.heap[j] = pq.heap[j], pq.heap[i]
	pq.heap[i].heapIndex = i
	pq.heap[j].heapIndex = j
}

func (pq *PriorityQueue[K, V, CTX]) init(iterators []IteratorWithContext[K, V, CTX]) error {
	// reserve the 0th element for nil, makes it easier to implement the rest of the logic
	pq.heap = []*Element[K, V, CTX]{nil}
	for i, it := range iterators {
		e := &Element[K, V, CTX]{heapIndex: i, iterator: it}
		err := pq.fillNext(e)
		if err == nil {
			pq.heap = append(pq.heap, e)
			pq.size++
			pq.upHeap(pq.size)
		} else if !errors.Is(err, Done) {
			return fmt.Errorf("INIT couldn't fill next heap entry: %w", err)
		}
	}

	return nil
}

func (pq *PriorityQueue[K, V, CTX]) Next() (_ K, _ V, _ CTX, err error) {
	err = Done

	if pq.size == 0 {
		return
	}
	// since we reserved index 0 for nil, the minimum element is always at index 1
	top := pq.heap[1]
	k := top.key
	v := top.value
	c := top.iterator.Context()
	err = pq.fillNext(top)
	// if we encounter a real error, we're returning immediately
	if err != nil && !errors.Is(err, Done) {
		err = fmt.Errorf("NEXT couldn't fill next heap entry: %w", err)
		return
	}

	// remove the element from the heap completely if its iterator is exhausted
	if errors.Is(err, Done) {
		// move the root away to the bottom leaf
		pq.swap(1, pq.size)
		// and chop it off the slice
		pq.heap = pq.heap[0:pq.size]
		pq.size--
	}

	// always down the heap at the end
	pq.downHeap()

	return k, v, c, nil
}

func (pq *PriorityQueue[K, V, CTX]) upHeap(i int) {
	element := pq.heap[i]
	j := i >> 1
	for j > 0 && pq.lessThan(element, pq.heap[j]) {
		pq.heap[i] = pq.heap[j]
		i = j
		j = j >> 1
	}
	pq.heap[i] = element
}

func (pq *PriorityQueue[K, V, CTX]) downHeap() {
	if pq.size == 0 {
		return
	}

	i := 1
	element := pq.heap[i]
	j := i << 1
	k := j + 1
	if k <= pq.size && pq.lessThan(pq.heap[k], pq.heap[j]) {
		j = k
	}
	for j <= pq.size && pq.lessThan(pq.heap[j], element) {
		pq.heap[i] = pq.heap[j]
		i = j
		j = i << 1
		k = j + 1
		if k <= pq.size && pq.lessThan(pq.heap[k], pq.heap[j]) {
			j = k
		}
	}
	pq.heap[i] = element
}

func (pq *PriorityQueue[K, V, CTX]) fillNext(item *Element[K, V, CTX]) error {
	k, v, e := item.iterator.Next()
	item.key = k
	item.value = v
	return e
}

func NewPriorityQueue[K any, V any, CTX any](comp skiplist.Comparator[K], iterators []IteratorWithContext[K, V, CTX]) (PriorityQueueI[K, V, CTX], error) {
	q := &PriorityQueue[K, V, CTX]{comp: comp}
	err := q.init(iterators)
	if err != nil {
		return nil, err
	}
	return q, nil
}
