package sstables

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"sort"
)

// This whole file is slightly adjusted coming from https://golang.org/pkg/container/heap/#example__priorityQueue

type PriorityQueueI interface {
	sort.Interface
	Init(iterators []SSTableIteratorI) error // initializes the heap with the initial values from the iterators
	Next() ([]byte, []byte, error)           // next key/value/error, Done is returned when all elements are exhausted
}

type Element struct {
	key       []byte
	value     []byte
	heapIndex int
	iterator  SSTableIteratorI
}

type PriorityQueue struct {
	heap []*Element
	comp skiplist.KeyComparator
}

func NewPriorityQueue(comp skiplist.KeyComparator) PriorityQueue {
	return PriorityQueue{comp: comp}
}

func (pq PriorityQueue) Len() int { return len(pq.heap) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.comp(pq.heap[i].key, pq.heap[j].key) > 0
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.heap[i], pq.heap[j] = pq.heap[j], pq.heap[i]
	pq.heap[i].heapIndex = i
	pq.heap[j].heapIndex = j
}

func (pq *PriorityQueue) Init(iterators []SSTableIteratorI) error {
	var heap []*Element

	for i, it := range iterators {
		e := &Element{heapIndex: i, iterator: it, key: nil, value: nil}
		err := fillNext(e)
		if err == nil {
			heap = append(heap, e)
		} else if err != Done {
			return err
		}
	}

	pq.heap = heap
	initHeap(pq)
	return nil
}

func (pq *PriorityQueue) Next() ([]byte, []byte, error) {
	n := len(pq.heap)
	if n == 0 {
		return nil, nil, Done
	}
	item := pq.heap[n-1]
	k := item.key
	v := item.value
	err := fillNext(item)
	if err == nil {
		fix(pq, item.heapIndex)
	} else if err == Done {
		// remove this element from the heap completely
		pq.heap[n-1] = nil
		item.heapIndex = -1
		pq.heap = pq.heap[0 : n-1]
	} else {
		return nil, nil, err
	}

	return k, v, nil
}

func fillNext(item *Element) error {
	k, v, e := item.iterator.Next()
	if e != nil {
		item.key = nil
		item.value = nil
		return e
	}

	item.key = k
	item.value = v

	return nil
}

// below is basically ripped from container/heap, to make the interface more compatible to our use case.
// sorry! Why does the interface for up/down and fix need to include the top level push/pop methods? Makes no sense.

func initHeap(h PriorityQueueI) {
	// heapify what is already in there
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

func fix(h PriorityQueueI, i int) {
	if !down(h, i, h.Len()) {
		up(h, i)
	}
}

func up(h PriorityQueueI, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h PriorityQueueI, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
