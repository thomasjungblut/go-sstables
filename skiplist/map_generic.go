//go:build go1.18
// +build go1.18

// Package skiplist is a translation from LevelDBs skiplist (https://github.com/google/leveldb/blob/master/db/skiplist.h)
package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

type Comparator[T any] interface {
	// Compare contract (similar to Java):
	// < 0 when a < b
	// == 0 when a == b
	// > 0 when a > b
	Compare(a T, b T) int
}

// Done indicates an iterator has returned all items.
// https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
var Done = errors.New("no more items in iterator")
var NotFound = errors.New("key was not found")

// Ordered represents the set of types for which the '<' and '>' operator work.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | string
}

type OrderedComparator[T Ordered] struct {
}

func (OrderedComparator[T]) Compare(a T, b T) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	}
	return 0
}

type BytesComparator struct {
}

func (BytesComparator) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

type IteratorI[K any, V any] interface {
	// Next returns the next key, value in sequence
	// returns Done as the error when the iterator is exhausted
	Next() (K, V, error)
}

type Iterator[K any, V any] struct {
	comp      Comparator[K]
	node      *Node[K, V]
	keyHigher *K
	doneNext  bool
}

func (it *Iterator[K, V]) Next() (_ K, _ V, done error) {
	done = Done
	if it.node == nil || it.doneNext {
		return
	}
	cur := it.node
	it.node = it.node.Next(0)

	if it.keyHigher != nil {
		c := it.comp.Compare(cur.key, *it.keyHigher)
		if c == 0 {
			// we have reached the higher end of the range, and we return it, next iteration stops
			it.doneNext = true
		} else if c > 0 { // we're over the higher end of the range already, return immediately
			return
		}
	}

	return cur.key, cur.value, nil
}

type MapI[K any, V any] interface {
	Size() int

	// Insert key/value into the list.
	// REQUIRES: nothing that compares equal to key is currently in the list.
	Insert(key K, value V)

	// Contains returns true if an entry that compares equal to key is in the list.
	Contains(key K) bool

	// Get returns the value element that compares equal to the key supplied or returns NotFound if it does not exist.
	Get(key K) (V, error)

	// Iterator returns an iterator over the whole sorted sequence
	Iterator() (IteratorI[K, V], error)

	// IteratorStartingAt returns an iterator over the sorted sequence starting at the given key (inclusive if key is in the list).
	// Using a key that is out of the sequence range will result in either an empty iterator or the full sequence.
	IteratorStartingAt(key K) (IteratorI[K, V], error)

	// IteratorBetween Returns an iterator over the sorted sequence starting at the given keyLower (inclusive if key is in the list)
	// and until the given keyHigher was reached (inclusive if key is in the list).
	// Using keys that are out of the sequence range will result in either an empty iterator or the full sequence.
	// If keyHigher is lower than keyLower an error will be returned
	IteratorBetween(keyLower K, keyHigher K) (IteratorI[K, V], error)
}

type NodeI[K any, V any] interface {
	Next(height int) *Node[K, V]
	SetNext(height int, node *Node[K, V])
}

type Node[K any, V any] struct {
	key   K
	value V
	// array length is equal to the current node's height, next[0] is the lowest level pointer
	next []*Node[K, V]
}

func (n *Node[K, V]) Next(height int) *Node[K, V] {
	return n.next[height]
}

func (n *Node[K, V]) SetNext(height int, node *Node[K, V]) {
	n.next[height] = node
}

func newSkipListNode[K any, V any](key K, value V, maxHeight int) *Node[K, V] {
	nextNodes := make([]*Node[K, V], maxHeight)
	return &Node[K, V]{key: key, value: value, next: nextNodes}
}

func newDefaultSkipListNode[K any, V any](maxHeight int) *Node[K, V] {
	nextNodes := make([]*Node[K, V], maxHeight)
	return &Node[K, V]{next: nextNodes}
}

type Map[K any, V any] struct {
	maxHeight int
	size      int

	comp Comparator[K]
	head *Node[K, V]
}

func (list *Map[K, V]) Insert(key K, value V) {
	prevTable := make([]*Node[K, V], list.maxHeight)
	x := findGreaterOrEqual(list, key, prevTable)

	// we don't allow dupes in this data structure
	if x != nil && list.comp.Compare(key, x.key) == 0 {
		panic("duplicate key insertions are not allowed")
	}

	randomHeight := randomHeight(list.maxHeight)
	// do a re-balancing if we have reached new heights
	if randomHeight > list.maxHeight {
		for i := list.maxHeight; i < randomHeight; i++ {
			prevTable[i] = list.head
		}
		list.maxHeight = randomHeight
	}

	x = newSkipListNode(key, value, randomHeight)
	for i := 0; i < randomHeight; i++ {
		x.SetNext(i, prevTable[i].Next(i))
		prevTable[i].SetNext(i, x)
	}

	list.size++
}

func (list *Map[K, V]) Size() int {
	return list.size
}

func (list *Map[K, V]) Contains(key K) bool {
	_, err := list.Get(key)
	if err == nil {
		return true
	}
	return false
}

func (list *Map[K, V]) Get(key K) (_ V, err error) {
	err = NotFound

	x := findGreaterOrEqual(list, key, nil)
	if x != nil && list.comp.Compare(key, x.key) == 0 {
		return x.value, nil
	}

	return
}

func (list *Map[K, V]) Iterator() (IteratorI[K, V], error) {
	// we start the iterator at the next node from the head, so we can share it with the range scan below
	return &Iterator[K, V]{node: list.head.Next(0), comp: list.comp, keyHigher: nil}, nil
}

func (list *Map[K, V]) IteratorStartingAt(key K) (IteratorI[K, V], error) {
	node := findGreaterOrEqual(list, key, nil)
	return &Iterator[K, V]{node: node, comp: list.comp, keyHigher: nil}, nil
}

func (list *Map[K, V]) IteratorBetween(keyLower K, keyHigher K) (IteratorI[K, V], error) {
	node := findGreaterOrEqual(list, keyLower, nil)
	if list.comp.Compare(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}
	return &Iterator[K, V]{node: node, comp: list.comp, keyHigher: &keyHigher}, nil
}

func NewSkipListMap[K any, V any](comp Comparator[K]) MapI[K, V] {
	const maxHeight = 12
	return &Map[K, V]{head: newDefaultSkipListNode[K, V](maxHeight), comp: comp, maxHeight: maxHeight}
}

func findGreaterOrEqual[K any, V any](list *Map[K, V], key K, prevTable []*Node[K, V]) *Node[K, V] {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.Next(level)
		// check if this key is after the next node
		if next != nil && list.comp.Compare(key, next.key) > 0 {
			// keep searching in this list
			x = next
		} else {
			if prevTable != nil {
				prevTable[level] = x
			}

			if level == 0 {
				return next
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func randomHeight(maxHeight int) int {
	const branchFactor = 4
	height := 1
	for height < maxHeight && ((rand.Int() % branchFactor) == 0) {
		height++
	}

	if height <= 0 || height > maxHeight {
		panic("height was invalid")
	}

	return height
}
