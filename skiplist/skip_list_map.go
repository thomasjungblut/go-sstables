// Package skiplist is basically a translation from LevelDBs skiplist (https://github.com/google/leveldb/blob/master/db/skiplist.h)
package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

// KeyComparator Typical comparator contract (similar to Java):
// < 0 when a < b
// == 0 when a == b
// > 0 when a > b
// type KeyComparator func[T constraints.Ordered](a T, b T) int
type KeyComparator func(a interface{}, b interface{}) int

// Done iterator pattern as described in https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
var Done = errors.New("no more items in iterator")
var NotFound = errors.New("key was not found")

// BytesComparator example comparator for plain byte arrays
func BytesComparator(a interface{}, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// IntComparator example comparator for plain integers
func IntComparator(a interface{}, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)

	if aInt > bInt {
		return 1
	} else if aInt < bInt {
		return -1
	}

	return 0
}

type SkipListIteratorI[K comparable, V any] interface {
	// Next returns the next key, value in sequence
	// returns Done as the error when the iterator is exhausted
	Next() (K, V, error)
}

type SkipListIterator[K comparable, V any] struct {
	comp      KeyComparator
	node      *SkipListNode[K, V]
	keyHigher K
	doneNext  bool
}

// Next TODO(thomas): running heavily into
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#the-zero-value
func (it *SkipListIterator[K, V]) Next() (K, V, error) {
	if it.node == nil || it.doneNext {
		return nil, nil, Done
	}
	cur := it.node
	it.node = it.node.Next(0)
	var defaultK K
	if it.keyHigher != defaultK {
		c := it.comp(cur.key, it.keyHigher)
		if c == 0 {
			// we have reached the higher end of the range, and we return it, next iteration stops
			it.doneNext = true
		} else if c > 0 { // we're over the higher end of the range already, return immediately
			return nil, nil, Done
		}
	}

	return cur.key, cur.value, nil
}

type SkipListMapI[K comparable, V any] interface {
	Size() int

	// Insert key/value into the list.
	// REQUIRES: nothing that compares equal to key is currently in the list.
	Insert(key K, value V)

	// Contains Returns true if an entry that compares equal to key is in the list.
	Contains(key K) bool

	// Get Returns the value element that compares equal to the key supplied or returns NotFound if it does not exist.
	Get(key K) (V, error)

	// Iterator Returns an iterator over the whole sorted sequence
	Iterator() (SkipListIteratorI[K, V], error)

	// IteratorStartingAt Returns an iterator over the sorted sequence starting at the given key (inclusive if key is in the list).
	// Using a key that is out of the sequence range will result in either an empty iterator or the full sequence.
	IteratorStartingAt(key K) (SkipListIteratorI[K, V], error)

	// IteratorBetween Returns an iterator over the sorted sequence starting at the given keyLower (inclusive if key is in the list)
	// and until the given keyHigher was reached (inclusive if key is in the list).
	// Using keys that are out of the sequence range will result in either an empty iterator or the full sequence.
	// If keyHigher is lower than keyLower an error will be returned
	IteratorBetween(keyLower K, keyHigher K) (SkipListIteratorI[K, V], error)
}

type SkipListNodeI[K comparable, V any] interface {
	Next(height int) *SkipListNode[K, V]
	SetNext(height int, node *SkipListNode[K, V])
}

type SkipListNode[K comparable, V any] struct {
	key   K
	value V
	// array length is equal to the current node's height, next[0] is the lowest level pointer
	next []*SkipListNode[K, V]
}

func (n *SkipListNode[K, V]) Next(height int) *SkipListNode[K, V] {
	return n.next[height]
}

func (n *SkipListNode[K, V]) SetNext(height int, node *SkipListNode[K, V]) {
	n.next[height] = node
}

// newSkipListNode TODO(thomas): running into issue
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#no-parameterized-methods
func newSkipListNode[K comparable, V](key K, value V, maxHeight int) *SkipListNode[K, V] {
	nextNodes := make([]*SkipListNode[K, V], maxHeight)
	return &SkipListNode[K, V]{key: key, value: value, next: nextNodes}
}

type SkipListMap[K comparable, V any] struct {
	maxHeight int
	size      int

	comp KeyComparator
	head *SkipListNode[K, V]
}

func (list *SkipListMap[K, V]) Insert(key K, value V) {
	prevTable := make([]*SkipListNode[K, V], list.maxHeight)
	x := list.findGreaterOrEqualPrevTable(key, prevTable)

	// we don't allow dupes in this data structure
	if x != nil && list.comp(key, x.key) == 0 {
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

func (list *SkipListMap[K, V]) Size() int {
	return list.size
}

func (list *SkipListMap[K, V]) Contains(key K) bool {
	_, err := list.Get(key)
	if err == nil {
		return true
	}
	return false
}

func (list *SkipListMap[K, V]) Get(key K) (V, error) {
	x := list.findGreaterOrEqual(key)
	if x != nil && list.comp(key, x.key) == 0 {
		return x.value, nil
	}

	return nil, NotFound
}

func (list *SkipListMap[K, V]) Iterator() (SkipListIteratorI[K, V], error) {
	// we start the iterator at the next node from the head, so we can share it with the range scan below
	return &SkipListIterator[K, V]{node: list.head.Next(0), comp: list.comp}, nil
}

func (list *SkipListMap[K, V]) IteratorStartingAt(key K) (SkipListIteratorI[K, V], error) {
	node := list.findGreaterOrEqual(key)
	return &SkipListIterator[K, V]{node: node, comp: list.comp}, nil
}

func (list *SkipListMap[K, V]) IteratorBetween(keyLower K, keyHigher K) (SkipListIteratorI[K, V], error) {
	node := list.findGreaterOrEqual(keyLower)
	if list.comp(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}
	return &SkipListIterator[K, V]{node: node, comp: list.comp, keyHigher: keyHigher}, nil
}

func (list *SkipListMap[K, V]) findGreaterOrEqual(key K) *SkipListNode[K, V] {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.Next(level)
		// check if this key is after the next node
		if next != nil && list.comp(key, next.key) > 0 {
			// keep searching in this list
			x = next
		} else {
			if level == 0 {
				return next
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func (list *SkipListMap[K, V]) findGreaterOrEqualPrevTable(key K, prevTable []*SkipListNode[K, V]) *SkipListNode[K, V] {
	x := list.head
	level := list.maxHeight - 1
	for {
		next := x.Next(level)
		// check if this key is after the next node
		if next != nil && list.comp(key, next.key) > 0 {
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

func NewSkipListMap[K, V](comp KeyComparator) SkipListMapI[K, V] {
	const maxHeight = 12
	next := make([]*SkipListNode[K, V], maxHeight)
	SkipListNode{next: next}
	return &SkipListMap[K, V]{head: newSkipListNode(nil, nil, maxHeight), comp: comp, maxHeight: maxHeight}
}

func randomHeight(maxHeight int) int {
	const branchFactor = 4
	height := 1
	// TODO(thomas): is the globalRand here actually a bottleneck?
	for height < maxHeight && ((rand.Int() % branchFactor) == 0) {
		height++
	}

	if height <= 0 || height > maxHeight {
		panic("height was invalid")
	}

	return height
}
