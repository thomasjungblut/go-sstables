//go:build !go1.18
// +build !go1.18

package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

// Typical comparator contract (similar to Java):
// < 0 when a < b
// == 0 when a == b
// > 0 when a > b
type KeyComparator func(a interface{}, b interface{}) int

// iterator pattern as described in https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
var Done = errors.New("no more items in iterator")
var NotFound = errors.New("key was not found")

// example comparator for plain byte arrays
func BytesComparator(a interface{}, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// example comparator for plain integers
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

type SkipListIteratorI interface {
	// returns the next key, value in sequence
	// returns Done as the error when the iterator is exhausted
	Next() (interface{}, interface{}, error)
}

type SkipListIterator struct {
	comp      KeyComparator
	node      *SkipListNode
	keyHigher interface{}
	doneNext  bool
}

func (it *SkipListIterator) Next() (interface{}, interface{}, error) {
	if it.node == nil || it.doneNext {
		return nil, nil, Done
	}
	cur := it.node
	it.node = it.node.Next(0)

	if it.keyHigher != nil {
		c := it.comp(cur.key, it.keyHigher)
		if c == 0 {
			// we have reached the higher end of the range and we return it, next iteration stops
			it.doneNext = true
		} else if c > 0 { // we're over the higher end of the range already, return immediately
			return nil, nil, Done
		}
	}

	return cur.key, cur.value, nil
}

type SkipListMapI interface {
	Size() int

	// Insert key/value into the list.
	// REQUIRES: nothing that compares equal to key is currently in the list.
	Insert(key interface{}, value interface{})

	// Returns true if an entry that compares equal to key is in the list.
	Contains(key interface{}) bool

	// Returns the value element that compares equal to the key supplied or returns NotFound if it does not exist.
	Get(key interface{}) (interface{}, error)

	// Returns an iterator over the whole sorted sequence
	Iterator() (SkipListIteratorI, error)

	// Returns an iterator over the sorted sequence starting at the given key (inclusive if key is in the list).
	// Using a key that is out of the sequence range will result in either an empty iterator or the full sequence.
	IteratorStartingAt(key interface{}) (SkipListIteratorI, error)

	// Returns an iterator over the sorted sequence starting at the given keyLower (inclusive if key is in the list)
	// and until the given keyHigher was reached (inclusive if key is in the list).
	// Using keys that are out of the sequence range will result in either an empty iterator or the full sequence.
	// If keyHigher is lower than keyLower an error will be returned
	IteratorBetween(keyLower interface{}, keyHigher interface{}) (SkipListIteratorI, error)
}

type SkipListNodeI interface {
	Next(height int) *SkipListNode
	SetNext(height int, node *SkipListNode)
}

type SkipListNode struct {
	key   interface{}
	value interface{}
	// array length is equal to the current nodes height, next[0] is the lowest level pointer
	next []*SkipListNode
}

func (n *SkipListNode) Next(height int) *SkipListNode {
	return n.next[height]
}

func (n *SkipListNode) SetNext(height int, node *SkipListNode) {
	n.next[height] = node
}

func newSkipListNode(key interface{}, value interface{}, maxHeight int) *SkipListNode {
	nextNodes := make([]*SkipListNode, maxHeight)
	return &SkipListNode{key: key, value: value, next: nextNodes}
}

type SkipListMap struct {
	maxHeight int
	size      int

	comp KeyComparator
	head *SkipListNode
}

func (list *SkipListMap) Insert(key interface{}, value interface{}) {
	prevTable := make([]*SkipListNode, list.maxHeight)
	x := findGreaterOrEqual(list, key, prevTable)

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

func (list *SkipListMap) Size() int {
	return list.size
}

func (list *SkipListMap) Contains(key interface{}) bool {
	_, err := list.Get(key)
	if err == nil {
		return true
	}
	return false
}

func (list *SkipListMap) Get(key interface{}) (interface{}, error) {
	x := findGreaterOrEqual(list, key, nil)
	if x != nil && list.comp(key, x.key) == 0 {
		return x.value, nil
	}

	return nil, NotFound
}

func (list *SkipListMap) Iterator() (SkipListIteratorI, error) {
	// we start the iterator at the next node from the head, so we can share it with the range scan below
	return &SkipListIterator{node: list.head.Next(0), comp: list.comp, keyHigher: nil}, nil
}

func (list *SkipListMap) IteratorStartingAt(key interface{}) (SkipListIteratorI, error) {
	node := findGreaterOrEqual(list, key, nil)
	return &SkipListIterator{node: node, comp: list.comp, keyHigher: nil}, nil
}

func (list *SkipListMap) IteratorBetween(keyLower interface{}, keyHigher interface{}) (SkipListIteratorI, error) {
	node := findGreaterOrEqual(list, keyLower, nil)
	if list.comp(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}
	return &SkipListIterator{node: node, comp: list.comp, keyHigher: keyHigher}, nil
}

func NewSkipListMap(comp KeyComparator) SkipListMapI {
	const maxHeight = 12
	return &SkipListMap{head: newSkipListNode(nil, nil, maxHeight), comp: comp, maxHeight: maxHeight}
}

func findGreaterOrEqual(list *SkipListMap, key interface{}, prevTable []*SkipListNode) *SkipListNode {
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
