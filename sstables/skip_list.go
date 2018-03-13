// basically a translation from LevelDBs skiplist (https://github.com/google/leveldb/blob/master/db/skiplist.h)
package sstables

import "math/rand"

// Typical comparator contract (similar to Java):
// < 0 when a < b
// == 0 when a == b
// > 0 when a > b
type KeyComparator func(a interface{}, b interface{}) int

type SkipListI interface {
	Size() int

	// Insert key into the list.
	// REQUIRES: nothing that compares equal to key is currently in the list.
	Insert(key interface{})

	// Returns true if an entry that compares equal to key is in the list.
	Contains(key interface{}) bool

	// TODO(thomas): sorted iterator
}

type SkipListNodeI interface {
	Next(height int) *SkipListNode
	SetNext(height int, node *SkipListNode)
}

type SkipListNode struct {
	key interface{}
	// array length is equal to the current nodes height, next[0] is the lowest level pointer
	next []*SkipListNode
}

func (n *SkipListNode) Next(height int) *SkipListNode {
	return n.next[height]
}

func (n *SkipListNode) SetNext(height int, node *SkipListNode) {
	n.next[height] = node
}

func newSkipListNode(key interface{}, maxHeight int) *SkipListNode {
	nextNodes := make([]*SkipListNode, maxHeight)
	return &SkipListNode{key: key, next: nextNodes}
}

type SkipList struct {
	maxHeight int
	size      int

	comp KeyComparator
	head *SkipListNode
}

func (list *SkipList) Insert(key interface{}) {
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

	x = newSkipListNode(key, randomHeight)
	for i := 0; i < randomHeight; i++ {
		x.SetNext(i, prevTable[i].Next(i))
		prevTable[i].SetNext(i, x)
	}

	list.size++
}

func (list *SkipList) Size() int {
	return list.size
}

func (list *SkipList) Contains(key interface{}) bool {
	x := findGreaterOrEqual(list, key, nil)
	if x != nil && list.comp(key, x.key) == 0 {
		return true
	} else {
		return false
	}
	return false
}

func NewSkipList(comp KeyComparator) SkipList {
	const maxHeight = 12
	return SkipList{head: newSkipListNode(nil, maxHeight), comp: comp, maxHeight: maxHeight}
}

func findGreaterOrEqual(list *SkipList, key interface{}, prevTable []*SkipListNode) *SkipListNode {
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

	panic("should never happen")
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
