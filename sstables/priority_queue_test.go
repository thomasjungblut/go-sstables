package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"sort"
	"testing"
)

type ListSSTableIteratorI struct {
	list         []int // every int is treated as key/value as a single element byte array
	currentIndex int
}

func (l *ListSSTableIteratorI) Next() ([]byte, []byte, error) {
	if l.currentIndex >= len(l.list) {
		return nil, nil, Done
	}
	k := []byte{byte(l.list[l.currentIndex])}
	l.currentIndex++
	return k, k, nil
}

func TestTwoListsHappyPath(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 3}, []int{2, 4})
}

func TestSingleList(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 3})
}

func TestSingleListEmpty(t *testing.T) {
	assertMergeAndListMatches(t, []int{})
}

func TestTwoListsSameItems(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 3}, []int{1, 3})
}

func TestMultiList(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 3}, []int{1, 3})
}

func TestTwoListsOneLonger(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 5, 7, 8, 9}, []int{3, 4, 6, 10, 11, 12, 13, 14, 15})
}

func TestTwoListsLeftLonger(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 5, 7, 8, 9, 25, 27, 100, 250}, []int{3, 4, 6, 10, 14, 15})
}

func TestMultiListConsecutive(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 2}, []int{3, 4}, []int{5, 6})
}

func TestMultiListConsecutiveReversed(t *testing.T) {
	assertMergeAndListMatches(t, []int{5, 6}, []int{3, 4}, []int{1, 2})
}

func TestMultiListEmptyMiddle(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 9}, []int{}, []int{5, 6})
}

func TestMultiListAllEmpty(t *testing.T) {
	assertMergeAndListMatches(t, []int{}, []int{}, []int{})
}

func assertMergeAndListMatches(t *testing.T, lists ...[]int) {
	var input []SSTableIteratorI
	var expected []int

	nonEmptyLists := 0
	for _, v := range lists {
		input = append(input, &ListSSTableIteratorI{list: v})
		expected = append(expected, v...)
		if len(v) > 0 {
			nonEmptyLists++
		}
	}

	pq := NewPriorityQueue(skiplist.BytesComparator)
	err := pq.Init(input)
	assert.Nil(t, err)
	assert.Equal(t, nonEmptyLists, pq.Len())

	var actual []int
	for {
		k, v, err := pq.Next()
		if err == Done {
			break
		}

		assert.Equal(t, 1, len(k))
		assert.Equal(t, 1, len(v))
		assert.Equal(t, k[0], v[0])

		actual = append(actual, int(k[0]))
	}

	sort.Ints(expected)
	assert.ElementsMatch(t, expected, actual)
}
