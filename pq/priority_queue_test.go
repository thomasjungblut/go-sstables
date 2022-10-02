package pq

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"sort"
	"testing"
)

type SliceIterator struct {
	list         []int // every int is treated as key/value as a single element byte array
	currentIndex int
	context      int
}

func (l *SliceIterator) Next() (int, int, error) {
	if l.currentIndex >= len(l.list) {
		return 0, 0, Done
	}

	k := l.list[l.currentIndex]
	l.currentIndex++

	return k, k + 1, nil
}

func (l *SliceIterator) Context() int {
	return l.context
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
	assertMergeAndListMatches(t, []int{1, 2}, []int{4, 6}, []int{0, 9}, []int{5, 8})
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

func TestMultiListMixed(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 5, 8, 19}, []int{2, 3, 4, 12}, []int{6, 9, 25})
}

func TestMultiListShortExhaust(t *testing.T) {
	assertMergeAndListMatches(t, []int{4, 5, 8, 19}, []int{4, 6, 9, 12}, []int{1, 2, 3})
}

func TestMultiListEmptyMiddle(t *testing.T) {
	assertMergeAndListMatches(t, []int{1, 9}, []int{}, []int{5, 6})
}

func TestMultiListAllEmpty(t *testing.T) {
	assertMergeAndListMatches(t, []int{}, []int{}, []int{})
}

func assertMergeAndListMatches(t *testing.T, lists ...[]int) {
	var iterators []IteratorWithContext[int, int, int]
	var expected []int

	for _, v := range lists {
		iterators = append(iterators, &SliceIterator{list: v, context: len(v)})
		expected = append(expected, v...)
	}

	pq, err := NewPriorityQueue[int, int, int](skiplist.OrderedComparator[int]{}, iterators)
	require.Nil(t, err)

	var actualKeys []int
	for {
		k, v, _, err := pq.Next()
		if err == Done {
			break
		}

		assert.Equal(t, k+1, v)
		actualKeys = append(actualKeys, k)
	}

	sort.Ints(expected)
	assert.Exactly(t, expected, actualKeys)
}
