package sstables

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func IntComp(a interface{}, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)

	if aInt > bInt {
		return 1
	} else if aInt < bInt {
		return -1
	}

	return 0
}

func TestSkipListSingleInsertHappyPath(t *testing.T) {
	list := NewSkipList(IntComp)
	list.Insert(13)

	assert.Equal(t, 1, list.Size())
	assert.True(t, list.Contains(13))
	assert.False(t, list.Contains(1))
}

func TestSkipListMultiInsertOrdered(t *testing.T) {
	list := NewSkipList(IntComp)
	batchInsertAndAssertContains(t, []int{1, 2, 3, 4, 5, 6, 7}, &list)
}

func TestSkipListMultiInsertUnordered(t *testing.T) {
	list := NewSkipList(IntComp)
	batchInsertAndAssertContains(t, []int{79, 14, 91, 27, 62, 41, 58, 2, 20, 87, 34}, &list)
}

func TestSkipListDoubleEqualInsert(t *testing.T) {
	assert.PanicsWithValue(t, "duplicate key insertions are not allowed", func() {
		list := NewSkipList(IntComp)
		list.Insert(13)
		list.Insert(13) // should panic on duped key
	})
}

func batchInsertAndAssertContains(t *testing.T, toInsert []int, list *SkipList) {
	for _, e := range toInsert {
		list.Insert(e)
	}
	assert.Equal(t, len(toInsert), list.Size())
	for _, e := range toInsert {
		assert.True(t, list.Contains(e))
	}
}
