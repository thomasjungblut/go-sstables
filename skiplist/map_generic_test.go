package skiplist

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"slices"
	"sort"
	"testing"
	"testing/quick"
)

func TestSkipListDefaultHandlingGenerics(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	list.Insert(0, 5)

	v, err := list.Get(0)
	assert.Nil(t, err)
	assert.Equal(t, v, 5)
}

func TestSkipListDefaultHandlingGenericsNotTound(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	v, err := list.Get(0)
	assert.Equal(t, err, NotFound)
	assert.Equal(t, v, 0)
}

func TestSkipListSingleInsertHappyPathIterator(t *testing.T) {
	list := singleElementSkipList(t)

	it, err := list.Iterator()
	assert.Nil(t, err)
	k, v, err := it.Next()
	assert.Nil(t, err)
	assert.Equal(t, 13, k)
	assert.Equal(t, 91, v)
	_, _, err = it.Next()
	assert.Equal(t, Done, err)
}

func TestSkipListSingleElementHappyPathGet(t *testing.T) {
	list := singleElementSkipList(t)
	e, err := list.Get(13)
	assert.Nil(t, err)
	assert.Equal(t, 91, e)

	_, err = list.Get(3)
	assert.Equal(t, NotFound, err)
}

func TestSkipListMultiInsertOrdered(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{1, 2, 3, 4, 5, 6, 7}, list)
}

func TestSkipListMultiInsertUnordered(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{79, 14, 91, 27, 62, 41, 58, 2, 20, 87, 34}, list)
}

func TestSkipListMultiInsertUnorderedNegatives(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{79, 14, -91, 27, 62, 41, -58, 2, -20, -87, 34}, list)
}

func TestSkipListMultiInsertZeroRun(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{2, 1, 0, -1, -2}, list)
}

func TestSkipListDoubleEqualInsert(t *testing.T) {
	assert.PanicsWithValue(t, "duplicate key insertions are not allowed", func() {
		list := NewSkipListMap[int, int](OrderedComparator[int]{})
		list.Insert(13, 91)
		list.Insert(13, 1) // should panic on duped key
	})
}

func TestSkipListEmptyIterator(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})

	assert.Equal(t, 0, list.Size())
	assert.False(t, list.Contains(1))

	// manually test the iterator
	it, err := list.Iterator()
	assert.Nil(t, err)
	_, _, err = it.Next()
	assert.Equal(t, Done, err)
}

func TestSkipListMultiInsertUnorderedStartingIterator(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{79, 14, 91, 27, 62, 41, 58, 2, 20, 87, 34}, list)
	expected := []int{2, 14, 20, 27, 34, 41, 58, 62, 79, 87, 91}
	// a lower key of the sequence should yield the whole sequence
	it, err := list.IteratorStartingAt(1)
	assert.Nil(t, err)
	assertIteratorOutputs(t, expected, it)

	// first key should also yield the whole sequence
	it, err = list.IteratorStartingAt(2)
	assert.Nil(t, err)
	assertIteratorOutputs(t, expected, it)

	// test a staggered range at each index
	for i, start := range expected {
		sliced := expected[i:]
		it, err = list.IteratorStartingAt(start)
		assert.Nil(t, err)
		assertIteratorOutputs(t, sliced, it)
	}

	// test out of range iteration, which should yield an empty iterator
	it, err = list.IteratorStartingAt(100)
	assert.Nil(t, err)
	_, _, err = it.Next()
	assert.Equal(t, Done, err)
}

func TestSkipListBetweenIterator(t *testing.T) {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	batchInsertAndAssertContains(t, []int{79, 14, 91, 27, 62, 41, 58, 2, 20, 87, 34}, list)
	expected := []int{2, 14, 20, 27, 34, 41, 58, 62, 79, 87, 91}
	// a lower/higher key of the sequence should yield the whole sequence
	it, err := list.IteratorBetween(1, 100)
	assert.Nil(t, err)
	assertIteratorOutputs(t, expected, it)

	// from 14 to 14 should only contain 14
	it, err = list.IteratorBetween(14, 14)
	assert.Nil(t, err)
	assertIteratorOutputs(t, []int{14}, it)

	// first/last key should also yield the whole sequence (inclusiveness test)
	it, err = list.IteratorBetween(2, 91)
	assert.Nil(t, err)
	assertIteratorOutputs(t, expected, it)

	// this should give an error
	it, err = list.IteratorBetween(2, 1)
	assert.NotNil(t, err)

	// test a staggered range at each index until the end exclusive
	for i, start := range expected {
		sliced := expected[i:]
		it, err = list.IteratorBetween(start, 100)
		assert.Nil(t, err)
		assertIteratorOutputs(t, sliced, it)
	}

	// test a staggered range at each index with crossing (inclusive)
	for i, start := range expected {
		it, err = list.IteratorBetween(start, expected[len(expected)-i-1])
		if i <= (len(expected) / 2) {
			assert.Nil(t, err)
			sliced := expected[i : len(expected)-i]
			assertIteratorOutputs(t, sliced, it)
		} else {
			assert.NotNil(t, err)
		}
	}

	// test out of range iteration, which should yield an empty iterator
	it, err = list.IteratorBetween(100, 200)
	assert.Nil(t, err)
	_, _, err = it.Next()
	assert.Equal(t, Done, err)
}

func TestSkipListBetweenIteratorScanOverHoles(t *testing.T) {
	list := NewSkipListMap[[]byte, []byte](BytesComparator{})
	wholeSequence := [][]byte{{0}, {1}, {2}, {4}, {8}, {9}, {10}}
	for i := 0; i < len(wholeSequence); i++ {
		list.Insert(wholeSequence[i], wholeSequence[i])
	}
	expected := [][]byte{{4}}
	it, err := list.IteratorBetween([]byte{3}, []byte{7})
	assert.Nil(t, err)

	currentIndex := 0
	for {
		k, v, err := it.Next()
		if err == Done {
			break
		}

		if err != nil {
			assert.Fail(t, "received an error while iterating, shouldn't happen")
		}

		assert.NotNil(t, k)
		assert.NotNil(t, v)

		assert.Equal(t, expected[currentIndex], k)
		assert.Equal(t, expected[currentIndex], v)
		currentIndex++
	}
	assert.Equal(t, len(expected), currentIndex)
}

func TestSkipListSortedQuick(t *testing.T) {
	err := quick.Check(func(a []int) bool {
		list := NewSkipListMap[int, int](OrderedComparator[int]{})
		for _, n := range a {
			list.Insert(n, n)
		}

		iterator, err := list.Iterator()
		if err != nil {
			t.Errorf("\nunexpected iterator err=%v", err)
			return false
		}

		result := []int{}
		for {
			k, _, err := iterator.Next()
			if err == Done {
				break
			}
			if err != nil {
				t.Errorf("\nunexpected Next err=%v", err)
				return false
			}
			result = append(result, k)
		}

		sort.Ints(a)
		if !reflect.DeepEqual(a, result) {
			t.Errorf("\nexp=%+v\ngot=%+v\n", a, result)
			return false
		}

		return true
	}, nil)
	require.NoError(t, err)
}

func TestSkipListRangeScanQuick(t *testing.T) {
	err := quick.Check(func(a []int, beginKey, endKey int) bool {
		if beginKey > endKey {
			beginKey, endKey = endKey, beginKey
		}

		list := NewSkipListMap[int, int](OrderedComparator[int]{})
		for _, n := range a {
			list.Insert(n, n)
		}

		iterator, err := list.IteratorBetween(beginKey, endKey)
		if err != nil {
			t.Errorf("\nunexpected iterator err=%v", err)
			return false
		}

		result := []int{}
		for {
			k, _, err := iterator.Next()
			if err == Done {
				break
			}
			if err != nil {
				t.Errorf("\nunexpected Next err=%v", err)
				return false
			}
			result = append(result, k)
		}

		sort.Ints(a)
		beginIndex, _ := slices.BinarySearch(a, beginKey)
		endIndex, _ := slices.BinarySearch(a, endKey)

		if !reflect.DeepEqual(a[beginIndex:endIndex], result) {
			t.Errorf("\nexp=%+v\ngot=%+v\n", a, result)
			return false
		}

		return true
	}, nil)
	require.NoError(t, err)
}

func singleElementSkipList(t *testing.T) MapI[int, int] {
	list := NewSkipListMap[int, int](OrderedComparator[int]{})
	list.Insert(13, 91)
	assert.Equal(t, 1, list.Size())
	assert.True(t, list.Contains(13))
	assert.False(t, list.Contains(1))
	return list
}

func assertIteratorOutputs(t *testing.T, expectedSeq []int, it IteratorI[int, int]) {
	currentIndex := 0
	for {
		k, v, err := it.Next()
		if err == Done {
			break
		}

		if err != nil {
			assert.Fail(t, "received an error while iterating, shouldn't happen")
		}

		assert.NotNil(t, k)
		assert.NotNil(t, v)

		assert.Equal(t, expectedSeq[currentIndex], k)
		assert.Equal(t, expectedSeq[currentIndex]+1, v)
		currentIndex++
	}

	// test whether we have actually read that much from the iterator
	assert.Equal(t, len(expectedSeq), currentIndex)
}

func batchInsertAndAssertContains(t *testing.T, toInsert []int, list MapI[int, int]) {
	for _, e := range toInsert {
		list.Insert(e, e+1)
	}
	assert.Equal(t, len(toInsert), list.Size())
	for _, e := range toInsert {
		v, err := list.Get(e)
		assert.Nil(t, err)
		assert.Equal(t, e+1, v)
		assert.True(t, list.Contains(e))
	}

	sort.Ints(toInsert)
	it, err := list.Iterator()
	assert.Nil(t, err)
	assertIteratorOutputs(t, toInsert, it)
}
