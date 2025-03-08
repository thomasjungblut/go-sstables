package sstables

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"reflect"
	"testing"
)

var indexLoaders = []func() IndexLoader{
	func() IndexLoader {
		return &SkipListIndexLoader{
			KeyComparator:  skiplist.BytesComparator{},
			ReadBufferSize: 4096,
		}
	},
	func() IndexLoader {
		return &SliceKeyIndexLoader{ReadBufferSize: 4096}
	},
}

func TestIndexContains(t *testing.T) {
	for _, loaderFunc := range indexLoaders {
		loader := loaderFunc()
		t.Run(reflect.TypeOf(loader).String(), func(t *testing.T) {
			idx, err := loader.Load("test_files/SimpleWriteHappyPathSSTableWithMetaData/index.rio", &proto.MetaData{})
			require.NoError(t, err)

			assert.False(t, idx.Contains([]byte{}))
			assert.False(t, idx.Contains([]byte{1}))
			assert.False(t, idx.Contains([]byte{1, 2, 3}))

			expected := []int{1, 2, 3, 4, 5, 6, 7}
			for _, i := range expected {
				k, _ := getKeyValueAsBytes(i)
				assert.True(t, idx.Contains(k))
			}
		})
	}
}

func TestIndexGet(t *testing.T) {
	for _, loaderFunc := range indexLoaders {
		loader := loaderFunc()
		t.Run(reflect.TypeOf(loader).String(), func(t *testing.T) {
			idx, err := loader.Load("test_files/SimpleWriteHappyPathSSTableWithMetaData/index.rio", &proto.MetaData{})
			require.NoError(t, err)

			v, err := idx.Get([]byte{})
			require.Equal(t, skiplist.NotFound, err)
			require.Equal(t, IndexVal{}, v)

			v, err = idx.Get([]byte{1})
			require.Equal(t, skiplist.NotFound, err)
			require.Equal(t, IndexVal{}, v)

			v, err = idx.Get([]byte{1, 2, 3})
			require.Equal(t, skiplist.NotFound, err)
			require.Equal(t, IndexVal{}, v)

			expected := []int{1, 2, 3, 4, 5, 6, 7}
			for _, i := range expected {
				k, _ := getKeyValueAsBytes(i)
				v, err := idx.Get(k)
				require.NoError(t, err)
				require.NotNil(t, v)
			}
		})
	}
}

func TestIndexIterator(t *testing.T) {
	for _, loaderFunc := range indexLoaders {
		loader := loaderFunc()
		t.Run(reflect.TypeOf(loader).String(), func(t *testing.T) {
			idx, err := loader.Load("test_files/SimpleWriteHappyPathSSTableWithMetaData/index.rio", &proto.MetaData{})
			require.NoError(t, err)

			it, err := idx.Iterator()
			require.NoError(t, err)
			expected := []int{1, 2, 3, 4, 5, 6, 7}
			assertIndexIteratorMatchesSlice(t, it, expected)
		})
	}
}

func TestIndexIteratorStartingAt(t *testing.T) {
	for _, loaderFunc := range indexLoaders {
		loader := loaderFunc()
		t.Run(reflect.TypeOf(loader).String(), func(t *testing.T) {
			idx, err := loader.Load("test_files/SimpleWriteHappyPathSSTableWithMetaData/index.rio", &proto.MetaData{})
			require.NoError(t, err)
			expected := []int{1, 2, 3, 4, 5, 6, 7}
			// whole sequence when out of bounds to the left
			it, err := idx.IteratorStartingAt(intToByteSlice(0))
			require.Nil(t, err)
			assertIndexIteratorMatchesSlice(t, it, expected)

			// staggered test
			for i, start := range expected {
				sliced := expected[i:]
				it, err := idx.IteratorStartingAt(intToByteSlice(start))
				require.Nil(t, err)
				assertIndexIteratorMatchesSlice(t, it, sliced)
			}

			// test out of range iteration, which should yield an empty iterator
			it, err = idx.IteratorStartingAt(intToByteSlice(10))
			require.Nil(t, err)
			k, v, err := it.Next()
			require.Nil(t, k)
			require.Equal(t, IndexVal{}, v)
			require.Equal(t, Done, err)
		})
	}
}

func TestIndexIteratorBetween(t *testing.T) {
	for _, loaderFunc := range indexLoaders {
		loader := loaderFunc()
		t.Run(reflect.TypeOf(loader).String(), func(t *testing.T) {
			idx, err := loader.Load("test_files/SimpleWriteHappyPathSSTableWithMetaData/index.rio", &proto.MetaData{})
			require.NoError(t, err)
			expected := []int{1, 2, 3, 4, 5, 6, 7}
			// whole sequence when out of bounds to the left and right
			it, err := idx.IteratorBetween(intToByteSlice(0), intToByteSlice(10))
			require.Nil(t, err)
			assertIndexIteratorMatchesSlice(t, it, expected)

			// whole sequence when in bounds for inclusiveness
			it, err = idx.IteratorBetween(intToByteSlice(1), intToByteSlice(7))
			require.Nil(t, err)
			assertIndexIteratorMatchesSlice(t, it, expected)

			// only 4 when requesting between 4 and 4
			it, err = idx.IteratorBetween(intToByteSlice(4), intToByteSlice(4))
			require.Nil(t, err)
			assertIndexIteratorMatchesSlice(t, it, []int{4})

			// error when higher key and lower key are inconsistent
			_, err = idx.IteratorBetween(intToByteSlice(1), intToByteSlice(0))
			require.Error(t, err)

			// staggered test with end outside of range
			for i, start := range expected {
				sliced := expected[i:]
				it, err := idx.IteratorBetween(intToByteSlice(start), intToByteSlice(10))
				require.Nil(t, err)
				assertIndexIteratorMatchesSlice(t, it, sliced)
			}

			// staggered test with end crossing to the left
			for i, start := range expected {
				it, err := idx.IteratorBetween(intToByteSlice(start), intToByteSlice(expected[len(expected)-i-1]))
				if i <= (len(expected) / 2) {
					require.Nil(t, err)
					sliced := expected[i : len(expected)-i]
					assertIndexIteratorMatchesSlice(t, it, sliced)
				} else {
					require.Error(t, err)
				}

			}

			// test out of range iteration, which should yield an empty iterator
			it, err = idx.IteratorBetween(intToByteSlice(10), intToByteSlice(100))
			require.Nil(t, err)
			k, v, err := it.Next()
			require.Nil(t, k)
			require.Equal(t, IndexVal{}, v)
			require.Equal(t, Done, err)
		})
	}
}

func assertIndexIteratorMatchesSlice(t *testing.T, it skiplist.IteratorI[[]byte, IndexVal], expectedSlice []int) {
	numRead := 0
	for _, e := range expectedSlice {
		actualKey, actualValue, err := it.Next()
		require.Nil(t, err)
		assert.Equal(t, e, int(binary.BigEndian.Uint32(actualKey)))
		require.NotNil(t, actualValue)
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(expectedSlice), numRead)
	// iterator must be in Done state too
	k, v, err := it.Next()
	assert.Equal(t, Done, err)
	require.Nil(t, k)
	require.Equal(t, IndexVal{}, v)
}
