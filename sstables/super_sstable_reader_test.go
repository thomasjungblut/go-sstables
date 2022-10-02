package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"testing"
)

func TestSuperSimpleHappyPathReadReadRecordIOV1(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	// 0 because there was no metadata file
	assert.Equal(t, 0, int(reader.MetaData().NumRecords))
	assert.Equal(t, 0, len(reader.MetaData().MinKey))
	assert.Equal(t, 0, len(reader.MetaData().MaxKey))
	assert.Equal(t, 0, int(reader.MetaData().Version))
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSuperSimpleHappyPathReadRecordIOV2(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableRecordIOV2"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSuperSimpleHappyPathReadRecordIOV2Overlapping(t *testing.T) {
	reader1, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableRecordIOV2"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader1)

	reader2, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableRecordIOV2"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader2)

	reader := SuperSSTableReader{
		readers: []SSTableReaderI{reader1, reader2},
		comp:    skiplist.BytesComparator{},
	}

	assert.Equal(t, 14, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSuperSimpleHappyPathBloomRead(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	assert.Equal(t, 1, int(reader.MetaData().Version))
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSuperSimpleHappyPathWithMetaData(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSuperNegativeContainsHappyPath(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	assertNegativeContains(t, reader)
}

func TestSuperNegativeContainsHappyPathBloom(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	assertNegativeContains(t, reader)
}

func TestSuperFullScan(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7}
	it, err := reader.Scan()
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)
}

func TestSuperFullScanOverlapping(t *testing.T) {
	reader1, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader1)

	reader2, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader2)

	reader := SuperSSTableReader{
		readers: []SSTableReaderI{reader1, reader2},
		comp:    skiplist.BytesComparator{},
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7}
	it, err := reader.Scan()
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)
}

func TestSuperScanStartingAt(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7}
	// whole sequence when out of bounds to the left
	it, err := reader.ScanStartingAt(intToByteSlice(0))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)

	// staggered test
	for i, start := range expected {
		sliced := expected[i:]
		it, err := reader.ScanStartingAt(intToByteSlice(start))
		require.Nil(t, err)
		assertIteratorMatchesSlice(t, it, sliced)
	}

	// test out of range iteration, which should yield an empty iterator
	it, err = reader.ScanStartingAt(intToByteSlice(10))
	require.Nil(t, err)
	k, v, err := it.Next()
	require.Nil(t, k)
	require.Nil(t, v)
	assert.Equal(t, Done, err)
}

func TestSuperScanRange(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	reader = SuperSSTableReader{
		readers: []SSTableReaderI{reader},
		comp:    skiplist.BytesComparator{},
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7}
	// whole sequence when out of bounds to the left and right
	it, err := reader.ScanRange(intToByteSlice(0), intToByteSlice(10))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)

	// whole sequence when in bounds for inclusiveness
	it, err = reader.ScanRange(intToByteSlice(1), intToByteSlice(7))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)

	// only 4 when requesting between 4 and 4
	it, err = reader.ScanRange(intToByteSlice(4), intToByteSlice(4))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, []int{4})

	// error when higher key and lower key are inconsistent
	_, err = reader.ScanRange(intToByteSlice(1), intToByteSlice(0))
	assert.NotNil(t, err)

	// staggered test with end outside of range
	for i, start := range expected {
		sliced := expected[i:]
		it, err := reader.ScanRange(intToByteSlice(start), intToByteSlice(10))
		require.Nil(t, err)
		assertIteratorMatchesSlice(t, it, sliced)
	}

	// staggered test with end crossing to the left
	for i, start := range expected {
		it, err := reader.ScanRange(intToByteSlice(start), intToByteSlice(expected[len(expected)-i-1]))
		if i <= (len(expected) / 2) {
			require.Nil(t, err)
			sliced := expected[i : len(expected)-i]
			assertIteratorMatchesSlice(t, it, sliced)
		} else {
			assert.NotNil(t, err)
		}
	}

	// test out of range iteration, which should yield an empty iterator
	it, err = reader.ScanRange(intToByteSlice(10), intToByteSlice(100))
	require.Nil(t, err)
	k, v, err := it.Next()
	require.Nil(t, k)
	require.Nil(t, v)
	assert.Equal(t, Done, err)
}

func TestScanReduceFunc(t *testing.T) {
	expectedKey := []byte{0}
	values := [][]byte{{0}, {1}, {2}, {3}}
	k, v := ScanReduceLatestWins(expectedKey, values, []interface{}{3, 2, 1, 0})
	assert.Equal(t, expectedKey, k)
	assert.Equal(t, v, values[0])

	k, v = ScanReduceLatestWins(expectedKey, values, []interface{}{0, 2, 1, 0})
	assert.Equal(t, expectedKey, k)
	assert.Equal(t, v, values[1])

	k, v = ScanReduceLatestWins(expectedKey, values, []interface{}{0, 0, 0, 0})
	assert.Equal(t, expectedKey, k)
	assert.Equal(t, v, values[0])
}

func TestSuperStaggeredAndOverlappingFull(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer.streamWriter)

	err = writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements([]int{0, 1, 2, 4, 8, 9, 10}))
	require.Nil(t, err)

	reader1, err := NewSSTableReader(
		ReadBasePath(writer.streamWriter.opts.basePath),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader1)

	reader2, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableRecordIOV2"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader2)

	reader := SuperSSTableReader{
		readers: []SSTableReaderI{reader1, reader2},
		comp:    skiplist.BytesComparator{},
	}

	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	assert.Equal(t, 14, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 10}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements(expected)
	assertContentMatchesSkipList(t, reader, skipListMap)

	// whole sequence when out of bounds to the left and right
	it, err := reader.ScanRange(intToByteSlice(0), intToByteSlice(25))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)

	// whole sequence when in bounds for inclusiveness
	it, err = reader.ScanRange(intToByteSlice(0), intToByteSlice(10))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)

	// only 4 when requesting between 4 and 4
	it, err = reader.ScanRange(intToByteSlice(4), intToByteSlice(4))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, []int{4})

	// only 3-7
	it, err = reader.ScanRange(intToByteSlice(3), intToByteSlice(7))
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, []int{3, 4, 5, 6, 7})

	// error when higher key and lower key are inconsistent
	_, err = reader.ScanRange(intToByteSlice(1), intToByteSlice(0))
	assert.NotNil(t, err)

	// staggered test with end outside of range
	for i, start := range expected {
		sliced := expected[i:]
		it, err := reader.ScanRange(intToByteSlice(start), intToByteSlice(25))
		require.Nil(t, err)
		assertIteratorMatchesSlice(t, it, sliced)
	}

	// staggered test with end crossing to the left
	for i, start := range expected {
		it, err := reader.ScanRange(intToByteSlice(start), intToByteSlice(expected[len(expected)-i-1]))
		if i <= (len(expected) / 2) {
			require.Nil(t, err)
			sliced := expected[i : len(expected)-i]
			assertIteratorMatchesSlice(t, it, sliced)
		} else {
			assert.NotNil(t, err)
		}
	}

	// test out of range iteration, which should yield an empty iterator
	it, err = reader.ScanRange(intToByteSlice(11), intToByteSlice(100))
	require.Nil(t, err)
	k, v, err := it.Next()
	require.Nil(t, k)
	require.Nil(t, v)
	assert.Equal(t, Done, err)
}
