package sstables

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"testing"
)

func TestSimpleHappyPathReadReadRecordIOV1(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	// 0 because there was no metadata file
	assert.Equal(t, 0, int(reader.MetaData().NumRecords))
	assert.Equal(t, 0, len(reader.MetaData().MinKey))
	assert.Equal(t, 0, len(reader.MetaData().MaxKey))
	assert.Equal(t, 0, int(reader.MetaData().Version))
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSimpleHappyPathReadRecordIOV2(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableRecordIOV2"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSimpleHappyPathBloomRead(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 1, int(reader.MetaData().Version))
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSimpleHappyPathWithMetaData(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestSimpleHappyPathWithCRCHashes(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashes"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestCRCHashMismatchError(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesMismatch"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.ErrorContains(t, err, "for key [[0 0 0 4]]: expected [688fffff90000000], got [738fffff90000000]")
	require.Nil(t, reader)
}

func TestCRCHashMismatchErrorSkipRecord(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesMismatch"),
		ReadWithKeyComparator(skiplist.BytesComparator{}),
		SkipInvalidHashesOnLoad())
	require.Nil(t, err)
	defer closeReader(t, reader)

	// TODO(thomas): the metadata doesn't match when a crc error was detected
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestCRCHashMismatchErrorSkipEntirelyReadChecks(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesMismatch"),
		ReadWithKeyComparator(skiplist.BytesComparator{}),
		SkipHashCheckOnLoad())
	require.Nil(t, err)
	defer closeReader(t, reader)

	// TODO(thomas): the metadata doesn't match when a crc error was detected
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)

	for _, i := range []int{1, 2, 3, 4, 5, 6, 7} {
		get, err := reader.Get(intToByteSlice(i))
		require.Nil(t, err)
		if i == 5 {
			// TODO(thomas): this should also fail on reading
			require.Equal(t, intToByteSlice(15), get)
		} else {
			require.Equal(t, intToByteSlice(i+1), get)
		}
	}
}

func TestNegativeContainsHappyPath(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTable"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assertNegativeContains(t, reader)
}

func TestNegativeContainsHappyPathBloom(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithBloom"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assertNegativeContains(t, reader)
}

func TestFullScan(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	expected := []int{1, 2, 3, 4, 5, 6, 7}
	it, err := reader.Scan()
	require.Nil(t, err)
	assertIteratorMatchesSlice(t, it, expected)
}

func TestScanStartingAt(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

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

func TestScanRange(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithMetaData"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

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

func assertNegativeContains(t *testing.T, reader SSTableReaderI) {
	assert.False(t, reader.Contains([]byte{}))
	assert.False(t, reader.Contains([]byte{1}))
	assert.False(t, reader.Contains([]byte{1, 2, 3}))
	_, err := reader.Get([]byte{})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1})
	assert.Equal(t, errors.New("key was not found"), err)
	_, err = reader.Get([]byte{1, 2, 3})
	assert.Equal(t, errors.New("key was not found"), err)
}
