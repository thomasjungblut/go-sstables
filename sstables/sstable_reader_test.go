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
	require.ErrorContains(t, err, "offset [41]: Checksum mismatch: expected 688fffff90000000, got 738fffff90000000")
	require.ErrorContains(t, err, "at key [[0 0 0 4]]")
	require.Nil(t, reader)
}

func TestCRCHashMismatchErrorSkipRecord(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesMismatch"),
		ReadWithKeyComparator(skiplist.BytesComparator{}),
		SkipInvalidHashesOnLoad())
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, 1, int(reader.MetaData().SkippedRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)
	// key 4 should be missing, as it has an invalid checksum
	skipListMap := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 5, 6, 7})
	assertContentMatchesSkipList(t, reader, skipListMap)
}

func TestCRCHashMismatchErrorSkipEntirelyReadChecks(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesMismatch"),
		ReadWithKeyComparator(skiplist.BytesComparator{}),
		SkipHashCheckOnLoad(),
		EnableHashCheckOnReads(),
	)
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	// zero, as we're skipping the load-time validation
	assert.Equal(t, 0, int(reader.MetaData().SkippedRecords))
	assert.Equal(t, []byte{0, 0, 0, 1}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 7}, reader.MetaData().MaxKey)

	for _, i := range []int{1, 2, 3, 4, 5, 6, 7} {
		get, err := reader.Get(intToByteSlice(i))
		if i == 4 {
			require.Equal(t, intToByteSlice(0x15), get)
			require.ErrorContains(t, err, "offset [41]: Checksum mismatch: expected 688fffff90000000, got 738fffff90000000")
		} else {
			require.Equal(t, intToByteSlice(i+1), get)
			require.Nil(t, err)
		}
	}

	it, err := reader.ScanStartingAt([]byte{})
	require.Nil(t, err)

	i := 0
	expectedBeforeErr := []int{1, 2, 3, 4}
	for {
		k, v, err := it.Next()
		if err != nil {
			require.ErrorContains(t, err, "offset [41]: Checksum mismatch: expected 688fffff90000000, got 738fffff90000000")
			break
		}

		require.Equal(t, intToByteSlice(expectedBeforeErr[i]), k)
		require.Equal(t, intToByteSlice(expectedBeforeErr[i]+1), v)
		i++
	}
	require.Equal(t, 3, i)

	it, err = reader.Scan()
	require.Nil(t, err)

	i = 0
	for {
		k, v, err := it.Next()
		if err != nil {
			require.Equal(t, ChecksumError{
				checksum:         0x738fffff90000000,
				expectedChecksum: 0x688fffff90000000,
			}, err)
			break
		}

		require.Equal(t, intToByteSlice(expectedBeforeErr[i]), k)
		require.Equal(t, intToByteSlice(expectedBeforeErr[i]+1), v)
		i++
	}
	require.Equal(t, 3, i)

}

func TestCRCHashEmptyValues(t *testing.T) {
	reader, err := NewSSTableReader(
		ReadBasePath("test_files/SimpleWriteHappyPathSSTableWithCRCHashesEmptyValues"),
		ReadWithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	defer closeReader(t, reader)

	assert.Equal(t, 2, int(reader.MetaData().NumRecords))
	assert.Equal(t, []byte{0, 0, 0, 0x2a}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0, 0, 0, 0x2d}, reader.MetaData().MaxKey)

	get, err := reader.Get(intToByteSlice(45))
	require.Nil(t, err)
	require.Equal(t, []byte{}, get)

	get, err = reader.Get(intToByteSlice(42))
	require.Nil(t, err)
	require.Equal(t, []byte{0, 0, 0, 0}, get)
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
