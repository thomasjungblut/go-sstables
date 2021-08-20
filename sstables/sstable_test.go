package sstables

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func TestReadSkipListWriteEndToEnd(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer.streamWriter)

	expectedNumbers := randomIntegerSlice(1000)
	err = writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements(expectedNumbers))
	require.Nil(t, err)

	assertRandomAndSequentialRead(t, writer.streamWriter.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEnd(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndCheckMetadata(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	reader, err := NewSSTableReader(
		ReadBasePath(writer.opts.basePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	require.Nil(t, err)
	defer closeReader(t, reader)

	// check the metadata is accurate
	assert.Equal(t, 1, int(reader.MetaData().Version))
	assert.Equal(t, len(expectedNumbers), int(reader.MetaData().NumRecords))
	assert.Equal(t, 11008, int(reader.MetaData().DataBytes))
	assert.Equal(t, 13997, int(reader.MetaData().IndexBytes))
	assert.Equal(t, 25005, int(reader.MetaData().TotalBytes))
	assert.Equal(t, []byte{0x0, 0xa, 0x5c, 0x94}, reader.MetaData().MinKey)
	assert.Equal(t, []byte{0x7f, 0xe0, 0x33, 0x1}, reader.MetaData().MaxKey)
}

// this is implicitly covered by the above tests already since it's a default
func TestReadStreamedWriteEndToEndDataCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeSnappy)
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeGZIP)
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionNone(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeNone)
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeSnappy)
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeGZIP)
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndForRangeTesting(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer)

	expectedNumbers := streamedWriteElements(t, writer, 100)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
	assertExhaustiveRangeReads(t, writer.opts.basePath, expectedNumbers)
}

func streamedWrite1kElements(t *testing.T, writer *SSTableStreamWriter) []int {
	return streamedWriteElements(t, writer, 1000)
}

func streamedWriteElements(t *testing.T, writer *SSTableStreamWriter, n int) []int {
	err := writer.Open()
	require.Nil(t, err)
	expectedNumbers := randomIntegerSliceSorted(n)
	for _, e := range expectedNumbers {
		key, value := getKeyValueAsBytes(e)
		err = writer.WriteNext(key, value)
		require.Nil(t, err)
	}
	err = writer.Close()
	require.Nil(t, err)
	return expectedNumbers
}

func streamedWriteAscendingIntegersWithStart(t *testing.T, writer *SSTableStreamWriter, start int, n int) []int {
	err := writer.Open()
	require.Nil(t, err)
	var expectedNumbers []int
	for i := start; i < n; i++ {
		key, value := getKeyValueAsBytes(i)
		err = writer.WriteNext(key, value)
		require.Nil(t, err)
		expectedNumbers = append(expectedNumbers, i)
	}
	err = writer.Close()
	require.Nil(t, err)
	return expectedNumbers
}

func streamedWriteAscendingIntegers(t *testing.T, writer *SSTableStreamWriter, n int) []int {
	return streamedWriteAscendingIntegersWithStart(t, writer, 0, n)
}

func newTestSSTableSimpleWriter() (*SSTableSimpleWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableSimpleWriter(WriteBasePath(tmpDir), WithKeyComparator(skiplist.BytesComparator))
}

func newTestSSTableStreamWriter() (*SSTableStreamWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableStreamWriter(WriteBasePath(tmpDir), WithKeyComparator(skiplist.BytesComparator))
}

func newTestSSTableStreamWriterWithDataCompression(compressionType int) (*SSTableStreamWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_WriterDataCompressed")
	if err != nil {
		return nil, err
	}

	return NewSSTableStreamWriter(
		WriteBasePath(tmpDir),
		WithKeyComparator(skiplist.BytesComparator),
		DataCompressionType(compressionType))
}

func newTestSSTableStreamWriterWithIndexCompression(compressionType int) (*SSTableStreamWriter, error) {
	tmpDir, err := ioutil.TempDir("", "sstables_WriterIndexCompressed")
	if err != nil {
		return nil, err
	}

	return NewSSTableStreamWriter(
		WriteBasePath(tmpDir),
		WithKeyComparator(skiplist.BytesComparator),
		IndexCompressionType(compressionType))
}

func randomIntegerSliceSorted(len int) []int {
	slice := randomIntegerSlice(len)
	sort.Ints(slice)
	return slice
}

func randomIntegerSlice(len int) []int {
	var slice []int

	for i := 0; i < len; i++ {
		// this will create non-negative randoms, we can treat them as uints later when serializing to bytes
		slice = append(slice, int(rand.Int31()))
	}

	return slice
}

//noinspection GoSnakeCaseUsage
func TEST_ONLY_NewSkipListMapWithElements(toInsert []int) skiplist.SkipListMapI {
	list := skiplist.NewSkipListMap(skiplist.BytesComparator)
	for _, e := range toInsert {
		key, value := getKeyValueAsBytes(e)
		list.Insert(key, value)
	}
	return list
}

func getKeyValueAsBytes(e int) ([]byte, []byte) {
	key := intToByteSlice(e)
	value := intToByteSlice(e + 1)
	return key, value
}

func assertContentMatchesSlice(t *testing.T, reader SSTableReaderI, expectedSlice []int) {
	numRead := 0
	for _, e := range expectedSlice {
		key := intToByteSlice(e)

		assert.True(t, reader.Contains(key))
		actualValue, err := reader.Get(key)
		require.Nil(t, err)
		assert.Equal(t, e+1, int(binary.BigEndian.Uint32(actualValue)))
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(expectedSlice), numRead)
}

func assertIteratorMatchesSlice(t *testing.T, it SSTableIteratorI, expectedSlice []int) {
	numRead := 0
	for _, e := range expectedSlice {
		actualKey, actualValue, err := it.Next()
		require.Nil(t, err)
		assert.Equal(t, e, int(binary.BigEndian.Uint32(actualKey)))
		assert.Equal(t, e+1, int(binary.BigEndian.Uint32(actualValue)))
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(expectedSlice), numRead)
	// iterator must be in Done state too
	k, v, err := it.Next()
	assert.Equal(t, Done, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

func assertContentMatchesSkipList(t *testing.T, reader SSTableReaderI, expectedSkipListMap skiplist.SkipListMapI) {
	it, _ := expectedSkipListMap.Iterator()
	numRead := 0
	for {
		expectedKey, expectedValue, err := it.Next()
		if err == skiplist.Done {
			break
		}
		require.Nil(t, err)

		assert.True(t, reader.Contains(expectedKey.([]byte)))
		actualValue, err := reader.Get(expectedKey.([]byte))
		require.Nil(t, err)
		assert.Equal(t, expectedValue, actualValue)
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, expectedSkipListMap.Size(), numRead)
}

func getFullScanIterator(t *testing.T, sstablePath string) (SSTableReaderI, SSTableIteratorI) {
	reader, err := NewSSTableReader(
		ReadBasePath(sstablePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	require.Nil(t, err)

	it, err := reader.Scan()
	require.Nil(t, err)
	return reader, it
}

func assertRandomAndSequentialRead(t *testing.T, sstablePath string, expectedNumbers []int) {
	reader, err := NewSSTableReader(
		ReadBasePath(sstablePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	require.Nil(t, err)
	defer closeReader(t, reader)

	// check the metadata is accurate
	assert.Equal(t, len(expectedNumbers), int(reader.MetaData().NumRecords))

	// test random reads
	rand.Shuffle(len(expectedNumbers), func(i, j int) {
		expectedNumbers[i], expectedNumbers[j] = expectedNumbers[j], expectedNumbers[i]
	})
	assertContentMatchesSlice(t, reader, expectedNumbers)

	// test ordered reads
	sort.Ints(expectedNumbers)
	assertContentMatchesSlice(t, reader, expectedNumbers)
}

func assertExhaustiveRangeReads(t *testing.T, sstablePath string, expectedNumbers []int) {
	reader, err := NewSSTableReader(
		ReadBasePath(sstablePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	require.Nil(t, err)
	defer closeReader(t, reader)

	// check the metadata is accurate
	assert.Equal(t, len(expectedNumbers), int(reader.MetaData().NumRecords))
	sort.Ints(expectedNumbers)

	// this is a bit exhaustive at O(n!) but the runtime is fine for up to 100 elements
	for i := 0; i < len(expectedNumbers); i++ {
		lowKey := intToByteSlice(expectedNumbers[i])
		for j := i; j < len(expectedNumbers); j++ {
			highKey := intToByteSlice(expectedNumbers[j])
			it, err := reader.ScanRange(lowKey, highKey)
			require.Nil(t, err)
			assertIteratorMatchesSlice(t, it, expectedNumbers[i:j+1])
		}
	}

}

func closeWriter(t *testing.T, writer *SSTableStreamWriter) {
	func() { require.Nil(t, writer.Close()) }()
}

func closeReader(t *testing.T, reader SSTableReaderI) {
	func() { require.Nil(t, reader.Close()) }()
}

func cleanWriterDir(t *testing.T, writer *SSTableStreamWriter) {
	func() { require.Nil(t, os.RemoveAll(writer.opts.basePath)) }()
}

func cleanWriterDirs(t *testing.T, writers *[]*SSTableStreamWriter) {
	for _, w := range *writers {
		cleanWriterDir(t, w)
	}
}

func intToByteSlice(e int) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(e))
	return key
}
