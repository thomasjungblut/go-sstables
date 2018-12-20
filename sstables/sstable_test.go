package sstables

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
	defer os.RemoveAll(writer.streamWriter.opts.basePath)

	expectedNumbers := randomIntegerSlice(1000)
	err = writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements(expectedNumbers))
	assert.Nil(t, err)

	assertRandomAndSequentialRead(t, writer.streamWriter.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEnd(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

// this is implicitly covered by the above tests already since it's a default
func TestReadStreamedWriteEndToEndDataCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeSnappy)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeGZIP)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionNone(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeNone)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeSnappy)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeGZIP)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite1kElements(t, writer)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
}

func TestReadStreamedWriteEndToEndForRangeTesting(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWriteElements(t, writer, 100)
	assertRandomAndSequentialRead(t, writer.opts.basePath, expectedNumbers)
	assertExhaustiveRangeReads(t, writer.opts.basePath, expectedNumbers)
}

func streamedWrite1kElements(t *testing.T, writer *SSTableStreamWriter) []int {
	return streamedWriteElements(t, writer, 1000)
}

func streamedWriteElements(t *testing.T, writer *SSTableStreamWriter, n int) []int {
	err := writer.Open()
	assert.Nil(t, err)
	expectedNumbers := randomIntegerSliceSorted(n)
	for _, e := range expectedNumbers {
		key, value := getKeyValueAsBytes(e)
		err = writer.WriteNext(key, value)
		assert.Nil(t, err)
	}
	err = writer.Close()
	assert.Nil(t, err)
	return expectedNumbers
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
func TEST_ONLY_NewSkipListMapWithElements(toInsert []int) *skiplist.SkipListMap {
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

func assertContentMatchesSlice(t *testing.T, reader *SSTableReader, expectedSlice []int) {
	numRead := 0
	for _, e := range expectedSlice {
		key := intToByteSlice(e)

		assert.True(t, reader.Contains(key))
		actualValue, err := reader.Get(key)
		assert.Nil(t, err)
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
		assert.Nil(t, err)
		assert.Equal(t, e, int(binary.BigEndian.Uint32(actualKey)))
		assert.Equal(t, e+1, int(binary.BigEndian.Uint32(actualValue)))
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(expectedSlice), numRead)
	// iterator must be in Done state too
	_, _, err := it.Next()
	assert.Equal(t, Done, err)
}

func assertContentMatchesSkipList(t *testing.T, reader *SSTableReader, expectedSkipListMap *skiplist.SkipListMap) {
	it, _ := expectedSkipListMap.Iterator()
	numRead := 0
	for {
		expectedKey, expectedValue, err := it.Next()
		if err == skiplist.Done {
			break
		}
		assert.Nil(t, err)

		assert.True(t, reader.Contains(expectedKey.([]byte)))
		actualValue, err := reader.Get(expectedKey.([]byte))
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, actualValue)
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, expectedSkipListMap.Size(), numRead)
}

func assertRandomAndSequentialRead(t *testing.T, sstablePath string, expectedNumbers []int) {
	reader, err := NewSSTableReader(
		ReadBasePath(sstablePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()

	// check the metadata is accurate
	assert.Equal(t, len(expectedNumbers), int(reader.metaData.NumRecords))

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
	assert.Nil(t, err)
	defer reader.Close()

	// check the metadata is accurate
	assert.Equal(t, len(expectedNumbers), int(reader.metaData.NumRecords))
	sort.Ints(expectedNumbers)

	// this is a bit exhaustive at O(n!) but the runtime is fine for up to 100 elements
	for i := 0; i < len(expectedNumbers); i++ {
		lowKey := intToByteSlice(expectedNumbers[i])
		for j := i; j < len(expectedNumbers); j++ {
			highKey := intToByteSlice(expectedNumbers[j])
			it, err := reader.ScanRange(lowKey, highKey)
			assert.Nil(t, err)
			assertIteratorMatchesSlice(t, it, expectedNumbers[i:j+1])
		}
	}

}

func intToByteSlice(e int) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(e))
	return key
}
