package sstables

import (
	"io/ioutil"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"encoding/binary"
	"math/rand"
	"sort"
	"github.com/thomasjungblut/go-sstables/recordio"
)

func TestReadSkipListWriteEndToEnd(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	assert.Nil(t, err)
	defer os.RemoveAll(writer.streamWriter.opts.basePath)

	expectedNumbers := randomIntegerSlice(10000)
	writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements(expectedNumbers))

	assertRandomAndSequentialRead(err, writer.streamWriter.opts.basePath, t, expectedNumbers)
}

func TestReadStreamedWriteEndToEnd(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

// this is implicitly covered by the above tests already since it's a default
func TestReadStreamedWriteEndToEndDataCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeSnappy)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeGZIP)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

func TestReadStreamedWriteEndToEndDataCompressionNone(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithDataCompression(recordio.CompressionTypeNone)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionSnappy(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeSnappy)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

func TestReadStreamedWriteEndToEndIndexCompressionGzip(t *testing.T) {
	writer, err := newTestSSTableStreamWriterWithIndexCompression(recordio.CompressionTypeGZIP)
	assert.Nil(t, err)
	defer os.RemoveAll(writer.opts.basePath)

	expectedNumbers := streamedWrite10kElements(t, writer)
	assertRandomAndSequentialRead(err, writer.opts.basePath, t, expectedNumbers)
}

func streamedWrite10kElements(t *testing.T, writer *SSTableStreamWriter) []int {
	err := writer.Open()
	assert.Nil(t, err)
	expectedNumbers := randomIntegerSliceSorted(10000)
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
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(e))
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, uint32(e+1))
	return key, value
}

func assertContentMatchesSlice(t *testing.T, reader *SSTableReader, expectedSlice []int) {
	numRead := 0
	for _, e := range expectedSlice {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(e))

		assert.True(t, reader.Contains(key))
		actualValue, err := reader.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, e+1, int(binary.BigEndian.Uint32(actualValue)))
		numRead++
	}
	// just to prevent that we've read something empty accidentally
	assert.Equal(t, len(expectedSlice), numRead)
}

func assertContentMatchesSkipList(t *testing.T, reader *SSTableReader, expectedSkipListMap *skiplist.SkipListMap) {
	it := expectedSkipListMap.Iterator()
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

func assertRandomAndSequentialRead(err error, sstablePath string, t *testing.T, expectedNumbers []int) {
	reader, err := NewSSTableReader(
		ReadBasePath(sstablePath),
		ReadWithKeyComparator(skiplist.BytesComparator))
	assert.Nil(t, err)
	defer reader.Close()
	// this tests both the sorted case through a skipList and random read via the shuffled integer slice
	assertContentMatchesSlice(t, reader, expectedNumbers)
	skipList := TEST_ONLY_NewSkipListMapWithElements(expectedNumbers)
	assertContentMatchesSkipList(t, reader, skipList)
}
