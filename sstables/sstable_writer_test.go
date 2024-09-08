package sstables

import (
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

func TestSkipListSimpleWriteHappyPath(t *testing.T) {
	writer, err := newTestSSTableSimpleWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, writer.streamWriter)

	list := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	err = writer.WriteSkipListMap(list)
	require.Nil(t, err)

	reader, err := NewSSTableReader(
		ReadBasePath(writer.streamWriter.opts.basePath),
		ReadWithKeyComparator(writer.streamWriter.opts.keyComparator))
	require.Nil(t, err)
	defer closeReader(t, reader)
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assertContentMatchesSkipList(t, reader, list)
}

func TestSkipListStreamedWriteFailsOnKeyComparison(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	defer cleanWriterDir(t, writer)
	defer closeWriter(t, writer)
	require.Nil(t, err)

	err = writer.Open()
	require.Nil(t, err)

	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	require.Nil(t, err)

	binary.BigEndian.PutUint32(key, uint32(6))
	err = writer.WriteNext(key, key)
	assert.Contains(t, err.Error(), "non-ascending key cannot be written")

	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	assert.Contains(t, err.Error(), "the same key cannot be written more than once")
}

func TestSkipListStreamedWriteKeyComparisonAdjustsBufferSize(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	defer cleanWriterDir(t, writer)
	defer closeWriter(t, writer)
	require.Nil(t, err)

	err = writer.Open()
	require.Nil(t, err)

	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(13))
	err = writer.WriteNext(key, key)
	require.Nil(t, err)

	key = make([]byte, 5)
	binary.BigEndian.PutUint32(key, uint32(14))
	err = writer.WriteNext(key, key)
	require.Nil(t, err)

	key = make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(12))
	err = writer.WriteNext(key, key)
	assert.Contains(t, err.Error(), "non-ascending key cannot be written")
}

func TestComparatorNotSupplied(t *testing.T) {
	_, err := NewSSTableSimpleWriter(WriteBasePath("abc"))
	assert.Equal(t, errors.New("no key comparator supplied"), err)
}

func TestDirectoryDoesNotExist(t *testing.T) {
	_, err := NewSSTableSimpleWriter(WithKeyComparator(skiplist.BytesComparator{}))
	assert.Equal(t, errors.New("basePath was not supplied"), err)
}

func TestUnopenedWrites(t *testing.T) {
	w, err := NewSSTableSimpleWriter(WriteBasePath("abc"), WithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	err = w.streamWriter.WriteNext([]byte{1}, []byte{1})
	assert.Contains(t, err.Error(), "no metadata available to write into, table might not be opened yet")
}

func TestCompressionTypeDoesNotExist(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstables_Writer")
	defer func() { require.Nil(t, os.RemoveAll(tmpDir)) }()
	require.Nil(t, err)
	writer, err := NewSSTableSimpleWriter(WriteBasePath(tmpDir), DataCompressionType(25), WithKeyComparator(skiplist.BytesComparator{}))
	require.Nil(t, err)
	err = writer.WriteSkipListMap(TEST_ONLY_NewSkipListMapWithElements([]int{}))
	assert.Equal(t, errors.New("unsupported compression type 25"), errors.Unwrap(errors.Unwrap(err)))
	require.Nil(t, writer.streamWriter.Close())
}

func TestEmptyBloomFilter(t *testing.T) {
	_, err := NewSSTableSimpleWriter(
		WriteBasePath("abc"),
		WithKeyComparator(skiplist.BytesComparator{}),
		BloomExpectedNumberOfElements(0))
	assert.Equal(t, errors.New("unexpected number of bloom filter elements, was: 0"), err)
}

func TestFailedDataAppend(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	dw := &failingRecordIoWriter{writer.dataWriter, false}
	writer.dataWriter = dw
	require.NoError(t, writer.WriteNext(intToByteSlice(42), intToByteSlice(43)))
	dw.failNext = true
	require.Error(t, writer.WriteNext(intToByteSlice(43), intToByteSlice(44)))
	dw.failNext = false
	require.NoError(t, writer.WriteNext(intToByteSlice(44), intToByteSlice(45)))
	require.NoError(t, writer.Close())

	reader, it := getFullScanIterator(t, writer.opts.basePath)
	defer closeReader(t, reader)

	assertIteratorMatchesSlice(t, it, []int{42, 44})
	assertContentMatchesSlice(t, reader, []int{42, 44})
	_, err = reader.Get(intToByteSlice(43))
	require.Equal(t, NotFound, err)
}

func TestFailedIndexAppend(t *testing.T) {
	writer, err := newTestSSTableStreamWriter()
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	iw := &failingProtoRecordIoWriter{writer.indexWriter, false}
	writer.indexWriter = iw
	require.NoError(t, writer.WriteNext(intToByteSlice(42), intToByteSlice(43)))
	iw.failNext = true
	require.Error(t, writer.WriteNext(intToByteSlice(43), intToByteSlice(44)))
	iw.failNext = false
	require.NoError(t, writer.WriteNext(intToByteSlice(44), intToByteSlice(45)))
	require.NoError(t, writer.Close())

	reader, it := getFullScanIterator(t, writer.opts.basePath)
	defer closeReader(t, reader)

	assertIteratorMatchesSlice(t, it, []int{42, 44})
	assertContentMatchesSlice(t, reader, []int{42, 44})
	_, err = reader.Get(intToByteSlice(43))
	require.Equal(t, NotFound, err)
}

type failingRecordIoWriter struct {
	w        recordio.WriterI
	failNext bool
}

func (f *failingRecordIoWriter) Close() error {
	return f.w.Close()
}

func (f *failingRecordIoWriter) Open() error {
	return f.w.Open()
}

func (f *failingRecordIoWriter) Size() uint64 {
	return f.w.Size()
}

func (f *failingRecordIoWriter) Seek(o uint64) error {
	return f.w.Seek(o)
}

func (f *failingRecordIoWriter) WriteSync(record []byte) (uint64, error) {
	if f.failNext {
		return 0, errors.New("failing record")
	}

	return f.w.WriteSync(record)
}

func (f *failingRecordIoWriter) Write(record []byte) (uint64, error) {
	if f.failNext {
		return 0, errors.New("failing record")
	}

	return f.w.Write(record)
}

type failingProtoRecordIoWriter struct {
	w        rProto.WriterI
	failNext bool
}

func (f *failingProtoRecordIoWriter) Close() error {
	return f.w.Close()
}

func (f *failingProtoRecordIoWriter) Open() error {
	return f.w.Open()
}

func (f *failingProtoRecordIoWriter) Size() uint64 {
	return f.w.Size()
}

func (f *failingProtoRecordIoWriter) WriteSync(record proto.Message) (uint64, error) {
	if f.failNext {
		return 0, errors.New("failing record")
	}

	return f.w.WriteSync(record)
}

func (f *failingProtoRecordIoWriter) Write(record proto.Message) (uint64, error) {
	if f.failNext {
		return 0, errors.New("failing record")
	}

	return f.w.Write(record)
}
