package simpledb

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/sstables/proto"

	sdbProto "github.com/thomasjungblut/go-sstables/simpledb/proto"
)

func TestSSTableManagerAdditionHappyPath(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	assert.Equal(t, 0, len(manager.allSSTableReaders))
	manager.addReader(sstables.EmptySStableReader{})

	assert.Equal(t, 1, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(&sstables.SuperSSTableReader{}), reflect.TypeOf(manager.currentSSTable()))
}

func TestSSTableInitialStateWillReturnSSTable(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")
	assert.Equal(t, reflect.TypeOf(sstables.EmptySStableReader{}), reflect.TypeOf(manager.currentSSTable()))
	_, err := manager.currentSSTable().Get([]byte{1, 2, 3})
	assert.Equal(t, sstables.NotFound, err)
}

func TestSSTableManagerClearingReaders(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	assert.Equal(t, 0, len(manager.allSSTableReaders))
	manager.addReader(sstables.EmptySStableReader{})

	assert.Equal(t, 1, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(&sstables.SuperSSTableReader{}), reflect.TypeOf(manager.currentSSTable()))

	manager.clearReaders()
	assert.Equal(t, 0, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(sstables.EmptySStableReader{}), reflect.TypeOf(manager.currentSSTable()))
}

func TestSSTableCompactionReflectionHappyPath(t *testing.T) {
	dir, err := os.MkdirTemp("", "simpledb_compactionReflection")
	assert.Nil(t, err)
	// that's our fake compaction path that actually must exist for the logic to work properly
	const compactionOutputPath = "4"
	assert.Nil(t, os.MkdirAll(filepath.Join(dir, compactionOutputPath), 0700))
	writeSSTableInDatabaseFolder(t, &DB{cmp: skiplist.BytesComparator{}, basePath: dir}, compactionOutputPath)

	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, dir)
	manager.addReader(&MockSSTableReader{metadata: &proto.MetaData{}, path: "1"})
	manager.addReader(&MockSSTableReader{metadata: &proto.MetaData{}, path: "2"})
	manager.addReader(&MockSSTableReader{metadata: &proto.MetaData{}, path: "3"})

	meta := &sdbProto.CompactionMetadata{
		WritePath:       compactionOutputPath,
		ReplacementPath: "1",
		SstablePaths:    []string{"1", "2"},
	}
	err = manager.reflectCompactionResult(meta)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(manager.allSSTableReaders))
	assert.Equal(t, "1", filepath.Base(manager.allSSTableReaders[0].BasePath()))
	assert.Equal(t, "3", filepath.Base(manager.allSSTableReaders[1].BasePath()))
}

func TestSSTableManagerSelectCompactionCandidates(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 10, TotalBytes: 100},
		path:     "1",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 5, TotalBytes: 50},
		path:     "2",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 100, TotalBytes: 1200},
		path:     "3",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 0, TotalBytes: 0},
		path:     "4",
	})

	assertCompactionAction(t, 0, []string{"4"}, manager.candidateTablesForCompaction(25, 1))
	assertCompactionAction(t, 105, []string{"2", "3", "4"}, manager.candidateTablesForCompaction(51, 1))
	assertCompactionAction(t, 115, []string{"1", "2", "3", "4"}, manager.candidateTablesForCompaction(101, 1))
	assertCompactionAction(t, 115, []string{"1", "2", "3", "4"}, manager.candidateTablesForCompaction(1500, 1))
}

func TestSSTableManagerSelectCompactionCandidatesTombstoneRatios(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 10, NullValues: 8, TotalBytes: 1000},
		path:     "1",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 5, NullValues: 0, TotalBytes: 1000},
		path:     "2",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 100, NullValues: 10, TotalBytes: 1000},
		path:     "3",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 0, NullValues: 0, TotalBytes: 1000},
		path:     "4",
	})

	assertCompactionAction(t, 10, []string{"1"}, manager.candidateTablesForCompaction(999, 0.2))
	// 1 and 3 should be selected by ratio, 2 is here for the ride because of flood filling
	assertCompactionAction(t, 115, []string{"1", "2", "3"}, manager.candidateTablesForCompaction(999, 0.1))
	assertCompactionAction(t, 115, []string{"1", "2", "3"}, manager.candidateTablesForCompaction(999, 0))
	assertCompactionAction(t, 0, nil, manager.candidateTablesForCompaction(999, 1))
}

func TestSSTableManagerSelectCompactionCandidatesEmptyStart(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 0, TotalBytes: 0},
		path:     "1",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 5, TotalBytes: 50},
		path:     "2",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 0, TotalBytes: 0},
		path:     "3",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 25, TotalBytes: 175},
		path:     "4",
	})

	assertCompactionAction(t, 5, []string{"1", "2", "3"}, manager.candidateTablesForCompaction(100, 1))
	assertCompactionAction(t, 30, []string{"1", "2", "3", "4"}, manager.candidateTablesForCompaction(200, 1))
}

func TestSSTableManagerSelectCompactionCandidatesTombstonedHoles(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, "")

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 1000, TotalBytes: 2000},
		path:     "1",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 5, TotalBytes: 1000},
		path:     "2",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 3000, TotalBytes: 3000},
		path:     "3",
	})

	manager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 5, TotalBytes: 1000},
		path:     "4",
	})

	assertCompactionAction(t, 3010, []string{"2", "3", "4"}, manager.candidateTablesForCompaction(2000, 1))
}

func assertCompactionAction(t *testing.T, numRecords int, paths []string, actualAction compactionAction) {
	assert.Equal(t, numRecords, int(actualAction.totalRecords))
	assert.Equal(t, paths, actualAction.pathsToCompact)
}

func TestFloodFill(t *testing.T) {
	cases := []struct {
		input    []bool
		expected []bool
	}{
		{input: []bool{true}, expected: []bool{true}},
		{input: []bool{false}, expected: []bool{false}},
		{input: []bool{true, false}, expected: []bool{true, false}},
		{input: []bool{false, true}, expected: []bool{false, true}},
		{input: []bool{true, false, true, false, false, false}, expected: []bool{true, true, true, false, false, false}},
		{input: []bool{true, false, false, true, false, true}, expected: []bool{true, true, true, true, true, true}},
		{input: []bool{false, false, true, false, false, true}, expected: []bool{false, false, true, true, true, true}},
		{input: []bool{true, false, false, false, false}, expected: []bool{true, false, false, false, false}},
		{input: []bool{false, false, false, false, true}, expected: []bool{false, false, false, false, true}},
		{input: []bool{false, false, false, true, false}, expected: []bool{false, false, false, true, false}},
		{input: []bool{false, false, false, true, true}, expected: []bool{false, false, false, true, true}},
		{input: []bool{true, false, false, true, true}, expected: []bool{true, true, true, true, true}},
	}
	for n, c := range cases {
		t.Run("testcase "+strconv.Itoa(n), func(t *testing.T) {
			assert.Equal(t, c.expected, floodFill(c.input))
		})
	}
}

type MockSSTableReader struct {
	metadata *proto.MetaData
	path     string
}

func (m *MockSSTableReader) MetaData() *proto.MetaData {
	return m.metadata
}

func (m *MockSSTableReader) Contains(key []byte) (bool, error) {
	return false, nil
}

func (m *MockSSTableReader) Get(key []byte) ([]byte, error) {
	return nil, sstables.NotFound
}

func (m *MockSSTableReader) Scan() (sstables.SSTableIteratorI, error) {
	return sstables.EmptySSTableIterator{}, nil
}

func (m *MockSSTableReader) ScanStartingAt(key []byte) (sstables.SSTableIteratorI, error) {
	return sstables.EmptySSTableIterator{}, nil
}

func (m *MockSSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (sstables.SSTableIteratorI, error) {
	return sstables.EmptySSTableIterator{}, nil
}

func (m *MockSSTableReader) Close() error {
	return nil
}

func (m *MockSSTableReader) BasePath() string {
	return m.path
}
