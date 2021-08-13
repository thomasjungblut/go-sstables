package simpledb

import (
	"github.com/stretchr/testify/assert"
	sdbProto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestSSTableManagerAdditionHappyPath(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator, &sync.RWMutex{}, "")

	assert.Equal(t, 0, len(manager.allSSTableReaders))
	manager.addReader(sstables.EmptySStableReader{})

	assert.Equal(t, 1, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(&sstables.SuperSSTableReader{}), reflect.TypeOf(manager.currentSSTable()))
}

func TestSSTableInitialStateWillReturnSSTable(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator, &sync.RWMutex{}, "")
	assert.Equal(t, reflect.TypeOf(sstables.EmptySStableReader{}), reflect.TypeOf(manager.currentSSTable()))
	_, err := manager.currentSSTable().Get([]byte{1, 2, 3})
	assert.Equal(t, sstables.NotFound, err)
}

func TestSSTableManagerClearingReaders(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator, &sync.RWMutex{}, "")

	assert.Equal(t, 0, len(manager.allSSTableReaders))
	manager.addReader(sstables.EmptySStableReader{})

	assert.Equal(t, 1, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(&sstables.SuperSSTableReader{}), reflect.TypeOf(manager.currentSSTable()))

	manager.clearReaders()
	assert.Equal(t, 0, len(manager.allSSTableReaders))
	assert.Equal(t, reflect.TypeOf(sstables.EmptySStableReader{}), reflect.TypeOf(manager.currentSSTable()))
}

func TestSSTableManagerSelectCompactionCandidates(t *testing.T) {
	manager := NewSSTableManager(skiplist.BytesComparator, &sync.RWMutex{}, "")

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

	assertCompactionAction(t, 0, []string(nil), manager.candidateTablesForCompaction(25))
	assertCompactionAction(t, 5, []string{"2"}, manager.candidateTablesForCompaction(51))
	assertCompactionAction(t, 15, []string{"1", "2"}, manager.candidateTablesForCompaction(101))
	assertCompactionAction(t, 115, []string{"1", "2", "3"}, manager.candidateTablesForCompaction(1500))
}

func TestSSTableCompactionReflectionHappyPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "simpledb_compactionReflection")
	assert.Nil(t, err)
	// that's our fake compaction path that actually must exist for the logic to work properly
	const compactionOutputPath = "4"
	assert.Nil(t, os.MkdirAll(filepath.Join(dir, compactionOutputPath), 0700))
	writeSSTableInDatabaseFolder(t, &DB{cmp: skiplist.BytesComparator, basePath: dir}, compactionOutputPath)

	manager := NewSSTableManager(skiplist.BytesComparator, &sync.RWMutex{}, dir)
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

func assertCompactionAction(t *testing.T, numRecords int, paths []string, actualAction compactionAction) {
	assert.Equal(t, numRecords, int(actualAction.totalRecords))
	assert.Equal(t, len(paths), len(actualAction.pathsToCompact))
	assert.Equal(t, paths, actualAction.pathsToCompact)
}

type MockSSTableReader struct {
	metadata *proto.MetaData
	path     string
}

func (m *MockSSTableReader) MetaData() *proto.MetaData {
	return m.metadata
}

func (m *MockSSTableReader) Contains(key []byte) bool {
	return false
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
