package simpledb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

func TestFlushHappyPath(t *testing.T) {
	tmpDummyWalFile, err := os.CreateTemp("", "simpledb_flushHappyPath")
	assert.Nil(t, err)
	assert.Nil(t, tmpDummyWalFile.Close())

	defer os.Remove(tmpDummyWalFile.Name())
	tmpDir, err := os.MkdirTemp("", "simpledb_flushHappyPath")
	assert.Nil(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		assert.Nil(t, err)
	}(tmpDir)

	action := memStoreFlushAction{
		memStore: &setupPrefilledRWMemstore(t).writeStore,
		walPath:  tmpDummyWalFile.Name(),
	}

	db := &DB{
		cmp:                  skiplist.BytesComparator{},
		basePath:             tmpDir,
		currentGeneration:    42,
		rwLock:               &sync.RWMutex{},
		sstableManager:       NewSSTableManager(skiplist.BytesComparator{}, &sync.RWMutex{}, tmpDir),
		readBufferSizeBytes:  DefaultReadBufferSizeBytes,
		writeBufferSizeBytes: DefaultWriteBufferSizeBytes,
	}
	err = executeFlush(db, action)
	assert.Nil(t, err)

	// ensure that the wal file was cleaned and a sstable with the right name was created
	_, err = os.Stat(tmpDummyWalFile.Name())
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tmpDir, "sstable_000000000000043"))
	assert.Nil(t, err)

	assert.Nil(t, db.sstableManager.currentReader.Close())
}

func TestFlushEmptyMemstore(t *testing.T) {
	m := memstore.NewMemStore()
	action := memStoreFlushAction{
		memStore: &m,
		walPath:  "some",
	}

	db := &DB{currentGeneration: 0}
	err := executeFlush(db, action)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), db.currentGeneration)
}

func TestFlushPathsSortCorrectly(t *testing.T) {
	var tables []string
	for i := 0; i < 10001; i++ {
		tables = append(tables, fmt.Sprintf(SSTablePattern, i))
	}

	sort.Strings(tables)
	for i := 0; i < 10001; i++ {
		is := strings.Split(tables[i], "_")
		atoi, err := strconv.Atoi(is[1])
		assert.Nil(t, err)
		assert.Equal(t, i, atoi)
	}
}
