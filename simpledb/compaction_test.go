package simpledb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

func TestExecCompactionLessFilesThanExpected(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_compactionLessFiles")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	db.compactionThreshold = 1
	db.sstableManager.addReader(&MockSSTableReader{
		metadata: &proto.MetaData{NumRecords: 10, TotalBytes: 100},
		path:     "1",
	})

	compaction, err := executeCompaction(db)
	assert.Nil(t, compaction)
	assert.Nil(t, err)
}

func TestExecCompactionSameContent(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_compactionSameContent")
	defer cleanDatabaseFolder(t, db)
	// we'll close the database to mock some internals directly, yes it's very hacky
	closeDatabase(t, db)
	db.closed = false
	db.compactionThreshold = 1

	writeSSTableInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 42))
	writeSSTableInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 43))
	assert.Nil(t, db.reconstructSSTables())

	compactionMeta, err := executeCompaction(db)
	assert.Nil(t, err)
	assert.Equal(t, "sstable_000000000000042", compactionMeta.ReplacementPath)
	assert.Equal(t, []string{"sstable_000000000000042", "sstable_000000000000043"}, compactionMeta.SstablePaths)

	v, err := db.Get("hello")
	assert.Nil(t, err)
	assert.Equal(t, "world", v)

	// for cleanups
	assert.Nil(t, db.sstableManager.currentReader.Close())
}

func writeSSTableWithDataInDatabaseFolder(t *testing.T, db *DB, p string) {
	fakeTablePath := filepath.Join(db.basePath, p)
	assert.Nil(t, os.MkdirAll(fakeTablePath, 0700))
	mStore := memstore.NewMemStore()
	for i := 0; i < 1000; i++ {
		assert.Nil(t, mStore.Add([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i))))
	}
	assert.Nil(t, mStore.Flush(
		sstables.WriteBasePath(fakeTablePath),
		sstables.WithKeyComparator(db.cmp),
	))
}

func writeSSTableWithTombstoneInDatabaseFolder(t *testing.T, db *DB, p string) {
	fakeTablePath := filepath.Join(db.basePath, p)
	assert.Nil(t, os.MkdirAll(fakeTablePath, 0700))
	mStore := memstore.NewMemStore()

	// delete all key between 500 and 800
	for i := 500; i < 800; i++ {
		assert.Nil(t, mStore.Tombstone([]byte(fmt.Sprintf("%d", i))))
	}
	assert.Nil(t, mStore.FlushWithTombstones(
		sstables.WriteBasePath(fakeTablePath),
		sstables.WithKeyComparator(db.cmp),
	))
}

func TestExecCompactionWithTombstone(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_compactionSameContent")
	defer cleanDatabaseFolder(t, db)
	// we'll close the database to mock some internals directly, yes it's very hacky
	closeDatabase(t, db)
	db.closed = false
	db.compactionThreshold = 0

	writeSSTableWithDataInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 42))
	// only one SStable with holes should shrink
	writeSSTableWithTombstoneInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 43))
	assert.Nil(t, db.reconstructSSTables())
	// 1000 initial + 300 Tombstone on second table
	assert.Equal(t, 1300, int(db.sstableManager.currentSSTable().MetaData().GetNumRecords()))

	compactionMeta, err := executeCompaction(db)
	assert.Nil(t, err)
	assert.Equal(t, "sstable_000000000000042", compactionMeta.ReplacementPath)
	assert.Equal(t, []string{"sstable_000000000000042", "sstable_000000000000043"}, compactionMeta.SstablePaths)
	fmt.Print(compactionMeta)
	err = db.sstableManager.reflectCompactionResult(compactionMeta)
	assert.NoError(t, err)
	v, err := db.Get("512")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, "", v)
	// for cleanups
	assert.Nil(t, db.sstableManager.currentReader.Close())

	// check size of compacted sstable
	assert.Equal(t, 700, int(db.sstableManager.currentSSTable().MetaData().NumRecords))
}
func TestExecCompactionWithTombstoneRewriten(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_compactionSameContent")
	defer cleanDatabaseFolder(t, db)
	// we'll close the database to mock some internals directly, yes it's very hacky
	closeDatabase(t, db)
	db.closed = false
	db.compactionThreshold = 0

	writeSSTableWithTombstoneInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 42))
	// the tombstone are overwrite
	writeSSTableWithDataInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern, 43))
	assert.Nil(t, db.reconstructSSTables())
	assert.Equal(t, 1300, int(db.sstableManager.currentSSTable().MetaData().GetNumRecords()))

	compactionMeta, err := executeCompaction(db)
	assert.Nil(t, err)
	assert.Equal(t, "sstable_000000000000042", compactionMeta.ReplacementPath)
	assert.Equal(t, []string{"sstable_000000000000042", "sstable_000000000000043"}, compactionMeta.SstablePaths)
	fmt.Print(compactionMeta)
	err = db.sstableManager.reflectCompactionResult(compactionMeta)
	assert.NoError(t, err)
	v, err := db.Get("512")
	assert.NoError(t, err)
	assert.Equal(t, "512", v)
	// for cleanups
	assert.Nil(t, db.sstableManager.currentReader.Close())

	// check size of compacted sstable
	assert.Equal(t, 1000, int(db.sstableManager.currentSSTable().MetaData().NumRecords))
}
