package simpledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"testing"
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
