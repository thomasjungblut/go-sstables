package simpledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"path"
	"strconv"
	"testing"
)

func TestRecoveryReconstructSSTables(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryReconstructSSTables")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	fakeTablePath := path.Join(db.basePath, fmt.Sprintf(SSTablePattern, 1337))
	assert.Nil(t, os.MkdirAll(fakeTablePath, 0700))
	mStore := memstore.NewMemStore()
	assert.Nil(t, mStore.Add([]byte("hello"), []byte("world")))
	assert.Nil(t, mStore.Flush(
		sstables.WriteBasePath(fakeTablePath),
		sstables.WithKeyComparator(db.cmp),
	))

	err := db.reconstructSSTables()
	assert.Nil(t, err)
	assert.Equal(t, 1337, int(db.currentGeneration))
	assert.Equal(t, 1, len(db.sstableManager.allSSTableReaders))
}

func TestRecoveryReconstructWithWrongPatternFails(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryReconstructSSTablesWrongPatternFail")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	fakeTablePath := path.Join(db.basePath, fmt.Sprintf(SSTablePattern+"-", 1337))
	assert.Nil(t, os.MkdirAll(fakeTablePath, 0700))
	mStore := memstore.NewMemStore()
	assert.Nil(t, mStore.Add([]byte("hello"), []byte("world")))
	assert.Nil(t, mStore.Flush(
		sstables.WriteBasePath(fakeTablePath),
		sstables.WithKeyComparator(db.cmp),
	))

	err := db.reconstructSSTables()
	assert.Equal(t, &strconv.NumError{
		Func: "ParseInt",
		Num:  "000000000001337-",
		Err:  strconv.ErrSyntax,
	}, err)
}
