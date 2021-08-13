package simpledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestRecoveryReconstructSSTables(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryReconstructSSTables")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	fakeTablePath := filepath.Join(db.basePath, fmt.Sprintf(SSTablePattern, 1337))
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

func TestRecoveryReconstructSSTablesWithExistingReaders(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryReconstructSSTablesWithExistingReaders")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	db.sstableManager.addReader(sstables.EmptySStableReader{})
	err := db.reconstructSSTables()
	assert.NotNil(t, err)
}

func TestRecoveryReconstructWithWrongPatternFails(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryReconstructSSTablesWrongPatternFail")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	writeSSTableInDatabaseFolder(t, db, fmt.Sprintf(SSTablePattern+"-", 1337))

	err := db.reconstructSSTables()
	assert.Equal(t, &strconv.NumError{
		Func: "ParseInt",
		Num:  "000000000001337-",
		Err:  strconv.ErrSyntax,
	}, err)
}

func TestRecoveryMalformedCompactionDeleted(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryMalformedCompactionDeleted")
	defer cleanDatabaseFolder(t, db)

	// closing early so we can screw some compaction up manually
	closeDatabase(t, db)
	compactionPath := fmt.Sprintf("%s-%d", SSTableCompactionPathPrefix, 1337)
	writeSSTableInDatabaseFolder(t, db, compactionPath)
	err := db.repairCompactions()
	assert.Nil(t, err)
	// the path should be deleted
	_, err = os.Stat(filepath.Join(db.basePath, compactionPath))
	assert.Truef(t, os.IsNotExist(err), "%v", err)
}

func TestRecoveryMalformedCompactionMetadataDeleted(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryMalformedCompactionMetadataDeleted")
	defer cleanDatabaseFolder(t, db)

	// closing early so we can screw some compaction up manually
	closeDatabase(t, db)
	compactionPath := fmt.Sprintf("%s-%d", SSTableCompactionPathPrefix, 1337)
	writeSSTableInDatabaseFolder(t, db, compactionPath)
	// we'll add a broken metadata (empty file) into the compaction path
	writeEmptyMetadataInDatabaseFolder(t, db, compactionPath)

	err := db.repairCompactions()
	assert.Nil(t, err)
	_, err = os.Stat(filepath.Join(db.basePath, compactionPath))
	assert.Truef(t, os.IsNotExist(err), "%v", err)
}

func TestRecoveryMultiMalformedCompactionMetadataDeleted(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoveryMultiMalformedCompactionMetadataDeleted")
	defer cleanDatabaseFolder(t, db)

	// closing early so we can screw some compaction up manually
	closeDatabase(t, db)
	var malformedPaths []string
	for i := 0; i < 5; i++ {
		compactionPath := fmt.Sprintf("%s-%d", SSTableCompactionPathPrefix, i)
		writeSSTableInDatabaseFolder(t, db, compactionPath)
		// we'll add a broken metadata (empty file) into the compaction path
		writeEmptyMetadataInDatabaseFolder(t, db, compactionPath)
		malformedPaths = append(malformedPaths, compactionPath)
	}

	err := db.repairCompactions()
	assert.Nil(t, err)

	for _, malformedPath := range malformedPaths {
		_, err = os.Stat(filepath.Join(db.basePath, malformedPath))
		assert.Truef(t, os.IsNotExist(err), "%v", err)
	}
}

func TestRecoverySuccessfulCompaction(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoverySuccessfulCompaction")
	defer cleanDatabaseFolder(t, db)

	// closing early so we can screw fake a successful compaction
	closeDatabase(t, db)
	// put an existing sstable (from which the compaction originated), to make sure we properly delete it
	replacementSSTablePath := fmt.Sprintf(SSTablePattern, 1337)
	writeSSTableInDatabaseFolder(t, db, replacementSSTablePath)
	// also a fake path we pretend to compact to ensure it is deleted later
	otherCompactionPath := fmt.Sprintf(SSTablePattern, 1338)
	writeSSTableInDatabaseFolder(t, db, otherCompactionPath)

	compactionPath := fmt.Sprintf("%s-%d", SSTableCompactionPathPrefix, 1337)
	absCompactionPath := filepath.Join(db.basePath, compactionPath)
	replacementTablePath := fmt.Sprintf(SSTablePattern, 1337)
	writeSSTableInDatabaseFolder(t, db, compactionPath)
	compMeta := &dbproto.CompactionMetadata{
		WritePath:       compactionPath,
		ReplacementPath: replacementTablePath,
		SstablePaths:    []string{replacementTablePath, otherCompactionPath},
	}
	assert.Nil(t, saveCompactionMetadata(absCompactionPath, compMeta))

	err := db.repairCompactions()
	assert.Nil(t, err)
	// the old compaction path should be deleted
	_, err = os.Stat(absCompactionPath)
	assert.Truef(t, os.IsNotExist(err), "%v", err)
	// there should be a compaction success in the replacement path, including the sstable that we wrote
	_, err = os.Stat(filepath.Join(db.basePath, replacementTablePath, CompactionFinishedSuccessfulFileName))
	assert.Nil(t, err)
	_, err = os.Stat(filepath.Join(db.basePath, replacementTablePath, sstables.IndexFileName))
	assert.Nil(t, err)

	// the fake table we added should also be deleted
	_, err = os.Stat(filepath.Join(db.basePath, otherCompactionPath))
	assert.Truef(t, os.IsNotExist(err), "%v", err)

	// sstables should be picked up after that
	err = db.reconstructSSTables()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(db.sstableManager.allSSTableReaders))
	assert.Equal(t, filepath.Join(db.basePath, replacementTablePath), db.sstableManager.currentReader.BasePath())

	// just for the clean up to work
	assert.Nil(t, db.sstableManager.currentReader.Close())
}

func TestRecoveryWALHappyPath(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpledb_recoverySuccessfulWAL")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	// create a fake WAL
	err := createWALWithEntries(db, []*dbproto.WalMutation{
		{Mutation: &dbproto.WalMutation_Addition{
			Addition: &dbproto.UpsertMutation{
				Key:   "hello",
				Value: "world",
			},
		}},
	})

	assert.Nil(t, err)

	err = db.replayAndSetupWriteAheadLog()
	assert.Nil(t, err)

	v, err := db.Get("hello")
	assert.Nil(t, err)
	assert.Equal(t, "world", v)
}

func createWALWithEntries(db *DB, mutations []*dbproto.WalMutation) error {
	// close the current WAL to overwrite the state
	err := db.wal.Close()
	if err != nil {
		return err
	}
	p := filepath.Join(db.basePath, WriteAheadFolder)
	err = os.Remove(filepath.Join(p, "000000.wal"))
	if err != nil {
		return err
	}
	opts, err := wal.NewWriteAheadLogOptions(wal.BasePath(p))
	if err != nil {
		return err
	}
	appender, err := wal.NewAppender(opts)
	if err != nil {
		return err
	}

	for i, _ := range mutations {
		marshal, err := proto.Marshal(mutations[i])
		if err != nil {
			return err
		}

		err = appender.Append(marshal)
		if err != nil {
			return err
		}
	}

	return appender.Close()
}

func writeSSTableInDatabaseFolder(t *testing.T, db *DB, p string) {
	fakeTablePath := filepath.Join(db.basePath, p)
	assert.Nil(t, os.MkdirAll(fakeTablePath, 0700))
	mStore := memstore.NewMemStore()
	assert.Nil(t, mStore.Add([]byte("hello"), []byte("world")))
	assert.Nil(t, mStore.Flush(
		sstables.WriteBasePath(fakeTablePath),
		sstables.WithKeyComparator(db.cmp),
	))
}

func writeEmptyMetadataInDatabaseFolder(t *testing.T, db *DB, compactionPath string) {
	metadata, err := os.Create(filepath.Join(db.basePath, compactionPath, CompactionFinishedSuccessfulFileName))
	assert.Nil(t, err)
	assert.Nil(t, metadata.Close())
}
