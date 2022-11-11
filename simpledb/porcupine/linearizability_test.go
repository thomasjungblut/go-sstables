//go:build simpleDBlinear
// +build simpleDBlinear

package porcupine

import (
	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/simpledb"
	"math/rand"
	"os"
	"strings"
	"testing"
)

type TestDatabase struct {
	db         *simpledb.DB
	recordedDb *DatabaseRecorder
	basePath   string
}

func TestHappyPath(t *testing.T) {
	db := newOpenedSimpleDB(t, "linearizability_HappyPath")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	key := "key"

	for i := 0; i < 10; i++ {
		_, _ = db.recordedDb.Get(key)
		_ = db.recordedDb.Put(key, randomString(5))
		if rand.Float32() < 0.25 {
			_ = db.recordedDb.Delete(key)
		}
	}

	verifyOperations(t, db.recordedDb.operations)
}

func verifyOperations(t *testing.T, operations []porcupine.Operation) {
	result, info := porcupine.CheckOperationsVerbose(Model, operations, 0)
	require.NoError(t, porcupine.VisualizePath(Model, info, t.Name()+"_porcupine.html"))
	require.Equal(t, porcupine.CheckResult(porcupine.Ok), result, "output was not linearizable")
}

func newSimpleDBWithTemp(t *testing.T, name string) *TestDatabase {
	tmpDir, err := os.MkdirTemp("", name)
	require.Nil(t, err)

	//for testing purposes we will flush with a tiny amount of 2mb
	db, err := simpledb.NewSimpleDB(tmpDir, simpledb.MemstoreSizeBytes(1024*1024*2))
	require.Nil(t, err)
	return &TestDatabase{
		db:         db,
		recordedDb: NewDatabaseRecorder(db),
		basePath:   tmpDir,
	}
}

func newOpenedSimpleDB(t *testing.T, name string) *TestDatabase {
	db := newSimpleDBWithTemp(t, name)
	require.Nil(t, db.db.Open())
	return db
}

func closeDatabase(t *testing.T, db *TestDatabase) {
	func(t *testing.T, db *simpledb.DB) { require.Nil(t, db.Close()) }(t, db.db)
}

func cleanDatabaseFolder(t *testing.T, db *TestDatabase) {
	func(t *testing.T, basePath string) { require.Nil(t, os.RemoveAll(basePath)) }(t, db.basePath)
}

func randomString(size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		builder.WriteRune(rand.Int31n(26) + 97)
	}

	return builder.String()
}
