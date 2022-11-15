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
	"sync"
	"testing"
)

type TestDatabase struct {
	db       *simpledb.DB
	basePath string
}

func TestHappyPath(t *testing.T) {
	db := newOpenedSimpleDB(t, "linearizability_HappyPath")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	key := "key"
	client := NewDatabaseRecorder(db.db, 0)
	for i := 0; i < 100; i++ {
		_, _ = client.Get(key)
		_ = client.Put(key, randomString(5))
		if rand.Float32() < 0.25 {
			_ = client.Delete(key)
		}
	}

	verifyOperations(t, client.operations)
}

func TestHappyPathMultiKey(t *testing.T) {
	db := newOpenedSimpleDB(t, "linearizability_HappyPathMultiKey")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	client := NewDatabaseRecorder(db.db, 0)
	for i := 0; i < 100; i++ {
		key := randomString(5)
		_, _ = client.Get(key)
		_ = client.Put(key, randomString(5))
		_, _ = client.Get(key)
		if rand.Float32() < 0.5 {
			_ = client.Delete(key)
		}
		_, _ = client.Get(key)
	}

	verifyOperations(t, client.operations)
}

func TestHappyPathMultiThread(t *testing.T) {
	db := newOpenedSimpleDB(t, "linearizability_HappyPathMultiThread")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	operations := parallelWriteGetDelete(db.db, 4, 100, 2500)
	verifyOperations(t, operations)
}

func TestMultiTriggerFlush(t *testing.T) {
	db := newOpenedSimpleDB(t, "linearizability_HappyPathMultiThread")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	operations := parallelWriteGetDelete(db.db, 8, 8*1000, 2500)
	verifyOperations(t, operations)
}

func verifyOperations(t *testing.T, operations []porcupine.Operation) {
	result, info := porcupine.CheckOperationsVerbose(Model, operations, 0)
	require.NoError(t, porcupine.VisualizePath(Model, info, t.Name()+"_porcupine.html"))
	require.Equal(t, porcupine.CheckResult(porcupine.Ok), result, "output was not linearizable")
}

func newSimpleDBWithTemp(t *testing.T, name string) *TestDatabase {
	tmpDir, err := os.MkdirTemp("", name)
	require.Nil(t, err)

	//for testing purposes we will flush with a tiny amount of 1mb
	db, err := simpledb.NewSimpleDB(tmpDir, simpledb.MemstoreSizeBytes(1024*1024*1))
	require.Nil(t, err)
	return &TestDatabase{
		db:       db,
		basePath: tmpDir,
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

func parallelWriteGetDelete(db *simpledb.DB, numGoRoutines int, numRecords int, valSizeBytes int) []porcupine.Operation {
	var operations []porcupine.Operation
	var opsLock sync.Mutex
	wg := sync.WaitGroup{}
	recordsPerRoutine := numRecords / numGoRoutines
	var keys []string
	var values []string
	for i := 0; i < recordsPerRoutine; i++ {
		keys = append(keys, randomString(5))
		values = append(values, randomString(valSizeBytes))
	}
	for n := 0; n < numGoRoutines; n++ {
		wg.Add(1)
		go func(db *simpledb.DB, id, start, end int) {
			client := NewDatabaseRecorder(db, id)
			rnd := rand.New(rand.NewSource(int64(id)))
			for i := start; i < end; i++ {
				// that ensures some overlap in the requests
				key := keys[rnd.Intn(len(keys))]
				val := values[rnd.Intn(len(keys))]
				_, _ = client.Get(key)
				_ = client.Put(key, val)
				_, _ = client.Get(key)
				if rnd.Float32() < 0.5 {
					_ = client.Delete(key)
				}
				_, _ = client.Get(key)
			}

			opsLock.Lock()
			defer opsLock.Unlock()

			operations = append(operations, client.operations...)

			wg.Done()
		}(db, n, n*recordsPerRoutine, n*recordsPerRoutine+recordsPerRoutine)
	}

	wg.Wait()

	return operations
}
