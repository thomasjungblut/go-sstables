package simpledb

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreationWhenDirNotAvailable(t *testing.T) {
	// apologies if such a directory actually exists in your filesystem
	_, err := NewSimpleDB("ääääääüüüü")
	assert.True(t, os.IsNotExist(err), "folder apparently exists")
}

func TestOperationUnopened(t *testing.T) {
	db := newSimpleDBWithTemp(t, "simpleDB_testClosingUnopened")
	defer cleanDatabaseFolder(t, db)

	assert.Equal(t, db.Close(), ErrNotOpenedYet)
	assert.Equal(t, db.Put("a", "b"), ErrNotOpenedYet)
	assert.Equal(t, db.Delete("a"), ErrNotOpenedYet)
	_, err := db.Get("a")
	assert.Equal(t, err, ErrNotOpenedYet)
}

func TestOpenErrOnOpenedDb(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGet")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, db.Open(), ErrAlreadyOpen)
}

func TestDoubleClose(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGet")
	defer cleanDatabaseFolder(t, db)
	closeDatabase(t, db)

	assert.Equal(t, db.Close(), ErrAlreadyClosed)
}

func TestSimplePutAndGet(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGet")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	require.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	require.Nil(t, err)
	assert.Equal(t, "b", val)
}

func TestGetNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testGetNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	_, err := db.Get("a")
	assert.Equal(t, ErrNotFound, err)
}

func TestEmptyPutDisallowed(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testEmptyPut")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	err := db.Put("a", "")
	assert.Equal(t, ErrEmptyKeyValue, err)

	err = db.Put("", "")
	assert.Equal(t, ErrEmptyKeyValue, err)

	err = db.Put("", "a")
	assert.Equal(t, ErrEmptyKeyValue, err)
}

func TestDeleteNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	err := db.Delete("a")
	require.Nil(t, err)
}

func TestPutDeleteGetNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)
	require.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	require.Nil(t, err)
	assert.Equal(t, "b", val)
	require.Nil(t, db.Delete("a"))

	_, err = db.Get("a")
	assert.Equal(t, ErrNotFound, err)
}

func TestPutAndGetAndDelete(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGetAndDelete")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	_, err := db.Get("a")
	assert.Equal(t, ErrNotFound, err)

	require.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	require.Nil(t, err)
	assert.Equal(t, "b", val)

	require.Nil(t, db.Delete("a"))

	_, err = db.Get("a")
	assert.Equal(t, ErrNotFound, err)
}

func TestCloseDeniesCrudOperations(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testCloseDenies")
	defer cleanDatabaseFolder(t, db)
	closeDatabase(t, db)

	_, err := db.Get("somehow")
	assert.Equal(t, ErrAlreadyClosed, err)
	err = db.Put("somehow", "somewhat")
	assert.Equal(t, ErrAlreadyClosed, err)
	err = db.Delete("somehow")
	assert.Equal(t, ErrAlreadyClosed, err)
}

func TestDisableCompactions(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testDisabledCompactions")
	require.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, DisableCompactions())
	require.Nil(t, err)
	require.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	require.Nil(t, db.compactionTicker)
}

func TestCompactionsMaxSize(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsMaxSize")
	require.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionMaxSizeBytes(1255))
	require.Nil(t, err)
	require.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, uint64(1255), db.compactedMaxSizeBytes)
}

func TestCompactionsRunInterval(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsRunInterval")
	require.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionRunInterval(1*time.Second))
	require.Nil(t, err)
	require.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, 1*time.Second, db.compactionInterval)
	assert.NotNil(t, db.compactionTicker)
}

func TestCompactionsFileThreshold(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsFileThreshold")
	require.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionFileThreshold(1337))
	require.Nil(t, err)
	require.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, 1337, db.compactionThreshold)
}

func assertGet(t *testing.T, db *DB, key string) {
	val, err := db.Get(key)
	require.Nil(t, err)
	if len(val) > 2 {
		val = val[:len(key)]
	}
	assert.Truef(t, strings.HasPrefix(val, key),
		"expected key %s as prefix, but was %s", key, val)
}

func recordWithSuffix(prefix int, suffix string) string {
	builder := strings.Builder{}
	builder.WriteString(strconv.Itoa(prefix))
	builder.WriteString("_")
	builder.WriteString(suffix)

	return builder.String()
}

func randomString(rand *rand.Rand, size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
}

func randomRecordWithPrefixWithSize(rand *rand.Rand, prefix, size int) string {
	builder := strings.Builder{}
	builder.WriteString(strconv.Itoa(prefix))
	builder.WriteString("_")
	for i := 0; i < size; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
}

func randomRecordWithPrefix(rand *rand.Rand, prefix int) string {
	return randomRecordWithPrefixWithSize(rand, prefix, 10000)
}

func newSimpleDBWithTemp(t *testing.T, name string) *DB {
	tmpDir, err := ioutil.TempDir("", name)
	require.Nil(t, err)

	//for testing purposes we will flush with a tiny amount of 2mb
	db, err := NewSimpleDB(tmpDir, MemstoreSizeBytes(1024*1024*2))
	require.Nil(t, err)
	return db
}

func newOpenedSimpleDB(t *testing.T, name string) *DB {
	db := newSimpleDBWithTemp(t, name)
	require.Nil(t, db.Open())
	return db
}

func closeDatabase(t *testing.T, db *DB) {
	func(t *testing.T, db *DB) { require.Nil(t, db.Close()) }(t, db)
}

func cleanDatabaseFolder(t *testing.T, db *DB) {
	func(t *testing.T, db *DB) { require.Nil(t, os.RemoveAll(db.basePath)) }(t, db)
}

func tryCleanDatabaseFolder(db *DB) {
	func(db *DB) {
		_ = db.Close()
		_ = os.RemoveAll(db.basePath)
	}(db)
}
