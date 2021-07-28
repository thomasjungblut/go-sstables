package simpledb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCreationWhenDirNotAvailable(t *testing.T) {
	// apologies if such a directory actually exists in your filesystem
	_, err := NewSimpleDB("ääääääüüüü")
	assert.True(t, os.IsNotExist(err), "folder apparently exists")
}

func TestSimplePutAndGet(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGet")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)
}

func TestGetNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testGetNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	_, err := db.Get("a")
	assert.Equal(t, NotFound, err)
}

func TestEmptyPutDisallowed(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testEmptyPut")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	err := db.Put("a", "")
	assert.Equal(t, EmptyKeyValue, err)

	err = db.Put("", "")
	assert.Equal(t, EmptyKeyValue, err)

	err = db.Put("", "a")
	assert.Equal(t, EmptyKeyValue, err)
}

func TestDeleteNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	err := db.Delete("a")
	assert.Nil(t, err)
}

func TestPutDeleteGetNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)
	assert.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)
	assert.Nil(t, db.Delete("a"))

	_, err = db.Get("a")
	assert.Equal(t, NotFound, err)
}

func TestPutAndGetAndDelete(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGetAndDelete")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	_, err := db.Get("a")
	assert.Equal(t, NotFound, err)

	assert.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)

	assert.Nil(t, db.Delete("a"))

	_, err = db.Get("a")
	assert.Equal(t, NotFound, err)
}

func TestCloseDeniesCrudOperations(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testCloseDenies")
	defer cleanDatabaseFolder(t, db)
	closeDatabase(t, db)

	_, err := db.Get("somehow")
	assert.Equal(t, AlreadyClosed, err)
	err = db.Put("somehow", "somewhat")
	assert.Equal(t, AlreadyClosed, err)
	err = db.Delete("somehow")
	assert.Equal(t, AlreadyClosed, err)
}

func TestDisableCompactions(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testDisabledCompactions")
	assert.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, DisableCompactions())
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Nil(t, db.compactionTicker)
}

func TestCompactionsMaxSize(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsMaxSize")
	assert.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionMaxSizeBytes(1255))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, uint64(1255), db.compactedMaxSizeBytes)
}

func TestCompactionsRunInterval(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsRunInterval")
	assert.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionRunInterval(1*time.Second))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, 1*time.Second, db.compactionInterval)
	assert.NotNil(t, db.compactionTicker)
}

func TestCompactionsFileThreshold(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "simpleDB_testCompactionsFileThreshold")
	assert.Nil(t, err)
	db, err := NewSimpleDB(tmpDir, CompactionFileThreshold(1337))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Equal(t, 1337, db.compactionThreshold)
}

func assertGet(t *testing.T, db *DB, key string) {
	val, err := db.Get(key)
	assert.Nil(t, err)
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

func newOpenedSimpleDB(t *testing.T, name string) *DB {
	tmpDir, err := ioutil.TempDir("", name)
	assert.Nil(t, err)

	//for testing purposes we will flush with a tiny amount of 2mb
	db, err := NewSimpleDB(tmpDir, MemstoreSizeBytes(1024*1024*2))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	return db
}

func closeDatabase(t *testing.T, db *DB) {
	func() { assert.Nil(t, db.Close()) }()
}

func cleanDatabaseFolder(t *testing.T, db *DB) {
	func() { assert.Nil(t, os.RemoveAll(db.basePath)) }()
}
