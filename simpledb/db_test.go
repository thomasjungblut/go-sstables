package simpledb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
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

func randomString(size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
}

func randomRecordWithPrefixWithSize(prefix, size int) string {
	builder := strings.Builder{}
	builder.WriteString(strconv.Itoa(prefix))
	builder.WriteString("_")
	for i := 0; i < size; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
}

func randomRecordWithPrefix(prefix int) string {
	return randomRecordWithPrefixWithSize(prefix, 10000)
}

func newOpenedSimpleDBWithSize(t *testing.T, name string, memstoreSize int) *DB {
	tmpDir, err := ioutil.TempDir("", name)
	assert.Nil(t, err)

	db, err := NewSimpleDB(tmpDir, MemstoreSizeBytes(uint64(memstoreSize)))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	return db
}

func newOpenedSimpleDB(t *testing.T, name string) *DB {
	tmpDir, err := ioutil.TempDir("", name)
	assert.Nil(t, err)

	//for testing purposes we will flush with a tiny amount of 1mb
	db, err := NewSimpleDB(tmpDir, MemstoreSizeBytes(1024*1024))
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
