package simpledb

import (
	"github.com/stretchr/testify/assert"
	"os"
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

func TestDeleteNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	err := db.Delete("a")
	assert.Equal(t, NotFound, err)
}

func TestSimplePutAndGetAndDelete(t *testing.T) {
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
