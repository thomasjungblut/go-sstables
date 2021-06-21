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
	defer os.RemoveAll(db.basePath)
	defer assert.Nil(t, db.Close())

	assert.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)

	assert.Nil(t, db.Close())
}

func TestGetNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testGetNotFound")
	defer os.RemoveAll(db.basePath)
	defer assert.Nil(t, db.Close())

	_, err := db.Get("a")
	assert.Equal(t, NotFound, err)
}

func TestDeleteNotFound(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testDeleteNotFound")
	defer os.RemoveAll(db.basePath)
	defer assert.Nil(t, db.Close())

	err := db.Delete("a")
	assert.Equal(t, NotFound, err)
}

func TestSimplePutAndGetAndDelete(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_testSimplePutAndGetAndDelete")
	defer os.RemoveAll(db.basePath)
	defer assert.Nil(t, db.Close())

	_, err := db.Get("a")
	assert.Equal(t, NotFound, err)

	assert.Nil(t, db.Put("a", "b"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)

	assert.Nil(t, db.Delete("a"))

	_, err = db.Get("a")
	assert.Equal(t, NotFound, err)

	assert.Nil(t, db.Close())
}
