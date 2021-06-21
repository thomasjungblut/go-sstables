package simpledb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

// those are end2end tests for the whole package

func TestPutAndGetsEndToEnd(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_EndToEnd")
	defer os.RemoveAll(db.basePath)
	defer assert.Nil(t, db.Close())

	assert.Nil(t, db.Put("a", "b"))
	assert.Nil(t, db.Put("b", "c"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)

	val, err = db.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, "c", val)
}

// TODO think of a good end2end test that will write sufficient amount of data in a pattern and delete/query accordingly

func newOpenedSimpleDB(t *testing.T, name string) *DB {
	tmpDir, err := ioutil.TempDir("", name)
	assert.Nil(t, err)

	db, err := NewSimpleDB(tmpDir)
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	return db
}
