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

// those are end2end tests for the whole package

func TestPutAndGetsEndToEnd(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_EndToEnd")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	assert.Nil(t, db.Put("a", "b"))
	assert.Nil(t, db.Put("b", "c"))

	val, err := db.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, "b", val)

	val, err = db.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, "c", val)
}

// that test writes a couple of integers as keys and very big string values
// to trigger the memstore flushes/table merges
// the test here roughly produces 150MB in WAL and a final sstable of 114mb
func TestPutAndGetsEndToEndLargerData(t *testing.T) {
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndLargerData")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	for i := 0; i < 10000; i++ {
		assert.Nil(t, db.Put(strconv.Itoa(i), randomRecordWithPrefix(i)))
	}
}

func randomRecordWithPrefix(prefix int) string {
	builder := strings.Builder{}
	builder.WriteString(strconv.Itoa(prefix))
	builder.WriteString("_")
	for i := 0; i < 10000; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
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

func closeDatabase(t *testing.T, db *DB) {
	func() { assert.Nil(t, db.Close()) }()
}

func cleanDatabaseFolder(t *testing.T, db *DB) {
	func() { assert.Nil(t, os.RemoveAll(db.basePath)) }()
}
