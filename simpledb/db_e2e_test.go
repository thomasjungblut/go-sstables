// +build simpleDBe2e

// disabling the race detector as this is a 10-20 minute thing for the below tests
package simpledb

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

// those are end2end tests for the whole package, some are very heavyweight

func TestPutOverlappingRangesEndToEnd(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndOverlap")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	// writing the same set of keys with a static 5mb record value
	r := randomRecord(5 * 1024 * 1024)
	numKeys := 100
	for n := 0; n < 5; n++ {
		for i := 0; i < numKeys; i++ {
			is := strconv.Itoa(i)
			assert.Nil(t, db.Put(is, recordWithSuffix(i, r)))
			// make sure that we currentSSTable the same thing we just put in there
			assertGet(t, db, is)

			// delete every second element
			if i%2 == 0 {
				assert.Nil(t, db.Delete(is))
			}
		}
	}

	for i := 0; i < numKeys; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			_, err := db.Get(key)
			assert.Equalf(t, NotFound, err, "found element %d", i)
		} else {
			assertGet(t, db, key)
		}
	}
}

func TestPutAndGetsAndDeletesMixedEndToEnd(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndMixedDeletes")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	testWriteAlternatingDeletes(t, db, 2000)
}

// that test writes a couple of integers as keys and very big string values
// to trigger the memstore flushes/table compactions
func TestPutAndGetsEndToEndLargerData(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDBWithSize(t, "simpleDB_EndToEndLargerData", 1024*1024*256)
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	testWriteAlternatingDeletes(t, db, 5000)
}

func TestPutAndGetsAndDeletesMixedConcurrent(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndMixedDeletesConcurrent")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	wg := sync.WaitGroup{}

	maxRoutines := 50
	rangeOverlap := 2
	multiplier := 250

	for numRoutines := 0; numRoutines < maxRoutines; numRoutines++ {
		wg.Add(1)
		go func(start, end int) {
			for i := start; i < end; i++ {
				is := strconv.Itoa(i)
				assert.Nil(t, db.Put(is, randomRecordWithPrefix(i)))
				if i%2 == 0 {
					assert.Nil(t, db.Delete(is))
				}
			}

			wg.Done()
			// the overlap is intended to check concurrent inserts/delete on the same keys
		}(numRoutines*multiplier, (numRoutines+rangeOverlap)*multiplier)
	}

	wg.Wait()

	// determine that the database is the expected set of keys
	// eg. we shall not expect any even key anymore
	for j := 0; j < maxRoutines*multiplier; j++ {
		key := strconv.Itoa(j)
		if j%2 == 0 {
			_, err := db.Get(key)
			assert.Equal(t, NotFound, err)
		} else {
			assertGet(t, db, key)
		}
	}
}

func TestRecoveryFromCloseHappyPath(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_RecoveryFromClose")
	defer cleanDatabaseFolder(t, db)

	n := 500
	testWriteAlternatingDeletes(t, db, n)
	assert.Nil(t, db.Close())

	db, err := NewSimpleDB(db.basePath, MemstoreSizeBytes(1024*1024))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer closeDatabase(t, db)

	for j := 0; j < n; j++ {
		key := strconv.Itoa(j)
		if j%2 == 0 {
			v, err := db.Get(key)
			assert.Equalf(t, NotFound, err, "found %d in the table where it should've been deleted", j)
			assert.Equal(t, "", v)
		} else {
			assertGet(t, db, key)
		}
	}
}

func TestNaiveCrashRecovery(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_RecoveryFromCrashNaive")
	defer cleanDatabaseFolder(t, db)

	n := 500
	testWriteAlternatingDeletes(t, db, n)
	// TODO(thomas): this is super naive
	// we poke holes by directly closing some files, channels and deleting the memstore
	// which doesn't necessary simulate a proper power failure
	close(db.storeFlushChannel)
	assert.Nil(t, db.wal.Close())
	db.memStore = nil
	assert.Nil(t, db.sstableManager.currentReader.Close()) // this is mostly to clean the folder properly later

	db, err := NewSimpleDB(db.basePath, MemstoreSizeBytes(1024*1024))
	assert.Nil(t, err)
	assert.Nil(t, db.Open())
	defer closeDatabase(t, db)

	for j := 0; j < n; j++ {
		key := strconv.Itoa(j)
		if j%2 == 0 {
			v, err := db.Get(key)
			assert.Equalf(t, NotFound, err, "found %d in the table where it should've been deleted", j)
			assert.Equal(t, "", v)
		} else {
			assertGet(t, db, key)
		}
	}
}

func testWriteAlternatingDeletes(t *testing.T, db *DB, n int) {
	for i := 0; i < n; i++ {
		is := strconv.Itoa(i)
		assert.Nil(t, db.Put(is, randomRecordWithPrefix(i)))
		// make sure that we currentSSTable the same thing we just put in there
		assertGet(t, db, is)

		for j := 0; j < i; j++ {
			key := strconv.Itoa(j)
			if j%2 == 0 {
				v, err := db.Get(key)
				assert.Equalf(t, NotFound, err, "found %d in the table where it should've been deleted", j)
				assert.Equal(t, "", v)
			} else {
				assertGet(t, db, key)
			}
		}

		// delete every second element
		if i%2 == 0 {
			assert.Nil(t, db.Delete(is))
		}
	}
}
