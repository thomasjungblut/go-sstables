// +build simpleDBe2e

// disabling the race detector as this is a 10-20 minute thing for the below tests
package simpledb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// those are end2end tests for the whole package, some are very heavyweight

func init() {
	// to save some log space, the logs with flushes are very verbose on the small memstore sizes
	log.SetOutput(ioutil.Discard)
}

func TestPutOverlappingRangesEndToEnd(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(0))
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndOverlap")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	// writing the same set of keys with a static 5mb record value
	r := randomString(rnd, 5*1024*1024)
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

func TestPutAndDeleteRandomKeysEndToEnd(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(0))
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndRandomKeys")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	r := randomString(rnd, 1024*1024)
	var keys []string
	for i := 0; i < 500; i++ {
		keys = append(keys, randomString(rnd, 10))
		assert.Nil(t, db.Put(keys[i], r))
	}

	assertDatabaseContains(t, db, keys)

	var expectedKeys []string
	var deletedKeys []string
	for i := 0; i < len(keys); i++ {
		if i%2 == 0 {
			expectedKeys = append(expectedKeys, keys[i])
		} else {
			deletedKeys = append(deletedKeys, keys[i])
			assert.Nil(t, db.Delete(keys[i]))
		}
	}

	assertDatabaseContains(t, db, expectedKeys)
	assertDatabaseNotContains(t, db, deletedKeys)
}

func TestPutAndDeleteRandomKeysReplacementEndToEnd(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(0))
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndRandomKeysWithReplacement")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	r := randomString(rnd, 1024*1024)
	var keys []string
	for i := 0; i < 500; i++ {
		keys = append(keys, randomString(rnd, 10))
		assert.Nil(t, db.Put(keys[i], r))
	}

	assertDatabaseContains(t, db, keys)

	// we try to add and delete ten times, a random subset of the initial key set
	for i := 0; i < 10; i++ {
		var expectedKeys []string
		var deletedKeys []string
		for i := 0; i < len(keys); i++ {
			if rand.Float32() > 0.5 {
				expectedKeys = append(expectedKeys, keys[i])
			} else {
				deletedKeys = append(deletedKeys, keys[i])
				assert.Nil(t, db.Delete(keys[i]))
			}
		}

		assertDatabaseContains(t, db, expectedKeys)
		assertDatabaseNotContains(t, db, deletedKeys)

		// add all of them back
		for i := 0; i < len(keys); i++ {
			assert.Nil(t, db.Put(keys[i], r))
		}
		assertDatabaseContains(t, db, keys)
	}
}

func TestPutAndGetsAndDeletesMixedEndToEnd(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndMixedDeletes")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	testWriteAlternatingDeletes(t, db, 2500)
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
			rnd := rand.New(rand.NewSource(int64(start)))
			for i := start; i < end; i++ {
				is := strconv.Itoa(i)
				assert.Nil(t, db.Put(is, randomRecordWithPrefix(rnd, i)))
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
	crashDatabaseInternally(t, db)

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

func TestContinuousCrashRecovery(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_RecoveryFromCrashContinuous")
	dbsToClose := []*DB{db}
	defer func() {
		for i := len(dbsToClose) - 1; i >= 0; i-- {
			_ = dbsToClose[i].Close()
		}

		cleanDatabaseFolder(t, db)
	}()

	for i := 1; i < 10; i++ {
		n := 100 * i // to make sure we overwrite a certain amount and new data
		testWriteAlternatingDeletes(t, db, n)
		crashDatabaseInternally(t, db)

		var err error
		db, err = NewSimpleDB(db.basePath, MemstoreSizeBytes(1024*1024),
			CompactionRunInterval(1*time.Second), CompactionFileThreshold(2))
		assert.Nil(t, err)
		dbsToClose = append(dbsToClose, db)
		assert.Nil(t, db.Open())

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

}

// TODO(thomas): this is super naive
// we poke holes by directly closing some files, channels and deleting the memstore
// which doesn't necessary simulate a proper power failure
func crashDatabaseInternally(t *testing.T, db *DB) {
	log.Println("crashing the database")
	close(db.storeFlushChannel)
	db.compactionTicker.Stop()
	db.compactionTickerStopChannel <- true
	close(db.compactionTickerStopChannel)
	assert.Nil(t, db.wal.Close())
	db.memStore = nil
	assert.Nil(t, db.sstableManager.currentReader.Close()) // this is mostly to clean the folder properly later
}

func testWriteAlternatingDeletes(t *testing.T, db *DB, n int) {
	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < n; i++ {
		is := strconv.Itoa(i)
		assert.Nil(t, db.Put(is, randomRecordWithPrefix(rnd, i)))
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

func assertDatabaseContains(t *testing.T, db *DB, keys []string) {
	for _, k := range keys {
		v, err := db.Get(k)
		assert.Nil(t, err)
		assert.NotNilf(t, v, "expecting value for key %v, but was nil", k)
	}
}

func assertDatabaseNotContains(t *testing.T, db *DB, keys []string) {
	for _, k := range keys {
		v, err := db.Get(k)
		assert.Equalf(t, NotFound, err, "found %v in the table where it should've been deleted", k)
		assert.Equal(t, "", v)
	}
}
