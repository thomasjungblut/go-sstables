// +build !race

package simpledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

// those are end2end tests for the whole package

func TestPutAndGetsEndToEnd(t *testing.T) {
	t.Parallel()
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

func TestPutOverlappingRangesEndToEnd(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndOverlap")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	// writing the same set of keys with a static 5mb record value
	r := randomRecord(5 * 1024 * 1024)
	numKeys := 100
	for n := 0; n < 5; n++ {
		fmt.Printf("iteration %d\n", n)
		for i := 0; i < numKeys; i++ {
			is := strconv.Itoa(i)
			assert.Nil(t, db.Put(is, recordWithSuffix(i, r)))
			// make sure that we get the same thing we just put in there
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

	for i := 0; i < 2000; i++ {
		is := strconv.Itoa(i)
		assert.Nil(t, db.Put(is, randomRecordWithPrefix(i)))
		// make sure that we get the same thing we just put in there
		assertGet(t, db, is)

		for j := 0; j < i; j++ {
			key := strconv.Itoa(j)
			if j%2 == 0 {
				_, err := db.Get(key)
				assert.Equal(t, NotFound, err)
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

// that test writes a couple of integers as keys and very big string values
// to trigger the memstore flushes/table merges
// the test here roughly produces 143MB in WAL and a final sstable of 114mb
func TestPutAndGetsEndToEndLargerData(t *testing.T) {
	t.Parallel()
	db := newOpenedSimpleDB(t, "simpleDB_EndToEndLargerData")
	defer cleanDatabaseFolder(t, db)
	defer closeDatabase(t, db)

	for i := 0; i < 10000; i++ {
		assert.Nil(t, db.Put(strconv.Itoa(i), randomRecordWithPrefix(i)))
		// try to scan 5% of the previous elements, otherwise the runtime becomes too long (30s)
		// TODO we can probably fix some performance issues here too
		for j := 0; j <= i; j++ {
			if rand.Float32() < 0.05 {
				key := strconv.Itoa(j)
				assertGet(t, db, key)
			}
		}
	}
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
		go func(start, end int) {
			wg.Add(1)
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
