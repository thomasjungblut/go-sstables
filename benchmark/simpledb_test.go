package benchmark

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/simpledb"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// just a handy copy&paste for easier profiling
// TODO(thomas): remove
func BenchmarkSimpleDBWriteLatencyForProfiling(b *testing.B) {
	dbSizes := []int{1000000}

	for _, n := range dbSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			tmpDir, err := ioutil.TempDir("", "simpledb_Bench")
			assert.Nil(b, err)
			defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()

			memstoreSize := uint64(1024 * 1024 * 1024)
			db, err := simpledb.NewSimpleDB(tmpDir, simpledb.MemstoreSizeBytes(memstoreSize))
			assert.Nil(b, err)
			defer func() { assert.Nil(b, db.Close()) }()
			assert.Nil(b, db.Open())

			b.ResetTimer()
			bytes := parallelWriteDB(b, db, runtime.NumCPU(), n)
			b.SetBytes(bytes)
		})
	}
}

func BenchmarkSimpleDBReadLatency(b *testing.B) {
	dbSizes := []int{100, 1000, 10000, 100000}

	for _, n := range dbSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			tmpDir, err := ioutil.TempDir("", "simpledb_Bench")
			assert.Nil(b, err)
			defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()
			db, err := simpledb.NewSimpleDB(tmpDir,
				simpledb.MemstoreSizeBytes(1024*1024*1024))
			assert.Nil(b, err)
			defer func() { assert.Nil(b, db.Close()) }()
			assert.Nil(b, db.Open())

			parallelWriteDB(b, db, runtime.NumCPU(), n)

			b.ResetTimer()
			i := 0
			for n := 0; n < b.N; n++ {
				k := strconv.Itoa(i)
				val, err := db.Get(k)
				if err != simpledb.NotFound {
					b.SetBytes(int64(len(k) + len(val)))
				}
				i++
				if i >= n {
					i = 0
				}
			}
		})
	}
}

func BenchmarkSimpleDBWriteLatency(b *testing.B) {
	dbSizes := []int{100, 1000, 10000, 100000, 1000000}

	for _, n := range dbSizes {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			tmpDir, err := ioutil.TempDir("", "simpledb_Bench")
			assert.Nil(b, err)
			defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()

			memstoreSize := uint64(1024 * 1024 * 1024)
			db, err := simpledb.NewSimpleDB(tmpDir,
				simpledb.MemstoreSizeBytes(memstoreSize))
			assert.Nil(b, err)
			defer func() { assert.Nil(b, db.Close()) }()
			assert.Nil(b, db.Open())

			b.ResetTimer()
			bytes := parallelWriteDB(b, db, runtime.NumCPU(), n)
			b.SetBytes(bytes)
		})
	}
}

func parallelWriteDB(b *testing.B, db *simpledb.DB, numGoRoutines int, numRecords int) int64 {
	log.Printf("writing %d records with %d goroutines\n", numRecords, numGoRoutines)
	start := time.Now()
	numRecordsWritten := int64(0)
	bytesWritten := int64(0)
	wg := sync.WaitGroup{}
	recordsPerRoutine := numRecords / numGoRoutines
	val := randomString()
	for n := 0; n < numGoRoutines; n++ {
		wg.Add(1)
		go func(db *simpledb.DB, start, end int) {
			for i := start; i < end; i++ {
				k := strconv.Itoa(i)
				_ = db.Put(k, val)
				atomic.AddInt64(&bytesWritten, int64(len(k)+len(val)))
				atomic.AddInt64(&numRecordsWritten, 1)
			}
			wg.Done()
		}(db, n*recordsPerRoutine, n*recordsPerRoutine+recordsPerRoutine)
	}

	wg.Wait()
	log.Printf("%d records with %d bytes written in %v\n", numRecordsWritten, bytesWritten, time.Since(start))
	return bytesWritten
}

func randomString() string {
	return randomStringSize(10000)
}

func randomStringSize(n int) string {
	builder := strings.Builder{}
	for i := 0; i < n; i++ {
		builder.WriteRune(rand.Int31n(255))
	}
	return builder.String()
}
