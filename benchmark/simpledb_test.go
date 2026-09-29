package benchmark

import (
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thomasjungblut/go-sstables/internal/testutil"
	"github.com/thomasjungblut/go-sstables/simpledb"
)

const simpleDBValueSize = 1024

func BenchmarkSimpleDBReadLatency(b *testing.B) {
	log.SetOutput(io.Discard)
	dbSizes := []int{1000, 10000, 100000}

	for _, numRecords := range dbSizes {
		// memstore: all records are still in the memstore, sstable: the database was reopened and all records are
		// read from the sstables, the page cache is only dropped once and warms up during the run
		for _, source := range []string{"memstore", "sstable"} {
			b.Run(fmt.Sprintf("%s/%d", source, numRecords), func(b *testing.B) {
				tmpDir := benchDir(b)
				db := openSimpleDB(b, tmpDir)
				keys := fillSimpleDB(b, db, numRecords)

				if source == "sstable" {
					require.NoError(b, db.Close())
					db = openSimpleDB(b, tmpDir)
					dropPageCache(b, tmpDir)
				}
				// closing flushes the memstore, which must not be measured
				defer func() {
					b.StopTimer()
					require.NoError(b, db.Close())
				}()

				b.SetBytes(int64(len(keys[0]) + simpleDBValueSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := db.Get(keys[i%len(keys)])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSimpleDBWriteLatency(b *testing.B) {
	log.SetOutput(io.Discard)
	for _, bm := range []struct {
		name string
		opts []simpledb.ExtraOption
	}{
		// every Put waits for the WAL fsync, concurrent Puts share it (group commit)
		{"groupCommit", nil},
		{"groupCommitWait1ms", []simpledb.ExtraOption{simpledb.GroupCommitMaxWait(time.Millisecond)}},
		{"groupCommitWait5ms", []simpledb.ExtraOption{simpledb.GroupCommitMaxWait(5 * time.Millisecond)}},
	} {
		b.Run(bm.name, func(b *testing.B) {
			db := openSimpleDB(b, benchDir(b), bm.opts...)
			// closing flushes the memstore, which must not be measured
			defer func() {
				b.StopTimer()
				require.NoError(b, db.Close())
			}()

			val := testutil.Letters(simpleDBValueSize)
			var nextKey atomic.Int64
			b.SetBytes(int64(len(simpleDBKey(0)) + simpleDBValueSize))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := db.Put(simpleDBKey(int(nextKey.Add(1))), val)
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func openSimpleDB(b *testing.B, dir string, opts ...simpledb.ExtraOption) *simpledb.DB {
	opts = append([]simpledb.ExtraOption{simpledb.MemstoreSizeBytes(1024 * 1024 * 1024)}, opts...)
	db, err := simpledb.NewSimpleDB(dir, opts...)
	require.NoError(b, err)
	require.NoError(b, db.Open())
	return db
}

// fillSimpleDB puts numRecords records with 1 KB values and returns their keys.
func fillSimpleDB(b *testing.B, db *simpledb.DB, numRecords int) []string {
	val := testutil.Letters(simpleDBValueSize)
	keys := make([]string, numRecords)
	for i := range keys {
		keys[i] = simpleDBKey(i)
	}

	// every Put waits for an fsync, many concurrent writers make the fill much faster through group commit
	const writers = 64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(keys); i += writers {
				if err := db.Put(keys[i], val); err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	return keys
}

func simpleDBKey(i int) string {
	return fmt.Sprintf("key_%010d", i)
}
