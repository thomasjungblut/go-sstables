package benchmark

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"testing"
)

func BenchmarkSSTableMemstoreFlush(b *testing.B) {
	benchmarks := []struct {
		name         string
		memstoreSize int
	}{
		{"32mb", 1024 * 1024 * 32},
		{"64mb", 1024 * 1024 * 64},
		{"128mb", 1024 * 1024 * 128},
		{"256mb", 1024 * 1024 * 256},
		{"512mb", 1024 * 1024 * 512},
		{"1024mb", 1024 * 1024 * 1024},
		{"2048mb", 1024 * 1024 * 1024 * 2},
	}

	cmp := skiplist.BytesComparator{}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			mStore := memstore.NewMemStore()
			bytes := randomRecordOfSize(1024)

			i := 0
			for mStore.EstimatedSizeInBytes() < uint64(bm.memstoreSize) {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(i))
				assert.Nil(b, mStore.Add(k, bytes))
				i++
			}

			var tmpDirs []string
			for n := 0; n < b.N; n++ {
				tmpDir, err := os.MkdirTemp("", "sstable_BenchWrite")
				assert.Nil(b, err)
				tmpDirs = append(tmpDirs, tmpDir)
			}

			defer func() {
				for i := 0; i < b.N; i++ {
					assert.Nil(b, os.RemoveAll(tmpDirs[i]))
				}
			}()

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				err := mStore.Flush(sstables.WriteBasePath(tmpDirs[n]),
					sstables.WithKeyComparator(cmp), sstables.WriteBufferSizeBytes(1024*1024*4))
				assert.Nil(b, err)
				b.SetBytes(int64(mStore.EstimatedSizeInBytes()))
			}
		})
	}
}
