package benchmark

import (
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"testing"
	"time"
)

// we're writing a gig worth of data and test how long it takes to read the index + all data
func BenchmarkSSTableRead(b *testing.B) {
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
		{"4096mb", 1024 * 1024 * 1024 * 4},
		{"8192mb", 1024 * 1024 * 1024 * 8},
	}

	cmp := skiplist.BytesComparator{}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "sstable_BenchRead_"+bm.name)
			assert.Nil(b, err)
			defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()

			mStore := memstore.NewMemStore()
			bytes := randomRecordOfSize(1024)

			i := 0
			for mStore.EstimatedSizeInBytes() < uint64(bm.memstoreSize) {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(i))
				assert.Nil(b, mStore.Add(k, bytes))
				i++
			}

			assert.Nil(b, mStore.Flush(sstables.WriteBasePath(tmpDir), sstables.WithKeyComparator(cmp)))
			defer func() {
				assert.Nil(b, os.RemoveAll(tmpDir))
			}()

			b.ResetTimer()
			fullScanTable(b, tmpDir, cmp)
		})
	}
}

func fullScanTable(b *testing.B, tmpDir string, cmp skiplist.Comparator[[]byte]) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loadStart := time.Now()
		reader, err := sstables.NewSSTableReader(sstables.ReadBasePath(tmpDir), sstables.ReadWithKeyComparator(cmp))
		b.ReportMetric(float64(time.Now().Sub(loadStart).Milliseconds()), "load_time_ms")
		b.ReportMetric(float64(time.Now().Sub(loadStart).Nanoseconds())/float64(reader.MetaData().NumRecords), "load_time_ns/record")

		defer func() {
			assert.Nil(b, reader.Close())
		}()

		assert.Nil(b, err)
		scanStart := time.Now()
		scanner, err := reader.Scan()
		assert.Nil(b, err)
		i := uint64(0)
		for {
			_, _, err := scanner.Next()
			if errors.Is(err, sstables.Done) {
				break
			}
			i++
		}
		if reader.MetaData().NumRecords != i {
			b.Fail()
		}
		b.SetBytes(int64(reader.MetaData().TotalBytes))
		b.ReportMetric(float64(time.Now().Sub(scanStart).Milliseconds()), "scan_time_ms")
		b.ReportMetric(float64(time.Now().Sub(scanStart).Nanoseconds())/float64(i), "scan_time_ns/record")
		b.ReportMetric(float64(i), "num_records")
	}
}
