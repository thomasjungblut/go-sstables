package benchmark

import (
	"errors"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"testing"
	"time"

	"crypto/sha1"
	"encoding/binary"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
)

const sizeTwoGigs = 1024 * 1024 * 1024 * 2

var cmp = skiplist.BytesComparator{}
var sizeBasedBenchmarks = []struct {
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

var loadTypeBenchmarks = []struct {
	name   string
	loader sstables.IndexLoader
}{
	{"skiplist", &sstables.SkipListIndexLoader{
		KeyComparator:  cmp,
		ReadBufferSize: 4096,
	}},
	{"slice", &sstables.SliceKeyIndexLoader{ReadBufferSize: 4096}},
	{"map", &sstables.MapKeyIndexLoader[[20]byte]{ReadBufferSize: 4096, Mapper: &sstables.Byte20KeyMapper{}}},
	{"disk", &sstables.DiskIndexLoader{}},
}

func BenchmarkSSTableScanDefault(b *testing.B) {
	for _, bm := range sizeBasedBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "sstable_BenchRead_"+bm.name)
			require.NoError(b, err)
			defer func() { require.NoError(b, os.RemoveAll(tmpDir)) }()

			writeSSTableWithSize(b, bm.memstoreSize, tmpDir, cmp)

			b.ResetTimer()
			fullScanTable(b, tmpDir, cmp, nil)
		})
	}
}

func BenchmarkSSTableRandomReadDefault(b *testing.B) {
	for _, bm := range sizeBasedBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "sstable_BenchRead_"+bm.name)
			require.NoError(b, err)
			defer func() { require.NoError(b, os.RemoveAll(tmpDir)) }()

			keys := writeSSTableWithSize(b, bm.memstoreSize, tmpDir, cmp)

			opts := []sstables.ReadOption{
				sstables.ReadBasePath(tmpDir),
				sstables.ReadWithKeyComparator(cmp),
				sstables.SkipHashCheckOnLoad(),
			}

			reader, err := sstables.NewSSTableReader(opts...)

			defer func() {
				require.NoError(b, reader.Close())
			}()

			randomRead(b, reader, keys)
		})
	}
}

func BenchmarkSSTableScanByReadIndexTypes(b *testing.B) {
	for _, bm := range loadTypeBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "sstable_BenchReadIndexLoad_"+bm.name)
			require.NoError(b, err)
			defer func() { require.NoError(b, os.RemoveAll(tmpDir)) }()

			writeSSTableWithSize(b, sizeTwoGigs, tmpDir, cmp)

			b.ResetTimer()
			fullScanTable(b, tmpDir, cmp, bm.loader)
		})
	}
}

func BenchmarkSSTableRandomReadByIndexTypes(b *testing.B) {
	for _, bm := range loadTypeBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "sstable_BenchReadIndexLoad_"+bm.name)
			require.NoError(b, err)
			defer func() { require.NoError(b, os.RemoveAll(tmpDir)) }()

			keys := writeSSTableWithSize(b, sizeTwoGigs, tmpDir, cmp)

			opts := []sstables.ReadOption{
				sstables.ReadBasePath(tmpDir),
				sstables.ReadWithKeyComparator(cmp),
				sstables.SkipHashCheckOnLoad(),
			}
			if bm.loader != nil {
				opts = append(opts, sstables.ReadIndexLoader(bm.loader))
			}

			reader, err := sstables.NewSSTableReader(opts...)
			defer func() {
				require.NoError(b, reader.Close())
			}()

			randomRead(b, reader, keys)
		})
	}
}

func writeSSTableWithSize(b *testing.B, sizeBytes int, tmpDir string, cmp skiplist.BytesComparator) [][]byte {
	mStore := memstore.NewMemStore()
	bytes := randomRecordOfSize(1024)

	var keys [][]byte
	i := 0
	for mStore.EstimatedSizeInBytes() < uint64(sizeBytes) {
		kx := make([]byte, 4)
		binary.BigEndian.PutUint32(kx, uint32(i))
		hash := sha1.New()
		hash.Write(kx)

		k := hash.Sum([]byte{})
		require.NoError(b, mStore.Add(k, bytes))

		// to keep the memory bound, we only store the first two million keys
		if len(keys) < 2000000 {
			keys = append(keys, k)
		}

		i++
	}

	require.NoError(b, mStore.Flush(sstables.WriteBasePath(tmpDir), sstables.WithKeyComparator(cmp)))
	return keys
}

func fullScanTable(b *testing.B, tmpDir string, cmp skiplist.Comparator[[]byte], loader sstables.IndexLoader) {
	for i := 0; i < b.N; i++ {
		loadStart := time.Now()
		opts := []sstables.ReadOption{
			sstables.ReadBasePath(tmpDir),
			sstables.ReadWithKeyComparator(cmp),
			sstables.SkipHashCheckOnLoad(),
		}
		if loader != nil {
			opts = append(opts, sstables.ReadIndexLoader(loader))
		}

		reader, err := sstables.NewSSTableReader(opts...)
		require.NoError(b, err)

		loadEnd := time.Now().Sub(loadStart)
		b.ReportMetric(float64(loadEnd.Milliseconds()), "load_time_ms")
		b.ReportMetric(float64(loadEnd.Nanoseconds())/float64(reader.MetaData().NumRecords), "load_time_ns/record")
		b.ReportMetric(float64(reader.MetaData().IndexBytes), "index_bytes")
		b.ReportMetric(float64(reader.MetaData().IndexBytes)/1024/1024/loadEnd.Seconds(), "load_bandwidth_mb/s")

		defer func() {
			require.NoError(b, reader.Close())
		}()

		scanStart := time.Now()
		scanner, err := reader.Scan()
		require.NoError(b, err)
		i := uint64(0)
		for {
			_, _, err := scanner.Next()
			if errors.Is(err, sstables.Done) {
				break
			} else {
				require.NoError(b, err)
			}
			i++
		}
		if reader.MetaData().NumRecords != i {
			b.Fail()
		}
		scanEnd := time.Now().Sub(scanStart)
		b.SetBytes(int64(reader.MetaData().TotalBytes))
		b.ReportMetric(float64(scanEnd.Milliseconds()), "scan_time_ms")
		b.ReportMetric(float64(scanEnd.Nanoseconds())/float64(i), "scan_time_ns/record")
		b.ReportMetric(float64(i), "num_records")
		b.ReportMetric(float64(reader.MetaData().DataBytes)/1024/1024/scanEnd.Seconds(), "scan_bandwidth_mb/s")
	}
}

func randomRead(b *testing.B, reader sstables.SSTableReaderI, keys [][]byte) {
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	b.ResetTimer()

	ix := 0
	for i := 0; i < b.N; i++ {
		get, err := reader.Get(keys[ix])
		require.NoError(b, err)
		require.Equal(b, 1024, len(get))

		ix += 1
		if ix >= len(keys) {
			ix = 0
		}
	}
}
