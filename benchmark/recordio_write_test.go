package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/internal/testutil"
	"github.com/thomasjungblut/go-sstables/recordio"
)

func BenchmarkRecordIOWrite(b *testing.B) {
	benchmarks := []struct {
		name     string
		recSize  int
		sync     bool
		compType int
	}{
		{"RecordSize1k", 1024, false, recordio.CompressionTypeNone},
		{"RecordSize10k", 1024 * 10, false, recordio.CompressionTypeNone},
		{"RecordSize100k", 1024 * 100, false, recordio.CompressionTypeNone},
		{"RecordSize1M", 1024 * 1000, false, recordio.CompressionTypeNone},

		{"GzipRecordSize1k", 1024, false, recordio.CompressionTypeGZIP},
		{"GzipRecordSize10k", 1024 * 10, false, recordio.CompressionTypeGZIP},
		{"GzipRecordSize100k", 1024 * 100, false, recordio.CompressionTypeGZIP},
		{"GzipRecordSize1M", 1024 * 1000, false, recordio.CompressionTypeGZIP},

		{"SnappyRecordSize1k", 1024, false, recordio.CompressionTypeSnappy},
		{"SnappyRecordSize10k", 1024 * 10, false, recordio.CompressionTypeSnappy},
		{"SnappyRecordSize100k", 1024 * 100, false, recordio.CompressionTypeSnappy},
		{"SnappyRecordSize1M", 1024 * 1000, false, recordio.CompressionTypeSnappy},

		{"LzwRecordSize1k", 1024, false, recordio.CompressionTypeLzw},
		{"LzwRecordSize10k", 1024 * 10, false, recordio.CompressionTypeLzw},
		{"LzwRecordSize100k", 1024 * 100, false, recordio.CompressionTypeLzw},
		{"LzwRecordSize1M", 1024 * 1000, false, recordio.CompressionTypeLzw},

		{"SyncRecordSize1k", 1024, true, recordio.CompressionTypeNone},
		{"SyncRecordSize10k", 1024 * 10, true, recordio.CompressionTypeNone},
		{"SyncRecordSize100k", 1024 * 100, true, recordio.CompressionTypeNone},
		{"SyncRecordSize1M", 1024 * 1000, true, recordio.CompressionTypeNone},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bytes := testutil.Bytes(bm.recSize)
			tmpFile, err := os.CreateTemp(benchDir(b), "recordio_Bench")
			assert.Nil(b, err)
			defer os.Remove(tmpFile.Name())

			writer, err := recordio.NewFileWriter(recordio.File(tmpFile), recordio.CompressionType(bm.compType))
			assert.Nil(b, err)
			assert.Nil(b, writer.Open())

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if bm.sync {
					_, _ = writer.WriteSync(bytes)
				} else {
					_, _ = writer.Write(bytes)
				}
				b.SetBytes(int64(len(bytes)))
			}

			// closing syncs the file, which is part of the measured time
			assert.Nil(b, writer.Close())
			b.StopTimer()
			stat, err := os.Stat(tmpFile.Name())
			assert.Nil(b, err)
			assert.Truef(b, stat.Size() > int64(len(bytes)*b.N), "unexpected small file size %d", stat.Size())
		})
	}
}

// BenchmarkRecordIOWriteDontCache compares the write throughput with and without DontCache, including the fsyncs.
func BenchmarkRecordIOWriteDontCache(b *testing.B) {
	for _, recordSize := range []int{4 * 1024, 64 * 1024} {
		for _, syncEvery := range []int{0, 1024 * 1024} {
			for _, mode := range [][]recordio.FileWriterOption{
				{recordio.DontCache()},
				nil,
			} {
				syncName := "syncOnClose"
				if syncEvery > 0 {
					mb := syncEvery / 1024 / 1024
					syncName = fmt.Sprintf("syncEvery%dMB", mb)
				}
				name := "Default"
				if mode != nil {
					name = "DontCache"
				}
				b.Run(fmt.Sprintf("%d/%s/%s", recordSize, syncName, name), func(b *testing.B) {
					dir := benchDir(b)
					bytes := 256 * 1024 * 1024
					b.SetBytes(int64(bytes))
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						path := filepath.Join(dir, fmt.Sprintf("write_%d.rio", n))
						w, err := recordio.NewFileWriter(append([]recordio.FileWriterOption{recordio.Path(path)}, mode...)...)
						require.NoError(b, err)
						require.NoError(b, w.Open())

						record := testutil.Bytes(recordSize)
						for written := 0; written < bytes; written += recordSize {
							if syncEvery > 0 && (written+recordSize)%syncEvery == 0 {
								_, err = w.WriteSync(record)
							} else {
								_, err = w.Write(record)
							}
							require.NoError(b, err)
						}
						require.NoError(b, w.Close())

						b.StopTimer()
						require.NoError(b, os.Remove(path))
						b.StartTimer()
					}
				})
			}
		}
	}
}
