package benchmark

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thomasjungblut/go-sstables/internal/testutil"
	"github.com/thomasjungblut/go-sstables/recordio"
)

const dontCacheWriteSize = 256 * 1024 * 1024

func dontCacheModes() []struct {
	name string
	opts []recordio.FileWriterOption
} {
	return []struct {
		name string
		opts []recordio.FileWriterOption
	}{
		{"cached", nil},
		{"dontCache", []recordio.FileWriterOption{recordio.DontCache()}},
	}
}

// writeRecords writes size bytes in records of recordSize into a new file, calling WriteSync every syncEvery bytes
// (0 only syncs on Close).
func writeRecords(b *testing.B, path string, size, recordSize, syncEvery int, opts ...recordio.FileWriterOption) {
	w, err := recordio.NewFileWriter(append([]recordio.FileWriterOption{recordio.Path(path)}, opts...)...)
	require.NoError(b, err)
	require.NoError(b, w.Open())

	record := testutil.Bytes(recordSize)
	for written := 0; written < size; written += recordSize {
		if syncEvery > 0 && (written+recordSize)%syncEvery == 0 {
			_, err = w.WriteSync(record)
		} else {
			_, err = w.Write(record)
		}
		require.NoError(b, err)
	}
	require.NoError(b, w.Close())
}

// BenchmarkRecordIOWriteDontCache compares the write throughput with and without DontCache, including the fsyncs.
func BenchmarkRecordIOWriteDontCache(b *testing.B) {
	for _, recordSize := range []int{4 * 1024, 64 * 1024} {
		for _, syncEvery := range []int{0, 1024 * 1024} {
			for _, mode := range dontCacheModes() {
				syncName := "syncOnClose"
				if syncEvery > 0 {
					syncName = "syncEvery1MB"
				}
				b.Run(fmt.Sprintf("%d/%s/%s", recordSize, syncName, mode.name), func(b *testing.B) {
					dir := benchDir(b)
					b.SetBytes(dontCacheWriteSize)
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						path := filepath.Join(dir, fmt.Sprintf("write_%d.rio", n))
						writeRecords(b, path, dontCacheWriteSize, recordSize, syncEvery, mode.opts...)

						b.StopTimer()
						require.NoError(b, os.Remove(path))
						b.StartTimer()
					}
				})
			}
		}
	}
}

// BenchmarkRecordIODontCachePagePollution shows how much a large write pushes other data out of the page cache: it
// warms a hot file (e.g. an SSTable that is read often), writes 2 GB with or without DontCache (e.g. a WAL) and then
// measures re-reading the hot file. Only meaningful with a memory limit below the written size. Only the re-read is
// timed, which is fast when the hot file stays cached, so go test would run many untimed 2 GB writes. Use a fixed
// number of iterations instead, for example:
//
//	systemd-run --user --scope -p MemoryMax=1G -- go test -run xxx -benchtime 3x -bench DontCachePagePollution ./benchmark
func BenchmarkRecordIODontCachePagePollution(b *testing.B) {
	const hotSize = 256 * 1024 * 1024
	const pollutionSize = 2 * 1024 * 1024 * 1024

	for _, mode := range dontCacheModes() {
		b.Run(mode.name, func(b *testing.B) {
			dir := benchDir(b)
			hotPath := filepath.Join(dir, "hot.rio")
			writeRecords(b, hotPath, hotSize, 64*1024, 0)

			b.SetBytes(hotSize)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				dropPageCache(b, hotPath)
				readFile(b, hotPath)
				walPath := filepath.Join(dir, "wal.rio")
				writeRecords(b, walPath, pollutionSize, 64*1024, 1024*1024, mode.opts...)
				cached, err := cachedFraction(hotPath)
				require.NoError(b, err)
				b.ReportMetric(cached*100, "hot_cached_%")
				b.StartTimer()

				readFile(b, hotPath)

				b.StopTimer()
				require.NoError(b, os.Remove(walPath))
				b.StartTimer()
			}
		})
	}
}

func readFile(b *testing.B, path string) {
	f, err := os.Open(path)
	require.NoError(b, err)
	defer func() { _ = f.Close() }()
	_, err = io.Copy(io.Discard, f)
	require.NoError(b, err)
}
