package benchmark

import (
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/internal/testutil"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
)

const benchFileSize = 1024 * 1024 * 16

var benchRecordSizes = []int{16, 128, 1024, 64 * 1024}

var benchCompressionTypes = []struct {
	name string
	comp int
}{
	{"none", recordio.CompressionTypeNone},
	{"snappy", recordio.CompressionTypeSnappy},
	{"gzip", recordio.CompressionTypeGZIP},
	{"lzw", recordio.CompressionTypeLzw},
}

// writeBenchFile writes records of the given size until benchFileSize is reached and returns the path, the offsets
// of all records and the total file size.
func writeBenchFile(b *testing.B, recordSize int, compType int) (string, []uint64, int64) {
	path := filepath.Join(benchDir(b), "bench.rio")
	w, err := recordio.NewFileWriter(recordio.Path(path), recordio.CompressionType(compType))
	require.NoError(b, err)
	require.NoError(b, w.Open())

	record := testutil.Bytes(recordSize)
	var offsets []uint64
	for w.Size() < benchFileSize {
		off, err := w.Write(record)
		require.NoError(b, err)
		offsets = append(offsets, off)
	}
	require.NoError(b, w.Close())

	stat, err := os.Stat(path)
	require.NoError(b, err)
	return path, offsets, stat.Size()
}

func BenchmarkRecordIOReadRecordSizes(b *testing.B) {
	for _, c := range benchCompressionTypes {
		for _, size := range benchRecordSizes {
			b.Run(fmt.Sprintf("%s/%d", c.name, size), func(b *testing.B) {
				path, _, fileSize := writeBenchFile(b, size, c.comp)
				b.SetBytes(fileSize)
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					dropPageCache(b, path)
					b.StartTimer()

					reader, err := recordio.NewFileReaderWithPath(path)
					require.NoError(b, err)
					require.NoError(b, reader.Open())
					for {
						_, err := reader.ReadNext()
						if errors.Is(err, io.EOF) {
							break
						}
						require.NoError(b, err)
					}
					require.NoError(b, reader.Close())
				}
			})
		}
	}
}

func BenchmarkRecordIOSkipNext(b *testing.B) {
	for _, size := range benchRecordSizes {
		b.Run(fmt.Sprintf("none/%d", size), func(b *testing.B) {
			path, _, fileSize := writeBenchFile(b, size, recordio.CompressionTypeNone)
			b.SetBytes(fileSize)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				dropPageCache(b, path)
				b.StartTimer()

				reader, err := recordio.NewFileReaderWithPath(path)
				require.NoError(b, err)
				require.NoError(b, reader.Open())
				for {
					err := reader.SkipNext()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(b, err)
				}
				require.NoError(b, reader.Close())
			}
		})
	}
}

func BenchmarkRecordIOMMapReadNextAt(b *testing.B) {
	for _, c := range benchCompressionTypes {
		for _, size := range benchRecordSizes {
			b.Run(fmt.Sprintf("%s/%d", c.name, size), func(b *testing.B) {
				path, offsets, fileSize := writeBenchFile(b, size, c.comp)
				b.SetBytes(fileSize)
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					reader := openColdMMapReader(b, path)
					for _, off := range offsets {
						_, err := reader.ReadNextAt(off)
						if err != nil {
							b.Fatal(err)
						}
					}
					require.NoError(b, reader.Close())
				}
			})
		}
	}
}

func BenchmarkRecordIOMMapSeekNext(b *testing.B) {
	for _, size := range benchRecordSizes {
		b.Run(fmt.Sprintf("none/%d", size), func(b *testing.B) {
			path, offsets, fileSize := writeBenchFile(b, size, recordio.CompressionTypeNone)
			b.SetBytes(fileSize)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				reader := openColdMMapReader(b, path)
				// seek from just past each record start, which forces a scan to the next record
				for _, off := range offsets[:len(offsets)-1] {
					_, _, err := reader.SeekNext(off + 1)
					if err != nil {
						b.Fatal(err)
					}
				}
				require.NoError(b, reader.Close())
			}
		})
	}
}

// openColdMMapReader drops the file from the page cache and maps it, both outside the measured time.
func openColdMMapReader(b *testing.B, path string) recordio.ReadAtI {
	b.StopTimer()
	defer b.StartTimer()
	dropPageCache(b, path)
	reader, err := recordio.NewMemoryMappedReaderWithPath(path)
	require.NoError(b, err)
	require.NoError(b, reader.Open())
	return reader
}
