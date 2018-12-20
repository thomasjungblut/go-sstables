package benchmark

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/recordio"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkWriteRecordSize1k(b *testing.B)   { benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeNone, b) }
func BenchmarkWriteRecordSize10k(b *testing.B)  { benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeNone, b) }
func BenchmarkWriteRecordSize100k(b *testing.B) { benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeNone, b) }
func BenchmarkWriteRecordSize1m(b *testing.B)   { benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeNone, b) }

func BenchmarkWriteGzipRecordSize1k(b *testing.B)   { benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeGZIP, b) }
func BenchmarkWriteGzipRecordSize10k(b *testing.B)  { benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeGZIP, b) }
func BenchmarkWriteGzipRecordSize100k(b *testing.B) { benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeGZIP, b) }
func BenchmarkWriteGzipRecordSize1m(b *testing.B)   { benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeGZIP, b) }

func BenchmarkWriteSnappyRecordSize1k(b *testing.B)   { benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeSnappy, b) }
func BenchmarkWriteSnappyRecordSize10k(b *testing.B)  { benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeSnappy, b) }
func BenchmarkWriteSnappyRecordSize100k(b *testing.B) { benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeSnappy, b) }
func BenchmarkWriteSnappyRecordSize1m(b *testing.B)   { benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeSnappy, b) }

func BenchmarkWriteSyncRecordSize1k(b *testing.B)   { benchmarkWriteRecordSize(1024, true, recordio.CompressionTypeNone, b) }
func BenchmarkWriteSyncRecordSize10k(b *testing.B)  { benchmarkWriteRecordSize(1024*10, true, recordio.CompressionTypeNone, b) }
func BenchmarkWriteSyncRecordSize100k(b *testing.B) { benchmarkWriteRecordSize(1024*100, true, recordio.CompressionTypeNone, b) }
func BenchmarkWriteSyncRecordSize1m(b *testing.B)   { benchmarkWriteRecordSize(1024*1024, true, recordio.CompressionTypeNone, b) }

func randomRecordOfSize(l int) []byte {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(rand.Intn(255))
	}

	return bytes
}

//noinspection GoDeferInLoop
func benchmarkWriteRecordSize(recSize int, sync bool, compType int, b *testing.B) {
	const numRecords = 1000 // should be max ~1G on the 1m record size
	// TODO(thomas): since this is random data, there is not much sense in benchmarking the compression really
	bytes := randomRecordOfSize(recSize)
	for n := 0; n < b.N; n++ {
		tmpFile, err := ioutil.TempFile("", "recordio_Bench")
		assert.Nil(b, err)
		defer os.Remove(tmpFile.Name())

		writer, err := recordio.NewCompressedFileWriterWithFile(tmpFile, compType)
		assert.Nil(b, err)
		assert.Nil(b, writer.Open())

		for i := 0; i < numRecords; i++ {
			if sync {
				_, err := writer.WriteSync(bytes)
				assert.Nil(b, err)
			} else {
				_, err := writer.Write(bytes)
				assert.Nil(b, err)
			}
		}

		assert.Nil(b, writer.Close())

		// report the size of the written file for throughput metrics
		stat, err := os.Stat(tmpFile.Name())
		assert.Nil(b, err)
		b.SetBytes(stat.Size())
	}
}
