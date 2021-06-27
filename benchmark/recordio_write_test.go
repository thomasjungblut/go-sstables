package benchmark

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/recordio"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkRecordIOWriteRecordSize1k(b *testing.B) {
	benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIOWriteRecordSize10k(b *testing.B) {
	benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIORecordIOWriteRecordSize100k(b *testing.B) {
	benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIOWriteRecordSize1m(b *testing.B) {
	benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeNone, b)
}

func BenchmarkRecordIOWriteGzipRecordSize1k(b *testing.B) {
	benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeGZIP, b)
}
func BenchmarkRecordIOWriteGzipRecordSize10k(b *testing.B) {
	benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeGZIP, b)
}
func BenchmarkRecordIOWriteGzipRecordSize100k(b *testing.B) {
	benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeGZIP, b)
}
func BenchmarkRecordIOWriteGzipRecordSize1m(b *testing.B) {
	benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeGZIP, b)
}

func BenchmarkRecordIOWriteSnappyRecordSize1k(b *testing.B) {
	benchmarkWriteRecordSize(1024, false, recordio.CompressionTypeSnappy, b)
}
func BenchmarkRecordIOWriteSnappyRecordSize10k(b *testing.B) {
	benchmarkWriteRecordSize(1024*10, false, recordio.CompressionTypeSnappy, b)
}
func BenchmarkRecordIOWriteSnappyRecordSize100k(b *testing.B) {
	benchmarkWriteRecordSize(1024*100, false, recordio.CompressionTypeSnappy, b)
}
func BenchmarkRecordIOWriteSnappyRecordSize1m(b *testing.B) {
	benchmarkWriteRecordSize(1024*1024, false, recordio.CompressionTypeSnappy, b)
}

func BenchmarkRecordIOWriteSyncRecordSize1k(b *testing.B) {
	benchmarkWriteRecordSize(1024, true, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIOWriteSyncRecordSize10k(b *testing.B) {
	benchmarkWriteRecordSize(1024*10, true, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIOWriteSyncRecordSize100k(b *testing.B) {
	benchmarkWriteRecordSize(1024*100, true, recordio.CompressionTypeNone, b)
}
func BenchmarkRecordIOWriteSyncRecordSize1m(b *testing.B) {
	benchmarkWriteRecordSize(1024*1024, true, recordio.CompressionTypeNone, b)
}

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

		writer, err := recordio.NewFileWriter(recordio.File(tmpFile), recordio.CompressionType(compType))
		assert.Nil(b, err)
		assert.Nil(b, writer.Open())

		b.StartTimer()
		for i := 0; i < numRecords; i++ {
			if sync {
				_, err := writer.WriteSync(bytes)
				assert.Nil(b, err)
			} else {
				_, err := writer.Write(bytes)
				assert.Nil(b, err)
			}
		}
		b.StopTimer()

		assert.Nil(b, writer.Close())

		// report the size of the written file for throughput metrics
		stat, err := os.Stat(tmpFile.Name())
		assert.Nil(b, err)
		b.SetBytes(stat.Size())
	}
}
