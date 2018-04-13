package benchmark

import (
	"testing"
	"math/rand"
	"io/ioutil"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/recordio"
	"os"
)

func BenchmarkWriteRecordSize1k(b *testing.B)   { benchmarkWriteRecordSize(1024, b) }
func BenchmarkWriteRecordSize10k(b *testing.B)  { benchmarkWriteRecordSize(1024*10, b) }
func BenchmarkWriteRecordSize100k(b *testing.B) { benchmarkWriteRecordSize(1024*100, b) }
func BenchmarkWriteRecordSize1m(b *testing.B)   { benchmarkWriteRecordSize(1024*1024, b) }

func randomRecordOfSize(l int) []byte {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(rand.Intn(255))
	}

	return bytes
}

//noinspection GoDeferInLoop
func benchmarkWriteRecordSize(recSize int, b *testing.B) {
	const numRecords = 1000 // should be ~1G on the 1m record size
	bytes := randomRecordOfSize(recSize)
	for n := 0; n < b.N; n++ {
		tmpFile, err := ioutil.TempFile("", "recordio_Bench")
		assert.Nil(b, err)
		defer os.Remove(tmpFile.Name())

		writer, err := recordio.NewFileWriterWithFile(tmpFile)
		assert.Nil(b, err)
		assert.Nil(b, writer.Open())

		for i := 0; i < numRecords; i++ {
			_, err := writer.Write(bytes)
			assert.Nil(b, err)
		}

		assert.Nil(b, writer.Close())
	}
}
