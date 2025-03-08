package benchmark

import (
	"errors"
	"github.com/stretchr/testify/assert"
	bProto "github.com/thomasjungblut/go-sstables/benchmark/proto"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"io"
	"os"
	"testing"
)

func BenchmarkRecordIORead(b *testing.B) {
	benchmarks := []struct {
		name     string
		fileSize int
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

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bytes := randomRecordOfSize(1024)
			tmpFile, err := os.CreateTemp("", "recordio_Bench")
			assert.NoError(b, err)
			defer os.Remove(tmpFile.Name())

			writer, err := recordio.NewFileWriter(recordio.File(tmpFile))
			assert.NoError(b, err)
			assert.NoError(b, writer.Open())

			for writer.Size() < uint64(bm.fileSize) {
				_, _ = writer.Write(bytes)
			}
			b.SetBytes(int64(writer.Size()))
			assert.NoError(b, writer.Close())

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				reader, err := recordio.NewFileReader(recordio.ReaderPath(tmpFile.Name()))
				assert.NoError(b, err)
				assert.NoError(b, reader.Open())

				for {
					_, err := reader.ReadNext()
					if errors.Is(err, io.EOF) {
						break
					}
				}

				assert.NoError(b, reader.Close())
			}
		})
	}
}

func BenchmarkRecordIOProtoRead(b *testing.B) {
	benchmarks := []struct {
		name     string
		fileSize int
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

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bytes := randomRecordOfSize(1024)
			tmpFile, err := os.CreateTemp("", "recordio_Bench")
			assert.NoError(b, err)
			defer os.Remove(tmpFile.Name())

			writer, err := rProto.NewWriter(rProto.File(tmpFile))
			assert.NoError(b, err)
			assert.NoError(b, writer.Open())

			msg := &bProto.BytesMsg{Key: bytes}
			for writer.Size() < uint64(bm.fileSize) {
				_, _ = writer.Write(msg)
			}
			b.SetBytes(int64(writer.Size()))
			assert.NoError(b, writer.Close())

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				reader, err := rProto.NewReader(rProto.ReaderPath(tmpFile.Name()))
				assert.NoError(b, err)
				assert.NoError(b, reader.Open())

				msg := &bProto.BytesMsg{}
				for {
					_, err := reader.ReadNext(msg)
					if errors.Is(err, io.EOF) {
						break
					}
				}

				assert.NoError(b, reader.Close())
			}
		})
	}

}
