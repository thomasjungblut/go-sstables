package compressor

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var allCompressors = []CompressionI{&GzipCompressor{}, &SnappyCompressor{}, LzwCompressor{}}

func testRecords(t *testing.T) [][]byte {
	var records [][]byte
	for _, size := range []int{0, 1, 17, 511, 512, 513, 4096, 70_000, 1 << 20} {
		random := make([]byte, size)
		_, err := rand.Read(random)
		require.NoError(t, err)
		records = append(records, random, bytes.Repeat([]byte{'a', 'b'}, size/2))
	}
	return records
}

func TestCompressBoundHoldsForIncompressibleData(t *testing.T) {
	for _, c := range allCompressors {
		for _, record := range testRecords(t) {
			compressed, err := c.Compress(record)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(compressed), c.CompressBound(len(record)), "%T with %d bytes", c, len(record))
		}
	}
}

func TestRoundTripWithBufferSizes(t *testing.T) {
	for _, c := range allCompressors {
		for _, record := range testRecords(t) {
			compressed, err := c.CompressWithBuf(record, make([]byte, c.CompressBound(len(record))))
			require.NoError(t, err)

			for _, destSize := range []int{0, len(record) / 2, len(record), len(record) + 1} {
				decompressed, err := c.DecompressWithBuf(compressed, make([]byte, destSize))
				require.NoError(t, err)
				assert.True(t, bytes.Equal(record, decompressed), "%T with %d bytes, dest %d", c, len(record), destSize)
			}

			decompressed, err := c.Decompress(compressed)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(record, decompressed))
		}
	}
}

func TestDecompressExactBufferIsNotReallocated(t *testing.T) {
	for _, c := range []CompressionI{&GzipCompressor{}, LzwCompressor{}} {
		record := bytes.Repeat([]byte{'x', 'y', 'z'}, 10_000)
		compressed, err := c.Compress(record)
		require.NoError(t, err)

		dest := make([]byte, len(record))
		decompressed, err := c.DecompressWithBuf(compressed, dest)
		require.NoError(t, err)
		assert.Equal(t, record, decompressed)
		assert.Same(t, &dest[0], &decompressed[0], "%T reallocated an exactly sized buffer", c)
	}
}

func TestPooledCompressorsConcurrently(t *testing.T) {
	for _, c := range allCompressors {
		t.Run(fmt.Sprintf("%T", c), func(t *testing.T) {
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						record := bytes.Repeat([]byte{byte(g), byte(i)}, 100+i)
						compressed, err := c.CompressWithBuf(record, nil)
						if !assert.NoError(t, err) {
							return
						}
						decompressed, err := c.DecompressWithBuf(compressed, make([]byte, len(record)))
						if !assert.NoError(t, err) || !assert.Equal(t, record, decompressed) {
							return
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}

func TestGzipDecompressInvalidInput(t *testing.T) {
	c := &GzipCompressor{}
	_, err := c.Decompress([]byte("not gzip"))
	assert.Error(t, err)
	// the pooled reader must still be usable afterwards
	compressed, err := c.Compress([]byte("hello"))
	require.NoError(t, err)
	decompressed, err := c.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), decompressed)
}
