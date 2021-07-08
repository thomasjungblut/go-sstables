package compressor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleSnappyCompression(t *testing.T) {
	comp := SnappyCompressor{}
	data := "some data"

	compressedBytes, err := comp.Compress([]byte(data))
	assert.Nil(t, err)
	assert.Equal(t, 11, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleSnappyCompressionWithBuffers(t *testing.T) {
	comp := SnappyCompressor{}
	data := "some data"

	destBuf := make([]byte, 11)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleSnappyCompressionWithSmallerBuffer(t *testing.T) {
	comp := SnappyCompressor{}
	data := "some data"

	destBuf := make([]byte, 10)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleSnappyCompressionWithLargerBuffer(t *testing.T) {
	comp := SnappyCompressor{}
	data := "some data"

	destBuf := make([]byte, 15)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func decompressAndCheck(t *testing.T, comp CompressionI, compressedBytes []byte, expectedData string, expectedByteSize int) {
	decompressedBytes, err := comp.Decompress(compressedBytes)
	assert.Nil(t, err)
	assert.Equal(t, expectedByteSize, len(decompressedBytes))
	assert.Equal(t, expectedData, string(decompressedBytes))
}
