package compressor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleGzipCompression(t *testing.T) {
	comp := GzipCompressor{}
	data := "some data"

	compressedBytes, err := comp.Compress([]byte(data))
	assert.Nil(t, err)
	assert.Equal(t, 33, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleGzipCompressionWithBuffers(t *testing.T) {
	comp := GzipCompressor{}
	data := "some data"

	destBuf := make([]byte, 33)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 33, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleGzipCompressionWithSmallerBuffer(t *testing.T) {
	comp := GzipCompressor{}
	data := "some data"

	destBuf := make([]byte, 30)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 33, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleGzipCompressionWithLargerBuffer(t *testing.T) {
	comp := GzipCompressor{}
	data := "some data"

	destBuf := make([]byte, 40)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 33, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}
