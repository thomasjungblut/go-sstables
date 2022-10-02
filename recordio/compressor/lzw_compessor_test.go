package compressor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimpleLzwCompression(t *testing.T) {
	comp := LzwCompressor{}
	data := "some data"

	compressedBytes, err := comp.Compress([]byte(data))
	assert.Nil(t, err)
	assert.Equal(t, 13, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleLzwCompressionWithBuffers(t *testing.T) {
	comp := LzwCompressor{}
	data := "some data"

	destBuf := make([]byte, 33)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 13, len(compressedBytes))

	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleLzwCompressionWithSmallerBuffer(t *testing.T) {
	comp := LzwCompressor{}
	data := "some data"

	destBuf := make([]byte, 30)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 13, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}

func TestSimpleLzwCompressionWithLargerBuffer(t *testing.T) {
	comp := LzwCompressor{}
	data := "some data"

	destBuf := make([]byte, 40)
	compressedBytes, err := comp.CompressWithBuf([]byte(data), destBuf)
	assert.Nil(t, err)
	assert.Equal(t, 13, len(compressedBytes))
	decompressAndCheck(t, &comp, compressedBytes, data, 9)
}
