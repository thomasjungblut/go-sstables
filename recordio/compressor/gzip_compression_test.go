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

	decompressedBytes, err := comp.Decompress(compressedBytes)
	assert.Nil(t, err)
	assert.Equal(t, 9, len(decompressedBytes))

	assert.Equal(t, data, string(decompressedBytes))
}
