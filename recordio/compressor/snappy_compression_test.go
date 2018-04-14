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

	decompressedBytes, err := comp.Decompress(compressedBytes)
	assert.Nil(t, err)
	assert.Equal(t, 9, len(decompressedBytes))

	assert.Equal(t, data, string(decompressedBytes))
}
