package compressor

import (
	"github.com/golang/snappy"
)

type SnappyCompressor struct {
}

func (c *SnappyCompressor) Compress(record []byte) ([]byte, error) {
	return snappy.Encode(nil, record), nil
}

func (c *SnappyCompressor) Decompress(buf []byte) ([]byte, error) {
	return snappy.Decode(nil, buf)
}

func (c *SnappyCompressor) CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error) {
	return snappy.Encode(destinationBuffer, record), nil
}

func (c *SnappyCompressor) CompressBound(n int) int {
	if bound := snappy.MaxEncodedLen(n); bound >= 0 {
		return bound
	}
	// too large for snappy, Encode will panic regardless
	return n
}

func (c *SnappyCompressor) DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error) {
	return snappy.Decode(destinationBuffer, buf)
}
