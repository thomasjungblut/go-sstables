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
