package compressor

import (
	"bytes"
	"compress/gzip"
	"sync"
)

type GzipCompressor struct {
}

// gzip writers and readers are expensive to allocate, thus they are pooled. The compressor is shared by concurrent
// readers (e.g. the mmap reader), which is why they can't live on the GzipCompressor itself.
type gzipEncoder struct {
	w   *gzip.Writer
	out sliceWriter
}

type gzipDecoder struct {
	r     *gzip.Reader
	src   bytes.Reader
	probe [1]byte
}

var gzipEncoderPool = sync.Pool{New: func() any {
	enc := &gzipEncoder{}
	// can only fail with an invalid level
	enc.w, _ = gzip.NewWriterLevel(&enc.out, gzip.DefaultCompression)
	return enc
}}

var gzipDecoderPool = sync.Pool{New: func() any {
	return &gzipDecoder{}
}}

func (c *GzipCompressor) Compress(record []byte) ([]byte, error) {
	return c.CompressWithBuf(record, make([]byte, 0, c.CompressBound(len(record))))
}

func (c *GzipCompressor) CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error) {
	enc := gzipEncoderPool.Get().(*gzipEncoder)
	defer func() {
		enc.out.buf = nil
		gzipEncoderPool.Put(enc)
	}()

	// we have to set the length of the buffer (keeping capacity) to make sure gzip doesn't append
	enc.out.buf = destinationBuffer[:0]
	enc.w.Reset(&enc.out)
	_, err := enc.w.Write(record)
	if err != nil {
		return nil, err
	}
	err = enc.w.Close()
	if err != nil {
		return nil, err
	}
	return enc.out.buf, nil
}

// CompressBound is zlib's compressBound plus the gzip header and trailer.
func (c *GzipCompressor) CompressBound(n int) int {
	return n + (n >> 12) + (n >> 14) + (n >> 25) + 13 + 18
}

func (c *GzipCompressor) Decompress(buf []byte) ([]byte, error) {
	return c.DecompressWithBuf(buf, nil)
}

func (c *GzipCompressor) DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error) {
	dec := gzipDecoderPool.Get().(*gzipDecoder)
	defer func() {
		dec.src.Reset(nil)
		gzipDecoderPool.Put(dec)
	}()

	dec.src.Reset(buf)
	if dec.r == nil {
		r, err := gzip.NewReader(&dec.src)
		if err != nil {
			return nil, err
		}
		dec.r = r
	} else if err := dec.r.Reset(&dec.src); err != nil {
		return nil, err
	}

	return readAllInto(dec.r, destinationBuffer, dec.probe[:])
}
