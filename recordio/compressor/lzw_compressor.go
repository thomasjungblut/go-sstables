package compressor

import (
	"bytes"
	"compress/lzw"
	"sync"
)

type LzwCompressor struct {
}

// lzw writers and readers carry large tables, thus they are pooled. The compressor is shared by concurrent
// readers (e.g. the mmap reader), which is why they can't live on the LzwCompressor itself.
type lzwEncoder struct {
	w   lzw.Writer
	out sliceWriter
}

type lzwDecoder struct {
	r     lzw.Reader
	src   bytes.Reader
	probe [1]byte
}

var lzwEncoderPool = sync.Pool{New: func() any { return &lzwEncoder{} }}
var lzwDecoderPool = sync.Pool{New: func() any { return &lzwDecoder{} }}

func (l LzwCompressor) Compress(record []byte) ([]byte, error) {
	return l.CompressWithBuf(record, make([]byte, 0, l.CompressBound(len(record))))
}

func (l LzwCompressor) Decompress(buf []byte) ([]byte, error) {
	return l.DecompressWithBuf(buf, nil)
}

func (l LzwCompressor) CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error) {
	enc := lzwEncoderPool.Get().(*lzwEncoder)
	defer func() {
		enc.out.buf = nil
		lzwEncoderPool.Put(enc)
	}()

	// we have to set the length of the buffer (keeping capacity) to make sure lzw doesn't append
	enc.out.buf = destinationBuffer[:0]
	enc.w.Reset(&enc.out, lzw.LSB, 8)
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

// CompressBound assumes the worst case of one 12-bit code per input byte, plus the clear and eof codes.
func (l LzwCompressor) CompressBound(n int) int {
	return n + (n+1)/2 + 8
}

func (l LzwCompressor) DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error) {
	dec := lzwDecoderPool.Get().(*lzwDecoder)
	defer func() {
		dec.src.Reset(nil)
		lzwDecoderPool.Put(dec)
	}()

	dec.src.Reset(buf)
	dec.r.Reset(&dec.src, lzw.LSB, 8)
	return readAllInto(&dec.r, destinationBuffer, dec.probe[:])
}
