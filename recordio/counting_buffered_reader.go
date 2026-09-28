package recordio

import (
	"io"
)

type Reset interface {
	Reset(r io.Reader)
}

// BufferPeeker allows to decode directly from the read buffer instead of going byte by byte.
type BufferPeeker interface {
	// PeekBuffered returns up to n buffered bytes without advancing the reader, see Reader.PeekBuffered.
	PeekBuffered(n int) []byte
	// DiscardBuffered skips n bytes previously returned by PeekBuffered.
	DiscardBuffered(n int)
}

type ByteReaderReset interface {
	io.ByteReader
	io.Reader
	Reset
	BufferPeeker
	Size() int
}

type ByteReaderResetCount interface {
	ByteReaderReset
	Count() uint64
}

type CountingBufferedReader struct {
	r     ByteReaderReset
	count uint64
}

// ReadByte reads and returns a single byte. If no byte is available, returns an error.
func (c *CountingBufferedReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.count = c.count + 1
	}
	return b, err
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// At EOF, the count will be zero and err will be io.EOF.
func (c *CountingBufferedReader) Read(p []byte) (n int, err error) {
	read, err := c.r.Read(p)
	if err == nil {
		c.count = c.count + uint64(read)
	}
	return read, err
}

// Reset discards any buffered data, resets all state, and switches
// the buffered reader to read from r.
func (c *CountingBufferedReader) Reset(r io.Reader) {
	c.r.Reset(r)
}

func (c *CountingBufferedReader) Count() uint64 {
	return c.count
}

func (c *CountingBufferedReader) Size() int {
	return c.r.Size()
}

// PeekBuffered returns up to n buffered bytes without advancing the reader or the count.
func (c *CountingBufferedReader) PeekBuffered(n int) []byte {
	return c.r.PeekBuffered(n)
}

// DiscardBuffered skips n bytes previously returned by PeekBuffered.
func (c *CountingBufferedReader) DiscardBuffered(n int) {
	c.r.DiscardBuffered(n)
	c.count += uint64(n)
}

func NewCountingByteReader(reader ByteReaderReset) ByteReaderResetCount {
	return &CountingBufferedReader{r: reader, count: 0}
}
