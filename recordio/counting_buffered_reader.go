package recordio

import (
	"bufio"
	"io"
)

type Reset interface {
	Reset(r io.Reader)
}

type ReaderResetComposite interface {
	io.ByteReader
	io.Reader
	Reset
}

type CountingReaderResetComposite interface {
	ReaderResetComposite
	Count() uint64
}

type CountingBufferedReader struct {
	r     ReaderResetComposite
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

func NewCountingByteReader(reader *bufio.Reader) CountingReaderResetComposite {
	return &CountingBufferedReader{r: reader, count: 0}
}
