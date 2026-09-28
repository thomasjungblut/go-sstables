package recordio

import (
	"fmt"
	"hash/crc32"
	"io"
)

// checksumByteReader generates a checksum on the bytes read so far
type checksumByteReader struct {
	io.ByteReader

	bytes []byte
	idx   int
}

func (h *checksumByteReader) ReadByte() (byte, error) {
	b, err := h.ByteReader.ReadByte()
	if err != nil {
		return b, err
	}

	if h.idx >= len(h.bytes) {
		return b, fmt.Errorf("checksum byte reader out of range: %d, only have %d", h.idx, len(h.bytes))
	}

	h.bytes[h.idx] = b
	h.idx++

	return b, err
}

func (h *checksumByteReader) Reset() {
	h.idx = 0
}

func (h *checksumByteReader) Count() int {
	return h.idx
}

func (h *checksumByteReader) Checksum() (uint64, error) {
	return uint64(crc32.Checksum(h.bytes[:h.idx], castagnoliTable)), nil
}

func newChecksumByteReader(r io.ByteReader, cachedBytes []byte) *checksumByteReader {
	return &checksumByteReader{
		ByteReader: r,
		bytes:      cachedBytes,
		idx:        0,
	}
}
