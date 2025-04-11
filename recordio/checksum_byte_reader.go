package recordio

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

// checksumByteReader generates a checksum on the bytes read so far
type checksumByteReader struct {
	io.ByteReader

	bytes []byte
	crc   hash.Hash32
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
	h.crc.Reset()
	h.idx = 0
}

func (h *checksumByteReader) Count() int {
	return h.idx
}

func (h *checksumByteReader) Checksum() (uint64, error) {
	_, err := h.crc.Write(h.bytes[:h.idx])
	if err != nil {
		return 0, err
	}
	return uint64(h.crc.Sum32()), nil
}

func newChecksumByteReader(r io.ByteReader, cachedBytes []byte) *checksumByteReader {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	return &checksumByteReader{
		ByteReader: r,
		bytes:      cachedBytes,
		crc:        crc,
		idx:        0,
	}
}
