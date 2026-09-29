package compressor

import (
	"errors"
	"io"
)

type CompressionI interface {
	// Compress compresses the given record of bytes
	Compress(record []byte) ([]byte, error)
	// Decompress decompresses the given byte buffer
	Decompress(buf []byte) ([]byte, error)

	// CompressWithBuf compresses the given record of bytes and a buffer where to compress into.
	// If the buffer doesn't fit, it will resize it (truncate/enlarging copy).
	// Thus, it's important to use the returned buffer value.
	CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error)
	// CompressBound returns an upper bound of the compressed size of a record with n bytes. It's used to size the
	// destination buffer of CompressWithBuf, so that compression doesn't need to allocate.
	CompressBound(n int) int
	// DecompressWithBuf decompresses the given byte buffer and a buffer where to decompress into.
	// If the buffer doesn't fit, it will resize it (truncate/enlarging copy).
	// Thus, it's important to use the returned buffer value.
	DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error)
}

// readAllInto reads r until EOF into the capacity of buf and only grows it when the output doesn't fit. Unlike
// io.ReadAll and bytes.Buffer.ReadFrom, a buffer that fits the output exactly is not reallocated, the check whether
// more data follows reads into the given single byte probe instead.
func readAllInto(r io.Reader, buf []byte, probe []byte) ([]byte, error) {
	if cap(buf) == 0 {
		buf = make([]byte, 0, 512)
	}

	buf = buf[:cap(buf)]
	n, err := io.ReadFull(r, buf)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}

	// the output filled the whole buffer, reading until EOF is necessary anyway, e.g. gzip verifies its checksum then
	_, err = io.ReadFull(r, probe[:1])
	if errors.Is(err, io.EOF) {
		return buf, nil
	}
	if err != nil {
		return nil, err
	}

	rest, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return append(append(buf, probe[0]), rest...), nil
}
