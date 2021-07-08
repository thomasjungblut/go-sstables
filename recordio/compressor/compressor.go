package compressor

type CompressionI interface {
	// compresses the given record of bytes
	Compress(record []byte) ([]byte, error)
	// decompresses the given byte buffer
	Decompress(buf []byte) ([]byte, error)

	// compresses the given record of bytes and a buffer where to compress into.
	// if the buffer doesn't fit, it will resize it (truncate/enlarging copy).
	// Thus it's important to use the returned buffer value.
	CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error)
	// decompresses the given byte buffer and a buffer where to decompress into.
	// if the buffer doesn't fit, it will resize it (truncate/enlarging copy).
	// Thus it's important to use the returned buffer value.
	DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error)
}
