package compressor

type CompressionI interface {
	// compresses the given record of bytes
	Compress(record []byte) ([]byte, error)
	// decompresses the given byte buffer
	Decompress(buf []byte) ([]byte, error)
}
