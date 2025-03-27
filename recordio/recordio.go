package recordio

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/thomasjungblut/go-sstables/recordio/compressor"
)

const Version1 uint32 = 0x01
const Version2 uint32 = 0x02
const Version3 uint32 = 0x03
const CurrentVersion = Version3
const MagicNumberSeparator uint32 = 0x130691
const MagicNumberSeparatorLong uint64 = 0x130691

var MagicNumberSeparatorLongBytes = []byte{0x91, 0x8d, 0x4c}

// FileHeaderSizeBytes has a 4 byte version number, 4 byte compression code = 8 bytes
const FileHeaderSizeBytes = 8
const RecordHeaderSizeBytesV1V2 = 20

// RecordHeaderV3MaxSizeBytes is the max buffer sizes to prevent PutUvarint to panic:
// 10 byte magic number, 10 byte uncompressed size, 10 bytes for compressed size, 1 byte for nil = 31 bytes
const RecordHeaderV3MaxSizeBytes = binary.MaxVarintLen64 + binary.MaxVarintLen64 + binary.MaxVarintLen64 + 1

// never reorder, always append
const (
	CompressionTypeNone   = iota
	CompressionTypeGZIP   = iota
	CompressionTypeSnappy = iota
	CompressionTypeLzw    = iota
)

// DefaultBufferSize is four mebibyte and can be customized using the option BufferSizeBytes.
const DefaultBufferSize = 1024 * 1024 * 4

type SizeI interface {
	// Size returns the current size of the file in bytes
	Size() uint64
}

type CloseableI interface {
	// Close closes the given file. Errors can happen when:
	// File was already closed before or is not yet open.
	// File could not be closed on the filesystem (eg when flushes fail)
	Close() error
}

type OpenableI interface {
	// Open opens the given file for reading or writing. Errors can happen in multiple circumstances:
	// File or directory doesn't exist or are not accessible.
	// File was already opened or closed before.
	// File is corrupt, header wasn't readable or versions are incompatible.
	Open() error
}

type OpenClosableI interface {
	CloseableI
	OpenableI
}

type WriterI interface {
	OpenClosableI
	SizeI
	// Write appends a record of bytes, returns the current offset this item was written to
	Write(record []byte) (uint64, error)
	// WriteSync appends a record of bytes and forces a disk sync, returns the current offset this item was written to
	WriteSync(record []byte) (uint64, error)
	// Seek will reset the current offset to the given offset. The offset is always
	// denoted as a value from the start (origin) of the file at offset zero.
	// An error will be returned when trying to seek into the file header or beyond the current size of the file.
	Seek(offset uint64) error
}

type ReaderI interface {
	OpenClosableI
	// ReadNext reads the next record, EOF error when it reaches the end signalled by (nil, io.EOF). It can be wrapped however, so always check using errors.Is(err, io.EOF).
	ReadNext() ([]byte, error)
	// SkipNext skips the next record, EOF error when it reaches the end signalled by io.EOF as the error. It can be wrapped however, so always check using errors.Is(err, io.EOF).
	SkipNext() error
}

// ReadAtI implementors must make their implementation thread-safe
type ReadAtI interface {
	OpenClosableI
	SizeI

	// ReadNextAt reads the next record at the given offset, EOF error when it reaches the end signalled by (nil, io.EOF).
	// It can be wrapped however, so always check using errors.Is(err, io.EOF). Implementation must be thread-safe.
	ReadNextAt(offset uint64) ([]byte, error)

	// SeekNext reads the next full record that comes after the provided offset. The main difference to ReadNextAt is
	// that this function seeks to the next record marker, whereas ReadNextAt always needs to be pointed to the start of
	// the record.
	// This function returns any io related error, for example io.EOF, or a wrapped equivalent, when the end is reached.
	SeekNext(offset uint64) (uint64, []byte, error)
}

type ReaderWriterCloserFactory interface {
	CreateNewReader(filePath string, bufSize int) (*os.File, ByteReaderResetCount, error)
	CreateNewWriter(filePath string, bufSize int) (*os.File, WriteSeekerCloserFlusher, error)
}

// NewCompressorForType returns an instance of the desired compressor defined by its identifier.
// An error is returned if the desired compressor is not implemented.
// Only CompressionTypeNone, CompressionTypeSnappy and CompressionTypeGZIP are available currently.
func NewCompressorForType(compType int) (compressor.CompressionI, error) {
	switch compType {
	case CompressionTypeNone:
		return nil, nil
	case CompressionTypeSnappy:
		return &compressor.SnappyCompressor{}, nil
	case CompressionTypeGZIP:
		return &compressor.GzipCompressor{}, nil
	case CompressionTypeLzw:
		return &compressor.LzwCompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %d", compType)
	}
}
