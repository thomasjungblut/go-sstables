package recordio

import (
	"encoding/binary"
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio/compressor"
)

const Version1 uint32 = 0x01
const Version2 uint32 = 0x02
const CurrentVersion = Version2
const MagicNumberSeparator uint32 = 0x130691
const MagicNumberSeparatorLong uint64 = 0x130691

// 4 byte version number, 4 byte compression code = 8 bytes
const FileHeaderSizeBytes = 8
const RecordHeaderSizeBytes = 20

// that's the max buffer sizes to prevent PutUvarint to panic:
// 10 byte magic number, 10 byte uncompressed size, 10 bytes for compressed size = 30 bytes
const RecordHeaderV2MaxSizeBytes = binary.MaxVarintLen64 + binary.MaxVarintLen64 + binary.MaxVarintLen64

const (
	// never reorder, always append
	CompressionTypeNone   = iota
	CompressionTypeGZIP   = iota
	CompressionTypeSnappy = iota
)

type SizeI interface {
	// returns the current size of the file in bytes
	Size() uint64
}

type CloseableI interface {
	// Closes the given file. Errors can happen when:
	// File was already closed before or is not yet open.
	// File could not be closed on the filesystem (eg when flushes fail)
	Close() error
}

type OpenableI interface {
	// Opens the given file for reading or writing. Errors can happen in multiple circumstances:
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
	// Appends a record of bytes, returns the current offset this item was written to
	Write(record []byte) (uint64, error)
	// Appends a record of bytes and forces a disk sync, returns the current offset this item was written to
	WriteSync(record []byte) (uint64, error)
}

type ReaderI interface {
	OpenClosableI
	// Reads the next record, EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext() ([]byte, error)
	// skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() error
}

// this type is thread-safe
type ReadAtI interface {
	OpenClosableI
	// Reads the next record at the given offset, EOF error when it reaches the end signalled by (nil, io.EOF), implementation must be thread-safe
	ReadNextAt(offset uint64) ([]byte, error)
}

func NewCompressorForType(compType int) (compressor.CompressionI, error) {
	switch compType {
	case CompressionTypeNone:
		return nil, nil
	case CompressionTypeSnappy:
		return &compressor.SnappyCompressor{}, nil
	case CompressionTypeGZIP:
		return &compressor.GzipCompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %d", compType)
	}
}
