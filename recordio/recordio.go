package recordio

import (
	"github.com/thomasjungblut/go-sstables/recordio/compressor"
	"fmt"
	"github.com/gogo/protobuf/proto"
)

const Version uint32 = 0x01
const MagicNumberSeparator uint32 = 0x130691

const HeaderSizeBytes = 20

const (
	// never reorder, always append
	CompressionTypeNone   = iota
	CompressionTypeGZIP   = iota
	CompressionTypeSnappy = iota
)

type OpenClosableI interface {
	Open() error
	Close() error
}

type WriterI interface {
	OpenClosableI
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
	SkipNext() (error)
}

type ProtoReaderI interface {
	OpenClosableI
	// Reads the next record into the passed message record, EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext(record proto.Message) (proto.Message, error)
	// skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() (error)
}

type ProtoWriterI interface {
	OpenClosableI
	// Appends a record, returns the current offset this item was written to
	Write(record proto.Message) (uint64, error)
	// Appends a record and forces a disk sync, returns the current offset this item was written to
	WriteSync(record proto.Message) (uint64, error)
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
