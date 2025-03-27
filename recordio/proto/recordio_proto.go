package proto

import "google.golang.org/protobuf/proto"
import "github.com/thomasjungblut/go-sstables/recordio"

type ReaderI interface {
	recordio.OpenClosableI
	// ReadNext reads the next record into the passed message record, EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext(record proto.Message) (proto.Message, error)
	// SkipNext skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() error
}

// ReadAtI is used to randomly read protobuf through byte offsets. This type is thread-safe
type ReadAtI interface {
	recordio.OpenClosableI
	recordio.SizeI

	// ReadNextAt reads the next record at the given offset into the passed message record, EOF error when it reaches the end signalled by (nil, io.EOF), implementation must be thread-safe
	ReadNextAt(record proto.Message, offset uint64) (proto.Message, error)

	// SeekNext reads the next full record that comes after the provided offset. The main difference to ReadNextAt is
	// that this function seeks to the next record marker, whereas ReadNextAt always needs to be pointed to the start of
	// the record. This function returns any io related error, for example io.EOF, or a wrapped equivalent, when the end is reached.
	SeekNext(record proto.Message, offset uint64) (uint64, proto.Message, error)
}

type WriterI interface {
	recordio.OpenClosableI
	recordio.SizeI
	// Write appends a record, returns the current offset this item was written to
	Write(record proto.Message) (uint64, error)
	// WriteSync appends a record and forces a disk sync, returns the current offset this item was written to
	WriteSync(record proto.Message) (uint64, error)
}
