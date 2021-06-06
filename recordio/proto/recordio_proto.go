package proto

import "google.golang.org/protobuf/proto"
import "github.com/thomasjungblut/go-sstables/recordio"

type ReaderI interface {
	recordio.OpenClosableI
	// Reads the next record into the passed message record, EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext(record proto.Message) (proto.Message, error)
	// skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() error
}

// this type is thread-safe
type ReadAtI interface {
	recordio.OpenClosableI
	// Reads the next record at the given offset into the passed message record, EOF error when it reaches the end signalled by (nil, io.EOF), implementation must be thread-safe
	ReadNextAt(record proto.Message, offset uint64) (proto.Message, error)
}

type WriterI interface {
	recordio.OpenClosableI
	recordio.SizeI
	// Appends a record, returns the current offset this item was written to
	Write(record proto.Message) (uint64, error)
	// Appends a record and forces a disk sync, returns the current offset this item was written to
	WriteSync(record proto.Message) (uint64, error)
}
