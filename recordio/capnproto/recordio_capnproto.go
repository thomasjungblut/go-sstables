package capnproto

import (
	"capnproto.org/go/capnp/v3"
	"github.com/thomasjungblut/go-sstables/recordio"
)

type ReaderI interface {
	recordio.OpenClosableI
	// ReadNext reads the next record and returns the unmarshalled message, io.EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext() (*capnp.Message, error)
	// SkipNext skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() error
}

type WriterI interface {
	recordio.OpenClosableI
	recordio.SizeI
	// Write appends a record, returns the current offset this item was written to
	Write(record *capnp.Message) (uint64, error)
	// WriteSync appends a record and forces a disk sync, returns the current offset this item was written to
	WriteSync(record *capnp.Message) (uint64, error)
}
