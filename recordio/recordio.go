package recordio

const Version uint32 = 0x01
const MagicNumberSeparator uint32 = 0x130691

const HeaderSizeBytes = 20
const RecordSizeBufferBytes = 4096

const (
	// never reorder, always append
	CompressionTypeNone   = iota
	CompressionTypeGZIP   = iota
	CompressionTypeSnappy = iota
)

type WriterI interface {
	// Opens this writer
	Open() error
	// Appends a record of bytes, returns the current offset this item was written to
	Write(record []byte) (uint64, error)
	// Appends a record of bytes and forces a disk sync, returns the current offset this item was written to
	WriteSync(record []byte) (uint64, error)
	// Closes this writer
	Close() error
}

type ReaderI interface {
	// Opens this reader, checks version/compression compatibility
	Open() error
	// Reads the next record, EOF error when it reaches the end signalled by (nil, io.EOF)
	ReadNext() ([]byte, error)
	// skips the next record, EOF error when it reaches the end signalled by io.EOF as the error
	SkipNext() (error)
	// Closes this reader
	Close() error
}
