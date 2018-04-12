package recordio

type WriterI interface {
	// Opens this writer
	Open() error
	// Appends a record of bytes, returns the current offset this item was written to
	Write(record []byte) (uint64, error)
	// Closes this writer
	Close() error
}

type ReaderI interface {
	// Reads the next record, EOF error when it reaches the end
	ReadNext() ([]byte, error)
	// skips the next record, EOF error when it reaches the end
	SkipNext() (error)
	// Closes this reader
	Close() error
}
