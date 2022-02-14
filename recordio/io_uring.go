package recordio

import (
	"github.com/godzie44/go-uring/uring"
	"os"
)

type IOUringFactory struct {
	numRingEntries uint32
	opts           []uring.SetupOption
}

func (f *IOUringFactory) CreateNewReader(filePath string, bufSize int) (*os.File, ByteReaderResetCount, error) {
	//TODO implement me
	panic("implement me")
}

func (f *IOUringFactory) CreateNewWriter(filePath string, _ int) (*os.File, WriteCloserFlusher, error) {
	writer, file, err := NewAsyncWriter(filePath, f.numRingEntries, f.opts...)
	if err != nil {
		return nil, nil, err
	}

	return file, writer, nil
}

func NewIOUringFactory(numRingEntries uint32, opts ...uring.SetupOption) *IOUringFactory {
	return &IOUringFactory{
		numRingEntries: numRingEntries,
		opts:           opts,
	}
}

// IsIOUringAvailable tests whether io_uring is supported by the kernel.
// It will return (true, nil) if that's the case, if it's not available it will be (false, nil).
// Any other error will be indicated by the error (either true/false).
func IsIOUringAvailable() (available bool, err error) {
	ring, err := uring.New(1)
	defer func() {
		err = ring.Close()
	}()

	return err == nil, err
}
