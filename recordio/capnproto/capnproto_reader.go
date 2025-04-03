package capnproto

import (
	"capnproto.org/go/capnp/v3"
	"errors"
	"os"

	"github.com/thomasjungblut/go-sstables/recordio"
)

type Reader struct {
	recordio.ReaderI
}

func (r *Reader) ReadNext() (*capnp.Message, error) {
	bytes, err := r.ReaderI.ReadNext()
	if err != nil {
		return nil, err
	}

	return capnp.Unmarshal(bytes)
}

// options

type ReaderOptions struct {
	path         string
	file         *os.File
	bufSizeBytes int
}

type ReaderOption func(*ReaderOptions)

func ReaderPath(p string) ReaderOption {
	return func(args *ReaderOptions) {
		args.path = p
	}
}

func ReaderFile(p *os.File) ReaderOption {
	return func(args *ReaderOptions) {
		args.file = p
	}
}

func ReadBufferSizeBytes(p int) ReaderOption {
	return func(args *ReaderOptions) {
		args.bufSizeBytes = p
	}
}

// NewReader creates a new reader with the given options. Either Path or File must be supplied
func NewReader(readerOptions ...ReaderOption) (ReaderI, error) {
	opts := &ReaderOptions{
		path:         "",
		file:         nil,
		bufSizeBytes: 1024 * 1024 * 4,
	}

	for _, readerOption := range readerOptions {
		readerOption(opts)
	}

	if (opts.file != nil) && (opts.path != "") {
		return nil, errors.New("either os.File or string path must be supplied, never both")
	}

	if opts.file == nil {
		if opts.path == "" {
			return nil, errors.New("path was not supplied")
		}
	}
	reader, err := recordio.NewFileReader(
		recordio.ReaderPath(opts.path),
		recordio.ReaderFile(opts.file),
		recordio.ReaderBufferSizeBytes(opts.bufSizeBytes))
	if err != nil {
		return nil, err
	}

	return &Reader{
		reader,
	}, nil

}
