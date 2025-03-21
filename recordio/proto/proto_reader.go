package proto

import (
	"errors"
	"google.golang.org/protobuf/encoding/protowire"
	"os"

	"github.com/thomasjungblut/go-sstables/recordio"
	gproto "google.golang.org/protobuf/proto"
)

type Reader struct {
	recordio.ReaderI
	opts *gproto.UnmarshalOptions
}

func (r *Reader) ReadNext(record gproto.Message) (gproto.Message, error) {
	bytes, err := r.ReaderI.ReadNext()
	if err != nil {
		return nil, err
	}

	err = r.opts.Unmarshal(bytes, record)
	if err != nil {
		return nil, err
	}

	return record, nil
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

// create a new reader with the given options. Either Path or File must be supplied
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
		ReaderI: reader,
		opts: &gproto.UnmarshalOptions{
			RecursionLimit: protowire.DefaultRecursionLimit,
		},
	}, nil

}

// Deprecated: use the NewProtoReader with options.
func NewProtoReaderWithPath(path string) (ReaderI, error) {
	return NewReader(ReaderPath(path))
}

// Deprecated: use the NewProtoReader with options.
func NewProtoReaderWithFile(file *os.File) (ReaderI, error) {
	return NewReader(ReaderFile(file))
}
