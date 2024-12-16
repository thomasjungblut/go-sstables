package proto

import (
	"errors"
	"os"

	"github.com/thomasjungblut/go-sstables/recordio"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	reader recordio.ReaderI
}

func (r *Reader) Open() error {
	return r.reader.Open()
}

func (r *Reader) ReadNext(record proto.Message) (proto.Message, error) {
	bytes, err := r.reader.ReadNext()
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(bytes, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (r *Reader) SkipNext() error {
	return r.reader.SkipNext()
}

func (r *Reader) Close() error {
	return r.reader.Close()
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
		reader: reader,
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
