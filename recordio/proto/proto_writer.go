package proto

import (
	"errors"
	"github.com/ncw/directio"
	"github.com/thomasjungblut/go-sstables/recordio"
	"google.golang.org/protobuf/proto"
	"os"
)

type Writer struct {
	writer recordio.WriterI
}

func (w *Writer) Open() error {
	return w.writer.Open()
}

func (w *Writer) Write(record proto.Message) (uint64, error) {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	return w.writer.Write(bytes)
}

func (w *Writer) WriteSync(record proto.Message) (uint64, error) {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	return w.writer.WriteSync(bytes)
}

func (w *Writer) Close() error {
	return w.writer.Close()
}

func (w *Writer) Size() uint64 {
	return w.writer.Size()
}

// options

type WriterOptions struct {
	path            string
	file            *os.File
	compressionType int
	bufSizeBytes    int
	useDirectIO     bool
}

type WriterOption func(*WriterOptions)

func Path(p string) WriterOption {
	return func(args *WriterOptions) {
		args.path = p
	}
}

func File(p *os.File) WriterOption {
	return func(args *WriterOptions) {
		args.file = p
	}
}

func CompressionType(p int) WriterOption {
	return func(args *WriterOptions) {
		args.compressionType = p
	}
}

func WriteBufferSizeBytes(p int) WriterOption {
	return func(args *WriterOptions) {
		args.bufSizeBytes = p
	}
}

func DirectIO() WriterOption {
	return func(args *WriterOptions) {
		args.useDirectIO = true
	}
}

// create a new writer with the given options. Either Path or File must be supplied, compression is optional and
// turned off by default.
func NewWriter(writerOptions ...WriterOption) (WriterI, error) {
	opts := &WriterOptions{
		path:            "",
		file:            nil,
		compressionType: recordio.CompressionTypeNone,
		bufSizeBytes:    1024 * 1024 * 4,
		useDirectIO:     false,
	}

	for _, writeOption := range writerOptions {
		writeOption(opts)
	}

	if (opts.file != nil) && (opts.path != "") {
		return nil, errors.New("either os.File or string path must be supplied, never both")
	}

	if opts.file == nil {
		if opts.path == "" {
			return nil, errors.New("path was not supplied")
		}
		if opts.useDirectIO {
			f, err := directio.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return nil, err
			}
			opts.file = f
		} else {
			f, err := os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return nil, err
			}
			opts.file = f
		}
	}

	writer, err := recordio.NewFileWriter(
		recordio.File(opts.file),
		recordio.CompressionType(opts.compressionType),
		recordio.BufferSizeBytes(opts.bufSizeBytes))
	if err != nil {
		return nil, err
	}

	return &Writer{writer: writer}, nil
}
