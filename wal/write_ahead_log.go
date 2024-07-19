package wal

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/recordio"
)

const DefaultMaxWalSize uint64 = 128 * 1024 * 1024 // 128mb
type WriteAheadLogReplayI interface {
	// Replay the whole WAL from start, calling the given process function
	// for each record in guaranteed order.
	Replay(process func(record []byte) error) error
}
type WriteAheadLogAppendI interface {
	recordio.CloseableI
	// Append a given record and does NOT execute fsync to guarantee the persistence of the record.
	Append(record []byte) error
	// AppendSync a given record and execute fsync to guarantee the persistence of the record.
	// Has considerably less throughput than Append.
	AppendSync(record []byte) error
	// Rotate - The WAL usually auto-rotates after a certain size - this method allows to force this rotation.
	// This can be useful in scenarios where you want to flush a memstore and rotate the WAL at the same time.
	// Therefore, this returns the path of the previous wal file that was closed through this operation.
	Rotate() (string, error)
}
type WriteAheadLogCleanI interface {
	// Clean Removes all WAL files and the directory it is contained in
	Clean() error
}
type WriteAheadLogCompactI interface {
	// Compact should compact the WAL, but isn't properly implemented just yet
	Compact() error
}
type WriteAheadLogI interface {
	WriteAheadLogAppendI
	WriteAheadLogReplayI
	WriteAheadLogCleanI
}
type WriteAheadLog struct {
	WriteAheadLogAppendI
	WriteAheadLogReplayI
	WriteAheadLogCleanI
}

// NewWriteAheadLog creates a new WAL by supplying options, for example using a base path: wal.NewWriteAheadLogOptions(wal.BasePath("some_directory"))
func NewWriteAheadLog(opts *Options) (WriteAheadLogI, error) {
	appender, err := NewAppender(opts)
	if err != nil {
		return nil, err
	}
	replayer, err := NewReplayer(opts)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{
		appender,
		replayer,
		NewCleaner(opts),
	}, nil
}

// NewWriteAheadLogOptions handles WAL configurations, the minimal required option is the base path: wal.NewWriteAheadLogOptions(wal.BasePath("some_directory"))
func NewWriteAheadLogOptions(walOptions ...Option) (*Options, error) {
	opts := &Options{
		basePath:       "",
		maxWalFileSize: DefaultMaxWalSize,
		writerFactory: func(path string) (recordio.WriterI, error) {
			return recordio.NewFileWriter(recordio.Path(path))
		},
		readerFactory: func(path string) (recordio.ReaderI, error) {
			return recordio.NewFileReaderWithPath(path)
		},
	}
	for _, walOption := range walOptions {
		walOption(opts)
	}
	if opts.basePath == "" {
		return nil, errors.New("basePath was not supplied")
	}
	return opts, nil
}

// options
type Options struct {
	// TODO(thomas): this should be ideally in a writer-only option
	writerFactory func(path string) (recordio.WriterI, error)
	// TODO(thomas): this should be ideally in a reader-only option
	readerFactory  func(path string) (recordio.ReaderI, error)
	basePath       string
	maxWalFileSize uint64
}
type Option func(*Options)

func BasePath(p string) Option {
	return func(args *Options) {
		args.basePath = p
	}
}
func MaximumWalFileSizeBytes(p uint64) Option {
	return func(args *Options) {
		args.maxWalFileSize = p
	}
}
func WriterFactory(writerFactory func(path string) (recordio.WriterI, error)) Option {
	return func(args *Options) {
		args.writerFactory = writerFactory
	}
}
func ReaderFactory(readerFactory func(path string) (recordio.ReaderI, error)) Option {
	return func(args *Options) {
		args.readerFactory = readerFactory
	}
}
