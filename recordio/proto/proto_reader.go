package proto

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	"google.golang.org/protobuf/proto"
	"os"
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

func NewProtoReaderWithPath(path string) (ReaderI, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewProtoReaderWithFile(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewProtoReaderWithFile(file *os.File) (ReaderI, error) {
	reader, err := recordio.NewFileReaderWithFile(file)
	if err != nil {
		return nil, err
	}

	return &Reader{
		reader: reader,
	}, nil
}
