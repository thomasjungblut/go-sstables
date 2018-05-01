package recordio

import (
	"os"
	"github.com/gogo/protobuf/proto"
)

type ProtoReader struct {
	reader *FileReader
}

func (r *ProtoReader) Open() error {
	return r.reader.Open()
}

func (r *ProtoReader) ReadNext(record proto.Message) (proto.Message, error) {
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

func (r *ProtoReader) SkipNext() error {
	return r.reader.SkipNext()
}

func (r *ProtoReader) Close() error {
	return r.reader.Close()
}

func NewProtoReaderWithPath(path string) (*ProtoReader, error) {
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

func NewProtoReaderWithFile(file *os.File) (*ProtoReader, error) {
	reader, err := NewFileReaderWithFile(file)
	if err != nil {
		return nil, err
	}

	return &ProtoReader{
		reader: reader,
	}, nil
}
