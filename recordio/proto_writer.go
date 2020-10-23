package recordio

import (
	"github.com/gogo/protobuf/proto"
	"os"
)

type ProtoWriter struct {
	writer *FileWriter
}

func (w *ProtoWriter) Open() error {
	return w.writer.Open()
}

func (w *ProtoWriter) Write(record proto.Message) (uint64, error) {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	return w.writer.Write(bytes)
}

func (w *ProtoWriter) WriteSync(record proto.Message) (uint64, error) {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	return w.writer.WriteSync(bytes)
}

func (w *ProtoWriter) Close() error {
	return w.writer.Close()
}

func (w *ProtoWriter) Size() uint64 {
	return w.writer.Size()
}

// TODO(thomas): use an option pattern instead
func NewProtoWriterWithPath(path string) (*ProtoWriter, error) {
	return NewCompressedProtoWriterWithPath(path, CompressionTypeNone)
}

func NewProtoWriterWithFile(file *os.File) (*ProtoWriter, error) {
	return NewCompressedProtoWriterWithFile(file, CompressionTypeNone)
}

func NewCompressedProtoWriterWithPath(path string, compType int) (*ProtoWriter, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	r, err := NewCompressedProtoWriterWithFile(f, compType)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewCompressedProtoWriterWithFile(file *os.File, compType int) (*ProtoWriter, error) {
	writer, err := NewCompressedFileWriterWithFile(file, compType)
	if err != nil {
		return nil, err
	}
	return &ProtoWriter{
		writer: writer,
	}, nil
}
