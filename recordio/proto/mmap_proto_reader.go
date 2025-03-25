package proto

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	"google.golang.org/protobuf/proto"
)

type MMapProtoReader struct {
	recordio.ReadAtI
}

func (r *MMapProtoReader) ReadNextAt(record proto.Message, offset uint64) (proto.Message, error) {
	bytes, err := r.ReadAtI.ReadNextAt(offset)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(bytes, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func NewMMapProtoReaderWithPath(path string) (ReadAtI, error) {
	r, err := recordio.NewMemoryMappedReaderWithPath(path)
	if err != nil {
		return nil, err
	}

	return &MMapProtoReader{r}, nil
}
