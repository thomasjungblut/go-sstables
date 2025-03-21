package capnproto

import (
	"capnproto.org/go/capnp/v3"
	"github.com/thomasjungblut/go-sstables/recordio"
)

type MMapProtoReader struct {
	reader recordio.ReadAtI
}

func (r *MMapProtoReader) Open() error {
	return r.reader.Open()
}

func (r *MMapProtoReader) ReadNextAt(offset uint64) (*capnp.Message, error) {
	bytes, err := r.reader.ReadNextAt(offset)
	if err != nil {
		return nil, err
	}

	return capnp.Unmarshal(bytes)
}

func (r *MMapProtoReader) Close() error {
	return r.reader.Close()
}

func NewMMapCapnProtoReaderWithPath(path string) (ReadAtI, error) {
	r, err := recordio.NewMemoryMappedReaderWithPath(path)
	if err != nil {
		return nil, err
	}

	return &MMapProtoReader{reader: r}, nil
}
