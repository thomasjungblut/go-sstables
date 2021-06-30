package sstables

import (
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"

	"path"
)

type SSTableIterator struct {
	reader      *SSTableReader
	keyIterator skiplist.SkipListIteratorI
}

func (it *SSTableIterator) Next() ([]byte, []byte, error) {
	key, valueOffset, err := it.keyIterator.Next()
	if err != nil {
		if err == skiplist.Done {
			return nil, nil, Done
		} else {
			return nil, nil, err
		}
	}

	valBytes, err := it.reader.getValueAtOffset(valueOffset.(uint64))
	if err != nil {
		return nil, nil, err
	}

	return key.([]byte), valBytes, nil
}

// this is an optimized iterator that does a sequential read over the index+data files instead of a
// sequential read on the index with a random access lookup on the data file via mmap
type SSTableFullScanIterator struct {
	SSTableIterator
	dataReader *rProto.Reader
}

func (it *SSTableFullScanIterator) Next() ([]byte, []byte, error) {
	key, _, err := it.keyIterator.Next()
	if err != nil {
		if err == skiplist.Done {
			return nil, nil, Done
		} else {
			return nil, nil, err
		}
	}

	value := &proto.DataEntry{}
	_, err = it.dataReader.ReadNext(value)
	if err != nil {
		return nil, nil, err
	}

	return key.([]byte), value.Value, nil
}

func newSStableFullScanIterator(reader *SSTableReader) (*SSTableFullScanIterator, error) {
	// TODO(thomas): super hack, this is being closed by the caller sstable reader once it closes
	dataReader, err := rProto.NewProtoReaderWithPath(path.Join(reader.opts.basePath, DataFileName))
	if err != nil {
		return nil, err
	}

	err = dataReader.Open()
	if err != nil {
		return nil, err
	}

	it, err := reader.index.Iterator()
	if err != nil {
		return nil, err
	}
	return &SSTableFullScanIterator{
		SSTableIterator: SSTableIterator{
			reader:      reader,
			keyIterator: it,
		},
		dataReader: dataReader,
	}, nil
}
