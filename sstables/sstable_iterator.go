package sstables

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
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

// deprecated, since this is for the v0 protobuf based sstables.
// this is an optimized iterator that does a sequential read over the index+data files instead of a
// sequential read on the index with a random access lookup on the data file via mmap
type V0SSTableFullScanIterator struct {
	keyIterator skiplist.SkipListIteratorI
	dataReader  *rProto.Reader
}

func (it *V0SSTableFullScanIterator) Next() ([]byte, []byte, error) {
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

func newV0SStableFullScanIterator(keyIterator skiplist.SkipListIteratorI, dataReader *rProto.Reader) (SSTableIteratorI, error) {
	return &V0SSTableFullScanIterator{
		keyIterator: keyIterator,
		dataReader:  dataReader,
	}, nil
}

// this is an optimized iterator that does a sequential read over the index+data files instead of a
// sequential read on the index with a random access lookup on the data file via mmap
type SSTableFullScanIterator struct {
	keyIterator skiplist.SkipListIteratorI
	dataReader  recordio.ReaderI
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

	next, err := it.dataReader.ReadNext()
	return key.([]byte), next, err
}

func newSStableFullScanIterator(keyIterator skiplist.SkipListIteratorI, dataReader recordio.ReaderI) (SSTableIteratorI, error) {
	return &SSTableFullScanIterator{
		keyIterator: keyIterator,
		dataReader:  dataReader,
	}, nil
}
