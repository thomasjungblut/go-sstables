package sstables

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

type SSTableIterator struct {
	reader      *SSTableReader
	keyIterator skiplist.IteratorI[[]byte, indexVal]
}

func (it *SSTableIterator) Next() ([]byte, []byte, error) {
	key, iv, err := it.keyIterator.Next()
	if err != nil {
		if errors.Is(err, skiplist.Done) {
			return nil, nil, Done
		} else {
			return nil, nil, err
		}
	}

	valBytes, err := it.reader.getValueAtOffset(iv, it.reader.opts.skipHashCheckOnRead)
	if err != nil {
		return nil, nil, err
	}

	return key, valBytes, nil
}

// V0SSTableFullScanIterator deprecated, since this is for the v0 protobuf based sstables.
// this is an optimized iterator that does a sequential read over the index+data files instead of a
// sequential read on the index with a random access lookup on the data file via mmap
type V0SSTableFullScanIterator struct {
	keyIterator skiplist.IteratorI[[]byte, indexVal]
	dataReader  rProto.ReaderI
}

func (it *V0SSTableFullScanIterator) Next() ([]byte, []byte, error) {
	key, _, err := it.keyIterator.Next()
	if err != nil {
		if errors.Is(err, skiplist.Done) {
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

	return key, value.Value, nil
}

func newV0SStableFullScanIterator(keyIterator skiplist.IteratorI[[]byte, indexVal], dataReader rProto.ReaderI) (SSTableIteratorI, error) {
	return &V0SSTableFullScanIterator{
		keyIterator: keyIterator,
		dataReader:  dataReader,
	}, nil
}

// SSTableFullScanIterator this is an optimized iterator that does a sequential read over the index+data files instead of a
// sequential read on the index with a random access lookup on the data file via mmap
type SSTableFullScanIterator struct {
	keyIterator skiplist.IteratorI[[]byte, indexVal]
	dataReader  recordio.ReaderI
}

func (it *SSTableFullScanIterator) Next() ([]byte, []byte, error) {
	key, _, err := it.keyIterator.Next()
	if err != nil {
		if errors.Is(err, skiplist.Done) {
			return nil, nil, Done
		} else {
			return nil, nil, err
		}
	}

	next, err := it.dataReader.ReadNext()
	return key, next, err
}

func newSStableFullScanIterator(keyIterator skiplist.IteratorI[[]byte, indexVal], dataReader recordio.ReaderI) (SSTableIteratorI, error) {
	return &SSTableFullScanIterator{
		keyIterator: keyIterator,
		dataReader:  dataReader,
	}, nil
}
