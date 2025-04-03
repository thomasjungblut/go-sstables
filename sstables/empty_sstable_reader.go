package sstables

import (
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// these types are mostly used for testing or default behaviour of an empty sstable

type EmptySStableReader struct{}

func (EmptySStableReader) Contains(_ []byte) (bool, error) {
	return false, nil
}

func (EmptySStableReader) Get(_ []byte) ([]byte, error) {
	return nil, NotFound
}

func (EmptySStableReader) Scan() (SSTableIteratorI, error) {
	return EmptySSTableIterator{}, nil
}

func (EmptySStableReader) ScanStartingAt(_ []byte) (SSTableIteratorI, error) {
	return EmptySSTableIterator{}, nil
}

func (EmptySStableReader) ScanRange(_ []byte, _ []byte) (SSTableIteratorI, error) {
	return EmptySSTableIterator{}, nil
}

func (EmptySStableReader) Close() error {
	return nil
}

func (EmptySStableReader) MetaData() *proto.MetaData {
	return &proto.MetaData{
		NumRecords: 0,
		MinKey:     nil,
		MaxKey:     nil,
	}
}

func (EmptySStableReader) BasePath() string {
	return ""
}

type EmptySSTableIterator struct{}

func (EmptySSTableIterator) Next() ([]byte, []byte, error) {
	return nil, nil, Done
}
