package sstables

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// SuperSSTableReader unifies several sstables under one single reader with the same interface.
// The ordering of the readers matters, it is assumed the older reader comes before the newer (ascending order).
type SuperSSTableReader struct {
	readers []SSTableReaderI
	comp    skiplist.KeyComparator
}

func (s SuperSSTableReader) Contains(key []byte) bool {
	// this can't be implemented using contains because NotFound is the same as false, thus we have to go via Get
	_, err := s.Get(key)
	if err != nil && err == NotFound {
		return false
	}
	return true
}

func (s SuperSSTableReader) Get(key []byte) ([]byte, error) {
	// scanning from back to front to get the latest definitive answer
	for i := len(s.readers) - 1; i >= 0; i-- {
		res, err := s.readers[i].Get(key)
		if err != nil {
			if err == NotFound {
				continue
			}
			return nil, err
		}

		return res, nil
	}

	return nil, NotFound
}

func (s SuperSSTableReader) Scan() (SSTableIteratorI, error) {
	var iterators []SSTableIteratorI
	var context []interface{}

	for i, reader := range s.readers {
		scanner, err := reader.Scan()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, scanner)
		context = append(context, i)
	}

	mergeContext := MergeContext{
		Iterators:       iterators,
		IteratorContext: context,
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(mergeContext, scanReduce)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func (s SuperSSTableReader) ScanStartingAt(key []byte) (SSTableIteratorI, error) {
	var iterators []SSTableIteratorI
	var context []interface{}

	for i, reader := range s.readers {
		scanner, err := reader.ScanStartingAt(key)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, scanner)
		context = append(context, i)
	}

	mergeContext := MergeContext{
		Iterators:       iterators,
		IteratorContext: context,
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(mergeContext, scanReduce)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func (s SuperSSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error) {
	var iterators []SSTableIteratorI
	var context []interface{}

	for i, reader := range s.readers {
		scanner, err := reader.ScanRange(keyLower, keyHigher)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, scanner)
		context = append(context, i)
	}

	mergeContext := MergeContext{
		Iterators:       iterators,
		IteratorContext: context,
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(mergeContext, scanReduce)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func scanReduce(key []byte, values [][]byte, context []interface{}) ([]byte, []byte) {
	// we're taking the value of the "latest" reader by checking the maximum value in the context
	maxCtx := 0
	maxCtxIndex := 0
	for i, x := range context {
		xInt := x.(int)
		if xInt > maxCtx {
			maxCtx = xInt
			maxCtxIndex = i
		}
	}

	return key, values[maxCtxIndex]
}

func (s SuperSSTableReader) Close() error {
	for _, reader := range s.readers {
		err := reader.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s SuperSSTableReader) MetaData() *proto.MetaData {
	// the usefulness is debatable, but we return the aggregation over all sstables.
	// this is problematic because with overlapping key ranges, the number of records are not correct
	sum := &proto.MetaData{
		NumRecords: 0,
		MinKey:     nil,
		MaxKey:     nil,
		DataBytes:  0,
		IndexBytes: 0,
		TotalBytes: 0,
		Version:    0,
	}

	for _, reader := range s.readers {
		m := reader.MetaData()
		sum.NumRecords += m.NumRecords
		sum.DataBytes += m.DataBytes
		sum.IndexBytes += m.IndexBytes
		sum.TotalBytes += m.TotalBytes
		sum.Version = m.Version // assuming all have the same version anyway
		if s.comp(sum.MinKey, m.MinKey) < 0 {
			sum.MinKey = m.MinKey
		}

		if s.comp(sum.MaxKey, m.MaxKey) < 0 {
			sum.MaxKey = m.MaxKey
		}
	}
	return sum
}
