package sstables

import (
	"errors"
	"strings"

	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

// SuperSSTableReader unifies several sstables under one single reader with the same interface.
// The ordering of the readers matters, it is assumed the older reader comes before the newer (ascending order).
type SuperSSTableReader struct {
	readers []SSTableReaderI
	comp    skiplist.Comparator[[]byte]
}

func (s SuperSSTableReader) Contains(key []byte) bool {
	// scanning from back to front to get the latest definitive answer
	for i := len(s.readers) - 1; i >= 0; i-- {
		// first check if key exist to return fast
		keyExist := s.readers[i].Contains(key)
		if !keyExist {
			continue
		}
		// we have to check if the value is not tombstoned
		// maybe had to be implemented in an IsTombstoned in sstableReader
		res, _ := s.readers[i].Get(key)
		return res != nil
	}

	return false
}

func (s SuperSSTableReader) Get(key []byte) ([]byte, error) {
	// scanning from back to front to get the latest definitive answer
	for i := len(s.readers) - 1; i >= 0; i-- {
		res, err := s.readers[i].Get(key)
		if err != nil {
			if errors.Is(err, NotFound) {
				continue
			}
			return nil, err
		}

		return res, nil
	}

	return nil, NotFound
}

func (s SuperSSTableReader) Scan() (SSTableIteratorI, error) {
	var iterators []SSTableMergeIteratorContext
	for i, reader := range s.readers {
		scanner, err := reader.Scan()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, NewMergeIteratorContext(i, scanner))
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(iterators, ScanReduceLatestWins)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func (s SuperSSTableReader) ScanStartingAt(key []byte) (SSTableIteratorI, error) {
	var iterators []SSTableMergeIteratorContext

	for i, reader := range s.readers {
		scanner, err := reader.ScanStartingAt(key)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, NewMergeIteratorContext(i, scanner))
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(iterators, ScanReduceLatestWins)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func (s SuperSSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error) {
	var iterators []SSTableMergeIteratorContext

	for i, reader := range s.readers {
		scanner, err := reader.ScanRange(keyLower, keyHigher)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, NewMergeIteratorContext(i, scanner))
	}

	iterator, err := NewSSTableMerger(s.comp).MergeCompactIterator(iterators, ScanReduceLatestWins)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

// ScanReduceLatestWins is a simple version of a merge where the latest value always wins. Latest is determined
// by looping the context and finding the biggest value denoted by integers (assuming context is actually []int).
func ScanReduceLatestWins(key []byte, values [][]byte, context []int) ([]byte, []byte) {
	// we're taking the value of the "latest" reader by checking the maximum value in the context
	maxCtx := 0
	maxCtxIndex := 0
	for i, x := range context {
		if x > maxCtx {
			maxCtx = x
			maxCtxIndex = i
		}
	}
	val := values[maxCtxIndex]
	if len(val) == 0 {
		return nil, nil
	}
	return key, val
}

func (s SuperSSTableReader) Close() (err error) {
	for _, reader := range s.readers {
		err = errors.Join(err, reader.Close())
	}
	return
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
		if s.comp.Compare(sum.MinKey, m.MinKey) < 0 {
			sum.MinKey = m.MinKey
		}

		if s.comp.Compare(sum.MaxKey, m.MaxKey) < 0 {
			sum.MaxKey = m.MaxKey
		}
	}
	return sum
}

func (s SuperSSTableReader) BasePath() string {
	// the usefulness here is also debatable, but we return a joined string of all sub files
	var paths []string
	for _, reader := range s.readers {
		paths = append(paths, reader.BasePath())
	}
	return strings.Join(paths, ",")
}

func NewSuperSSTableReader(readers []SSTableReaderI, comp skiplist.Comparator[[]byte]) *SuperSSTableReader {
	return &SuperSSTableReader{readers: readers, comp: comp}
}
