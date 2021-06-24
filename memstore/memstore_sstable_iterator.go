package memstore

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
)

type SkipListSStableIterator struct {
	iterator skiplist.SkipListIteratorI
}

func (s SkipListSStableIterator) Next() ([]byte, []byte, error) {
	key, val, err := s.iterator.Next()
	if err != nil {
		if err == skiplist.Done {
			return nil, nil, sstables.Done
		} else {
			return nil, nil, err
		}
	}
	valStruct := val.(ValueStruct)
	return key.([]byte), *valStruct.value, nil
}
