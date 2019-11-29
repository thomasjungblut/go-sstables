package sstables

import "github.com/thomasjungblut/go-sstables/skiplist"

type SSTableMerger struct {
	comp skiplist.KeyComparator
}

func (m SSTableMerger) Merge(iterators []SSTableIteratorI, writer SSTableStreamWriterI) error {
	pq := NewPriorityQueue(m.comp)
	err := pq.Init(iterators)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	for {
		k, v, err := pq.Next()
		if err == Done {
			break
		}

		err = writer.WriteNext(k, v)
		if err != nil {
			return err
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

func NewSSTableMerger(comp skiplist.KeyComparator) SSTableMerger {
	return SSTableMerger{comp}
}
