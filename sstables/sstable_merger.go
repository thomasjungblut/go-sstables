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

func (m SSTableMerger) MergeCompact(iterators []SSTableIteratorI, writer SSTableStreamWriterI,
	reduce func([]byte, [][]byte) ([]byte, []byte)) error {

	pq := NewPriorityQueue(m.comp)
	err := pq.Init(iterators)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	var prevKey []byte
	valBuf := make([][]byte, 0)

	for {
		k, v, err := pq.Next()
		if err == Done {
			break
		}

		if prevKey != nil && m.comp(k, prevKey) != 0 {
			kReduced, vReduced := reduce(prevKey, valBuf)
			err = writer.WriteNext(kReduced, vReduced)
			if err != nil {
				return err
			}
			valBuf = make([][]byte, 0)
		}

		prevKey = k
		valBuf = append(valBuf, v)
	}

	if len(valBuf) > 0 {
		kReduced, vReduced := reduce(prevKey, valBuf)
		err = writer.WriteNext(kReduced, vReduced)
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
