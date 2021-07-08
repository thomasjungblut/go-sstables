package sstables

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
)

type SSTableMerger struct {
	comp skiplist.KeyComparator
}

type MergeContext struct {
	Iterators       []SSTableIteratorI
	IteratorContext []interface{}
}

func (m SSTableMerger) Merge(ctx MergeContext, writer SSTableStreamWriterI) error {
	pq := NewPriorityQueue(m.comp)
	err := pq.Init(ctx)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	for {
		k, v, _, err := pq.Next()
		if err != nil {
			if err == Done {
				break
			} else {
				return err
			}
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

func (m SSTableMerger) MergeCompact(ctx MergeContext, writer SSTableStreamWriterI,
	reduce func([]byte, [][]byte, []interface{}) ([]byte, []byte)) error {

	pq := NewPriorityQueue(m.comp)
	err := pq.Init(ctx)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	var prevKey []byte
	valBuf := make([][]byte, 0)
	ctxBuf := make([]interface{}, 0)

	for {
		k, v, c, err := pq.Next()
		if err != nil {
			if err == Done {
				break
			} else {
				return err
			}
		}

		if prevKey != nil && m.comp(k, prevKey) != 0 {
			kReduced, vReduced := reduce(prevKey, valBuf, ctxBuf)
			if kReduced != nil && vReduced != nil {
				err = writer.WriteNext(kReduced, vReduced)
				if err != nil {
					return err
				}
			}
			valBuf = make([][]byte, 0)
			ctxBuf = make([]interface{}, 0)
		}

		prevKey = k
		valBuf = append(valBuf, v)
		ctxBuf = append(ctxBuf, c)
	}

	if len(valBuf) > 0 {
		kReduced, vReduced := reduce(prevKey, valBuf, ctxBuf)
		if kReduced != nil && vReduced != nil {
			err = writer.WriteNext(kReduced, vReduced)
			if err != nil {
				return err
			}
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
