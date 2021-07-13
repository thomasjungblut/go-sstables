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

type MergeCompactionIterator struct {
	ctx     MergeContext
	comp    skiplist.KeyComparator
	reduce  func([]byte, [][]byte, []interface{}) ([]byte, []byte)
	pq      *PriorityQueue
	prevKey []byte
	valBuf  [][]byte
	ctxBuf  []interface{}
}

func (m *MergeCompactionIterator) Next() ([]byte, []byte, error) {
	for {
		k, v, c, err := m.pq.Next()
		if err != nil {
			if err == Done {
				if len(m.valBuf) > 0 {
					kReduced, vReduced := m.reduce(m.prevKey, m.valBuf, m.ctxBuf)
					if kReduced != nil && vReduced != nil {
						// clear the buffer so we don't infinite loop on the last elements
						m.valBuf = m.valBuf[:0]
						return kReduced, vReduced, nil
					}
				}
				return nil, nil, Done
			} else {
				return nil, nil, err
			}
		}

		var toReturnKey, toReturnVal []byte
		//we have to accumulate the whole sequence
		if m.prevKey != nil && m.comp(k, m.prevKey) != 0 {
			kReduced, vReduced := m.reduce(m.prevKey, m.valBuf, m.ctxBuf)
			if kReduced != nil && vReduced != nil {
				toReturnKey = kReduced
				toReturnVal = vReduced
			}
			m.valBuf = make([][]byte, 0)
			m.ctxBuf = make([]interface{}, 0)
		}

		m.prevKey = k
		m.valBuf = append(m.valBuf, v)
		m.ctxBuf = append(m.ctxBuf, c)

		if toReturnKey != nil && toReturnVal != nil {
			return toReturnKey, toReturnVal, nil
		}
	}
}

func (m SSTableMerger) MergeCompactIterator(ctx MergeContext,
	reduce func([]byte, [][]byte, []interface{}) ([]byte, []byte)) (SSTableIteratorI, error) {

	pq := NewPriorityQueue(m.comp)
	err := pq.Init(ctx)
	if err != nil {
		return nil, err
	}

	var prevKey []byte
	valBuf := make([][]byte, 0)
	ctxBuf := make([]interface{}, 0)

	return &MergeCompactionIterator{
		ctx:     ctx,
		comp:    m.comp,
		reduce:  reduce,
		pq:      &pq,
		prevKey: prevKey,
		valBuf:  valBuf,
		ctxBuf:  ctxBuf,
	}, nil

}

func (m SSTableMerger) MergeCompact(ctx MergeContext, writer SSTableStreamWriterI,
	reduce func([]byte, [][]byte, []interface{}) ([]byte, []byte)) error {

	iterator, err := m.MergeCompactIterator(ctx, reduce)
	if err != nil {
		return err
	}

	err = writer.Open()
	if err != nil {
		return err
	}

	for {
		k, v, err := iterator.Next()
		if err != nil {
			if err == Done {
				break
			} else {
				return err
			}
		}
		err = writer.WriteNext(k, v)
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
