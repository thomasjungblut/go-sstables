package sstables

import (
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/pq"
	"github.com/thomasjungblut/go-sstables/skiplist"
)

type SSTableMergeIteratorContext struct {
	ctx      int
	iterator SSTableIteratorI
}

func (s SSTableMergeIteratorContext) Next() ([]byte, []byte, error) {
	k, v, err := s.iterator.Next()
	if errors.Is(err, Done) {
		return nil, nil, pq.Done
	}
	return k, v, nil
}

func (s SSTableMergeIteratorContext) Context() int {
	return s.ctx
}

func NewMergeIteratorContext(context int, iterator SSTableIteratorI) SSTableMergeIteratorContext {
	return SSTableMergeIteratorContext{
		ctx:      context,
		iterator: iterator,
	}
}

type SSTableMerger struct {
	comp skiplist.Comparator[[]byte]
}

func (m SSTableMerger) Merge(iterators []SSTableMergeIteratorContext, writer SSTableStreamWriterI) error {
	var iteratorWithContext []pq.IteratorWithContext[[]byte, []byte, int]
	for _, iterator := range iterators {
		iteratorWithContext = append(iteratorWithContext, iterator)
	}
	pqq, err := pq.NewPriorityQueue[[]byte, []byte, int](m.comp, iteratorWithContext)
	if err != nil {
		return fmt.Errorf("merge error while initializing the heap: %w", err)
	}

	err = writer.Open()
	if err != nil {
		return fmt.Errorf("merge error while opening writer: %w", err)
	}

	for {
		k, v, _, err := pqq.Next()
		if err != nil {
			if errors.Is(err, pq.Done) {
				break
			} else {
				return fmt.Errorf("merge error during heap next: %w", err)
			}
		}

		err = writer.WriteNext(k, v)
		if err != nil {
			return fmt.Errorf("merge error while writing next record: %w", err)
		}
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("merge error while closing the writer: %w", err)
	}

	return nil
}

type MergeCompactionIterator struct {
	comp    skiplist.Comparator[[]byte]
	reduce  func([]byte, [][]byte, []int) ([]byte, []byte)
	pq      pq.PriorityQueueI[[]byte, []byte, int]
	prevKey []byte
	valBuf  [][]byte
	ctxBuf  []int
}

func (m *MergeCompactionIterator) Next() ([]byte, []byte, error) {
	for {
		k, v, c, err := m.pq.Next()
		if err != nil {
			if errors.Is(err, pq.Done) {
				if len(m.valBuf) > 0 {
					kReduced, vReduced := m.reduce(m.prevKey, m.valBuf, m.ctxBuf)
					if kReduced != nil && vReduced != nil {
						// clear the buffer, so we don't infinite loop on the last elements
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
		if m.prevKey != nil && m.comp.Compare(k, m.prevKey) != 0 {
			kReduced, vReduced := m.reduce(m.prevKey, m.valBuf, m.ctxBuf)
			if kReduced != nil && vReduced != nil {
				toReturnKey = kReduced
				toReturnVal = vReduced
			}
			m.valBuf = make([][]byte, 0)
			m.ctxBuf = make([]int, 0)
		}

		m.prevKey = k
		m.valBuf = append(m.valBuf, v)
		m.ctxBuf = append(m.ctxBuf, c)

		if toReturnKey != nil && toReturnVal != nil {
			return toReturnKey, toReturnVal, nil
		}
	}
}

func (m SSTableMerger) MergeCompactIterator(iterators []SSTableMergeIteratorContext,
	reduce func([]byte, [][]byte, []int) ([]byte, []byte)) (SSTableIteratorI, error) {
	var iteratorWithContext []pq.IteratorWithContext[[]byte, []byte, int]
	for _, iterator := range iterators {
		iteratorWithContext = append(iteratorWithContext, iterator)
	}
	pqq, err := pq.NewPriorityQueue[[]byte, []byte, int](m.comp, iteratorWithContext)
	if err != nil {
		return nil, fmt.Errorf("merge compact error while initializing the heap: %w", err)
	}

	var prevKey []byte
	valBuf := make([][]byte, 0)
	ctxBuf := make([]int, 0)

	return &MergeCompactionIterator{
		comp:    m.comp,
		reduce:  reduce,
		pq:      pqq,
		prevKey: prevKey,
		valBuf:  valBuf,
		ctxBuf:  ctxBuf,
	}, nil

}

func (m SSTableMerger) MergeCompact(iterators []SSTableMergeIteratorContext, writer SSTableStreamWriterI,
	reduce func([]byte, [][]byte, []int) ([]byte, []byte)) error {
	iterator, err := m.MergeCompactIterator(iterators, reduce)
	if err != nil {
		return fmt.Errorf("merge compact error while initializing the iterator: %w", err)
	}

	err = writer.Open()
	if err != nil {
		return fmt.Errorf("merge compact error while opening the writer: %w", err)
	}

	for {
		k, v, err := iterator.Next()
		if err != nil {
			if errors.Is(err, Done) {
				break
			} else {
				return fmt.Errorf("merge compact error while iterating: %w", err)
			}
		}
		err = writer.WriteNext(k, v)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("merge compact error while closing the writer: %w", err)
	}

	return nil
}

func NewSSTableMerger(comp skiplist.Comparator[[]byte]) SSTableMerger {
	return SSTableMerger{comp}
}
