package simpledb

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"sync"
)

type SSTableManager struct {
	cmp               skiplist.KeyComparator
	lock              sync.RWMutex
	allSSTableReaders []sstables.SSTableReaderI
	currentReader     sstables.SSTableReaderI
}

// TODO(thomas): after compaction we need to re-arrange our readers a bit and delete old ones
func (s *SSTableManager) mergeSwapCloseDelete() {

}

func (s *SSTableManager) addNewReader(newReader sstables.SSTableReaderI) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.allSSTableReaders = append(s.allSSTableReaders, newReader)
	s.currentReader = sstables.NewSuperSSTableReader(s.allSSTableReaders, s.cmp)
}

func (s *SSTableManager) currentSSTable() sstables.SSTableReaderI {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.currentReader
}

func NewSSTableManager(cmp skiplist.KeyComparator) *SSTableManager {
	return &SSTableManager{
		cmp:           cmp,
		lock:          sync.RWMutex{},
		currentReader: sstables.EmptySStableReader{},
	}
}
