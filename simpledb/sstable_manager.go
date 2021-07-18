package simpledb

import (
	"github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"sort"
	"sync"
)

type SSTableManager struct {
	cmp               skiplist.KeyComparator
	lock              *sync.RWMutex
	allSSTableReaders []sstables.SSTableReaderI
	currentReader     sstables.SSTableReaderI

	// after how many sstables to trigger a compaction
	compactionThreshold    int
	compactionChannel      chan compactionAction
	compactionMaxSizeBytes uint64
}

func (s *SSTableManager) reflectCompactionResult(m *proto.CompactionMetadata) error {
	s.lock.Lock()
	return func() error {
		defer s.lock.Unlock()

		for _, path := range m.SstablePaths {
			i := indexOfReader(s.allSSTableReaders, path)
			if i >= 0 {
				err := s.allSSTableReaders[i].Close()
				if err != nil {
					return err
				}
				// this is actually a "neuralgic" point in terms of recovery, we know that the SSTable backed by newReader
				// contains the whole data of all the SSTables we're about to remove. So it's safe to delete them here.
				err = os.RemoveAll(path)
				if err != nil {
					return err
				}
			}
		}

		// this is another important step in the recovery process, we need to ensure the ordering is preserved in case of crashes and
		// thus replace the very first written SSTable in the path set. This creates a couple of "holes" in the numbering schema of
		// the SSTables, but we guarantee that the compaction is in the right place.
		err := os.Rename(m.WritePath, m.ReplacementPath)
		if err != nil {
			return err
		}

		replacedReader, err := sstables.NewSSTableReader(
			sstables.ReadBasePath(m.ReplacementPath),
			sstables.ReadWithKeyComparator(s.cmp),
		)

		i := indexOfReader(s.allSSTableReaders, m.ReplacementPath)
		if i >= 0 {
			s.allSSTableReaders[i] = replacedReader
			// remove the remainder of the deleted paths
			for _, p := range m.SstablePaths {
				if p != m.ReplacementPath {
					readerIndex := indexOfReader(s.allSSTableReaders, p)
					s.allSSTableReaders = removeReaderAt(s.allSSTableReaders, readerIndex)
				}
			}
		}
		// if we haven't found that path i == -1, then we are currently in a recovery and the
		// recovery logic later will read all sstables again in the right order

		s.currentReader = sstables.NewSuperSSTableReader(s.allSSTableReaders, s.cmp)

		return nil
	}()
}

func (s *SSTableManager) addReaderAndMaybeTriggerCompaction(newReader sstables.SSTableReaderI) {
	s.lock.Lock()
	func() {
		defer s.lock.Unlock()

		allSSTableReaders := append(s.allSSTableReaders, newReader)
		s.currentReader = sstables.NewSuperSSTableReader(allSSTableReaders, s.cmp)
		s.allSSTableReaders = allSSTableReaders
	}()

	// when we're over the sstable count threshold, we'll trigger a compaction.
	// note that this CAN block here and that in turn can block a flush, which in turn blocks a Put.
	// this is entirely intended as a way to deal with back pressure.
	// in order for us to later on update the sstable manager,
	// we'll keep this outside of the lock to not block ongoing reads
	compactionAction := s.candidateTablesForCompaction()
	if len(s.compactionChannel) == 0 && len(compactionAction.pathsToCompact) >= s.compactionThreshold {
		s.compactionChannel <- compactionAction
	}
}

func (s *SSTableManager) currentSSTable() sstables.SSTableReaderI {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.currentReader
}

func (s *SSTableManager) candidateTablesForCompaction() compactionAction {
	s.lock.RLock()
	defer s.lock.RUnlock()

	numRecords := uint64(0)
	var paths []string
	var readers []sstables.SSTableReaderI
	for i := 0; i < len(s.allSSTableReaders); i++ {
		reader := s.allSSTableReaders[i]
		// avoid the EmptySStableReader (or empty files) and only include small enough SSTables
		if reader.MetaData().NumRecords > 0 && reader.MetaData().TotalBytes < s.compactionMaxSizeBytes {
			paths = append(paths, reader.BasePath())
			readers = append(readers, reader)
			numRecords += reader.MetaData().NumRecords
		}
	}

	sort.Strings(paths)

	return compactionAction{
		pathsToCompact: paths,
		readers:        readers,
		totalRecords:   numRecords,
	}
}

func NewSSTableManager(
	cmp skiplist.KeyComparator,
	compactionThreshold int,
	compactionMaxSizeBytes uint64,
	compactionChannel chan compactionAction) *SSTableManager {
	return &SSTableManager{
		cmp:                    cmp,
		lock:                   &sync.RWMutex{},
		currentReader:          sstables.EmptySStableReader{},
		compactionThreshold:    compactionThreshold,
		compactionMaxSizeBytes: compactionMaxSizeBytes,
		compactionChannel:      compactionChannel,
	}
}

// CC-4.0 helper, type changed from https://stackoverflow.com/a/37335777/540873
func removeReaderAt(slice []sstables.SSTableReaderI, i int) []sstables.SSTableReaderI {
	return append(slice[:i], slice[i+1:]...)
}

func indexOfReader(slice []sstables.SSTableReaderI, basePath string) int {
	for i := 0; i < len(slice); i++ {
		if slice[i].BasePath() == basePath {
			return i
		}
	}
	return -1
}
