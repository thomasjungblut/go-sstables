package simpledb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"golang.org/x/exp/slices"
)

type SSTableManager struct {
	cmp               skiplist.Comparator[[]byte]
	databaseLock      *sync.RWMutex
	basePath          string
	managerLock       *sync.RWMutex
	allSSTableReaders []sstables.SSTableReaderI
	currentReader     sstables.SSTableReaderI
}

func (s *SSTableManager) reflectCompactionResult(m *proto.CompactionMetadata) error {
	// careful about the lock ordering, we always need to acquire the full DB lock first to not corrupt reads
	s.databaseLock.Lock()
	s.managerLock.Lock()
	return func() error {
		defer s.databaseLock.Unlock()
		defer s.managerLock.Unlock()

		for _, p := range m.SstablePaths {
			i := indexOfReader(s.allSSTableReaders, p)
			if i >= 0 {
				err := s.allSSTableReaders[i].Close()
				if err != nil {
					return err
				}
				// this is actually a "neuralgic" point in terms of recovery, we know that the SSTable backed by newReader
				// contains the whole data of all the SSTables we're about to remove. So it's safe to delete them here.
				err = os.RemoveAll(filepath.Join(s.basePath, p))
				if err != nil {
					return err
				}
			}
		}

		// this is another important step in the recovery process, we need to ensure the ordering is preserved in case of crashes and
		// thus replace the very first written SSTable in the path set. This creates a couple of "holes" in the numbering schema of
		// the SSTables, but we guarantee that the compaction is in the right place.
		err := os.Rename(filepath.Join(s.basePath, m.WritePath), filepath.Join(s.basePath, m.ReplacementPath))
		if err != nil {
			return err
		}

		replacedReader, err := sstables.NewSSTableReader(
			sstables.ReadBasePath(filepath.Join(s.basePath, m.ReplacementPath)),
			sstables.ReadWithKeyComparator(s.cmp),
		)
		if err != nil {
			return err
		}

		i := indexOfReader(s.allSSTableReaders, m.ReplacementPath)
		if i < 0 {
			return fmt.Errorf("couldn't find replacement sstable in current readers. Path: %v", m.ReplacementPath)
		}

		s.allSSTableReaders[i] = replacedReader
		// remove the remainder of the deleted paths
		for _, p := range m.SstablePaths {
			if p != m.ReplacementPath {
				readerIndex := indexOfReader(s.allSSTableReaders, p)
				if readerIndex < 0 {
					return fmt.Errorf("couldn't find sstable in current readers. Path: %v", p)
				}
				s.allSSTableReaders = removeReaderAt(s.allSSTableReaders, readerIndex)
			}
		}

		s.currentReader = sstables.NewSuperSSTableReader(s.allSSTableReaders, s.cmp)

		return nil
	}()
}

func (s *SSTableManager) clearReaders() {
	s.managerLock.Lock()
	func() {
		defer s.managerLock.Unlock()

		s.currentReader = sstables.EmptySStableReader{}
		s.allSSTableReaders = []sstables.SSTableReaderI{}
	}()
}

func (s *SSTableManager) addReader(newReader sstables.SSTableReaderI) {
	s.managerLock.Lock()
	func() {
		defer s.managerLock.Unlock()

		allSSTableReaders := append(s.allSSTableReaders, newReader)
		s.currentReader = sstables.NewSuperSSTableReader(allSSTableReaders, s.cmp)
		s.allSSTableReaders = allSSTableReaders
	}()
}

func (s *SSTableManager) currentSSTable() sstables.SSTableReaderI {
	s.managerLock.RLock()
	defer s.managerLock.RUnlock()

	return s.currentReader
}

func (s *SSTableManager) candidateTablesForCompaction(compactionMaxSizeBytes uint64) compactionAction {
	s.managerLock.RLock()
	defer s.managerLock.RUnlock()

	numRecords := uint64(0)
	canRemoveTombstone := false
	var paths []string
	for i := 0; i < len(s.allSSTableReaders); i++ {
		reader := s.allSSTableReaders[i]
		// avoid the EmptySStableReader (or empty files) and only include small enough SSTables
		if reader.MetaData().TotalBytes < compactionMaxSizeBytes {
			paths = append(paths, reader.BasePath())
			numRecords += reader.MetaData().NumRecords
			if numRecords == 0 {
				canRemoveTombstone = true
			}
		}
	}

	sort.Strings(paths)

	return compactionAction{
		pathsToCompact:     paths,
		totalRecords:       numRecords,
		canRemoveTombstone: canRemoveTombstone,
	}
}

func NewSSTableManager(cmp skiplist.Comparator[[]byte], dbLock *sync.RWMutex, basePath string) *SSTableManager {
	return &SSTableManager{
		cmp:           cmp,
		managerLock:   &sync.RWMutex{},
		databaseLock:  dbLock,
		basePath:      basePath,
		currentReader: sstables.EmptySStableReader{},
	}
}

// CC-4.0 helper, type changed from https://stackoverflow.com/a/37335777/540873
func removeReaderAt(slice []sstables.SSTableReaderI, i int) []sstables.SSTableReaderI {
	return append(slice[:i], slice[i+1:]...)
}

func indexOfReader(slice []sstables.SSTableReaderI, p string) int {
	return slices.IndexFunc(slice, func(i sstables.SSTableReaderI) bool {
		return filepath.Base(i.BasePath()) == p
	})
}
