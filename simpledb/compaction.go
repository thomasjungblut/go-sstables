package simpledb

import (
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func backgroundCompaction(db *DB) {
	defer func() {
		db.doneCompactionChannel <- true
	}()

	if !db.enableCompactions {
		return
	}

	err := func(db *DB) error {
		for {
			select {
			case <-db.compactionTickerStopChannel:
				return nil
			case _ = <-db.compactionTicker.C:
				metadata, err := executeCompaction(db)
				if err != nil {
					return err
				}

				// nothing that was compacted, wait for the next tick
				if metadata == nil {
					continue
				}

				err = db.sstableManager.reflectCompactionResult(metadata)
				if err != nil {
					return err
				}
			}

		}
	}(db)

	if err != nil {
		log.Panicf("error while compacting, error was %v", err)
	}
}

func executeCompaction(db *DB) (*proto.CompactionMetadata, error) {
	compactionAction := db.sstableManager.candidateTablesForCompaction(db.compactedMaxSizeBytes)
	paths := compactionAction.pathsToCompact
	numRecords := compactionAction.totalRecords
	if len(paths) <= db.compactionThreshold {
		return nil, nil
	}

	// make sure we're always compacting with the right order in mind
	sort.Strings(paths)

	start := time.Now()
	writeFolder, err := os.MkdirTemp(db.basePath, SSTableCompactionPathPrefix)
	if err != nil {
		return nil, err
	}

	log.Printf("starting compaction of %d files in %v with %v\n", len(paths), writeFolder, strings.Join(paths, ","))

	writer, err := sstables.NewSSTableStreamWriter(
		sstables.WriteBasePath(writeFolder),
		sstables.WithKeyComparator(skiplist.BytesComparator{}),
		sstables.BloomExpectedNumberOfElements(numRecords))
	if err != nil {
		return nil, err
	}

	var readers []sstables.SSTableReaderI
	var iterators []sstables.SSTableMergeIteratorContext
	for i := 0; i < len(paths); i++ {
		reader, err := sstables.NewSSTableReader(
			sstables.ReadBasePath(paths[i]),
			sstables.ReadWithKeyComparator(db.cmp),
		)
		if err != nil {
			return nil, err
		}

		scanner, err := reader.Scan()
		if err != nil {
			return nil, err
		}

		readers = append(readers, reader)
		iterators = append(iterators, sstables.NewMergeIteratorContext(i, scanner))
	}

	// TODO(thomas): this includes tombstones, do we really need to?
	err = sstables.NewSSTableMerger(db.cmp).MergeCompact(iterators, writer, sstables.ScanReduceLatestWins)
	if err != nil {
		return nil, err
	}

	for _, reader := range readers {
		err := reader.Close()
		if err != nil {
			return nil, err
		}
	}

	// in order to be portable, we are taking only relative paths from the db base path
	// later in reconstruction they are "rebased" over the database base path
	for i := 0; i < len(paths); i++ {
		paths[i] = filepath.Base(paths[i])
	}

	compactionMetadata := &proto.CompactionMetadata{
		WritePath:       filepath.Base(writeFolder),
		ReplacementPath: paths[0],
		SstablePaths:    paths,
	}

	// at this point the compaction is finished, we save the metadata that this was successful for potential recoveries
	err = saveCompactionMetadata(writeFolder, compactionMetadata)
	if err != nil {
		return nil, err
	}

	log.Printf("done compacting %d sstables in %v. Path: [%s]\n", len(paths), time.Since(start), writeFolder)

	return compactionMetadata, nil
}

func saveCompactionMetadata(writeFolder string, compactionMetadata *proto.CompactionMetadata) error {
	metaWriter, err := rProto.NewWriter(rProto.Path(filepath.Join(writeFolder, CompactionFinishedSuccessfulFileName)))
	if err != nil {
		return err
	}
	err = metaWriter.Open()
	if err != nil {
		return err
	}

	_, err = metaWriter.Write(compactionMetadata)
	if err != nil {
		return err
	}

	err = metaWriter.Close()
	if err != nil {
		return err
	}
	return nil
}
