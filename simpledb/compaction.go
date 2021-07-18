package simpledb

import (
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

func backgroundCompaction(db *DB) {
	defer func() {
		db.doneCompactionChannel <- true
	}()
	err := func(db *DB) error {
		if !db.enableCompactions {
			return nil
		}
	outer:
		for compactionAction := range db.compactionChannel {
			paths := compactionAction.pathsToCompact
			readers := compactionAction.readers
			numRecords := compactionAction.totalRecords

			// first thing we need to check is whether there are any tables that we already compacted - if so ignore
			for _, p := range paths {
				_, err := os.Stat(p)
				if err != nil {
					if os.IsNotExist(err) {
						continue outer
					}
					return err
				}
			}

			start := time.Now()
			writeFolder, err := ioutil.TempDir(db.basePath, SSTableCompactionPathPrefix)
			if err != nil {
				return err
			}

			log.Printf("Starting compaction in %v with %v\n", writeFolder, strings.Join(paths, ","))

			writer, err := sstables.NewSSTableStreamWriter(
				sstables.WriteBasePath(writeFolder),
				sstables.WithKeyComparator(skiplist.BytesComparator),
				sstables.BloomExpectedNumberOfElements(numRecords))
			if err != nil {
				return err
			}

			var iterators []sstables.SSTableIteratorI
			var iteratorContext []interface{}
			for i := 0; i < len(readers); i++ {
				scanner, err := readers[i].Scan()
				if err != nil {
					return err
				}

				iterators = append(iterators, scanner)
				iteratorContext = append(iteratorContext, i)
			}

			ctx := sstables.MergeContext{
				Iterators:       iterators,
				IteratorContext: iteratorContext,
			}

			// TODO(thomas): this includes tombstones, do we really need to?
			err = sstables.NewSSTableMerger(db.cmp).MergeCompact(ctx, writer, sstables.ScanReduceLatestWins)
			if err != nil {
				return err
			}

			compactionMetadata := proto.CompactionMetadata{
				WritePath:       writeFolder,
				ReplacementPath: compactionAction.pathsToCompact[0],
				SstablePaths:    compactionAction.pathsToCompact,
			}

			// at this point the compaction is finished, we save the metadata that this was successful for potential recoveries
			metaWriter, err := rProto.NewWriter(rProto.Path(path.Join(writeFolder, CompactionFinishedSuccessfulFileName)))
			if err != nil {
				return err
			}
			err = metaWriter.Open()
			if err != nil {
				return err
			}

			_, err = metaWriter.Write(&compactionMetadata)
			if err != nil {
				return err
			}

			err = metaWriter.Close()
			if err != nil {
				return err
			}

			log.Printf("done compacting %d sstables in %v. Path: [%s]\n", len(paths), time.Since(start), writeFolder)

			err = db.sstableManager.reflectCompactionResult(&compactionMetadata)
			if err != nil {
				return err
			}
		}
		return nil
	}(db)

	if err != nil {
		log.Panicf("error while compacting, error was %v", err)
	}
}
