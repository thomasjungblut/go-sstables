package simpledb

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// this reads all existing sstables and adds them (if any), along with the generation number
func (db *DB) reconstructSSTables() error {
	var tablePaths []string

	err := filepath.Walk(db.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && strings.HasPrefix(info.Name(), SSTablePrefix) {
			tablePaths = append(tablePaths, path)
			suffix := info.Name()[len(SSTablePrefix)+1:]
			i, err := strconv.Atoi(suffix)
			if err != nil {
				return err
			}
			if int64(i) > db.currentGeneration {
				db.currentGeneration = int64(i)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(tablePaths) > 0 {
		log.Printf("found %d existing sstables, starting recovery...\n", len(tablePaths))
		// do not rely on the order of the FS, we do an additional sort to make sure we start reading from 0000 to 9999
		sort.Strings(tablePaths)
		for _, p := range tablePaths {
			reader, err := sstables.NewSSTableReader(
				sstables.ReadBasePath(p),
				sstables.ReadWithKeyComparator(db.cmp),
			)
			if err != nil {
				return err
			}

			db.sstableManager.addReaderAndMaybeTriggerCompaction(reader)
		}
	}

	return nil
}

func (db *DB) replayAndSetupWriteAheadLog(err error) error {
	walBasePath := path.Join(db.basePath, WriteAheadFolder)
	err = os.MkdirAll(walBasePath, 0700)
	if err != nil {
		return err
	}

	walOpts, err := wal.NewWriteAheadLogOptions(wal.BasePath(walBasePath),
		// we do manual rotation in lockstep with the memstore flushes, thus just set this super high
		wal.MaximumWalFileSizeBytes(db.memstoreMaxSize*10),
		wal.WriterFactory(func(path string) (recordio.WriterI, error) {
			return recordio.NewFileWriter(recordio.Path(path), recordio.CompressionType(recordio.CompressionTypeSnappy))
		}),
		wal.ReaderFactory(func(path string) (recordio.ReaderI, error) {
			return recordio.NewFileReaderWithPath(path)
		}),
	)

	replayer, err := wal.NewReplayer(walOpts)
	if err != nil {
		return err
	}

	start := time.Now()
	numRecords := 0
	err = replayer.Replay(func(record []byte) error {
		mutation := &dbproto.WalMutation{}
		err := proto.Unmarshal(record, mutation)
		if err != nil {
			return err
		}

		switch u := mutation.Mutation.(type) {
		case *dbproto.WalMutation_Addition:
			err := db.memStore.Upsert([]byte(u.Addition.Key), []byte(u.Addition.Value))
			if err != nil {
				return err
			}
			break
		case *dbproto.WalMutation_DeleteTombStone:
			err := db.memStore.Tombstone([]byte(u.DeleteTombStone.Key))
			if err != nil {
				return err
			}
			break
		}

		numRecords++

		return nil
	})

	if err != nil {
		return err
	}

	if numRecords == 0 {
		// there is nothing to reply, we cut the remainder of the recovery and create a new log from here
		writeAheadLog, err := wal.NewWriteAheadLog(walOpts)
		if err != nil {
			return err
		}
		db.wal = writeAheadLog
		return nil
	} else {
		elapsedDuration := time.Since(start)
		log.Printf("done replaying WAL in %v with %d records\n", elapsedDuration, numRecords)
	}

	// we trigger a memstore flush here (even if inefficient) to be able to start from an empty WAL directory
	// we rotate and flush the wal one last time to get a clean slate
	err = executeFlush(db, memStoreFlushAction{
		memStore: swapMemstore(db),
	})

	err = os.RemoveAll(walBasePath)
	if err != nil {
		return err
	}

	err = os.MkdirAll(walBasePath, 0700)
	if err != nil {
		return err
	}

	log.Printf("done with recovery starting with fresh WAL directory in %v\n", walBasePath)
	writeAheadLog, err := wal.NewWriteAheadLog(walOpts)
	if err != nil {
		return err
	}
	db.wal = writeAheadLog
	return nil
}