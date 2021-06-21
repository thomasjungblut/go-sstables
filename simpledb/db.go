package simpledb

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/recordio"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type DB struct {
	basePath          string
	memStore          memstore.MemStoreI
	rwLock            *sync.RWMutex
	wal               wal.WriteAheadLogI
	mainSSTableReader sstables.SSTableReaderI
}

func (db *DB) Open() error {
	// TODO if files already exist we need to reconstruct from the WAL
	return nil
}

func (db *DB) Close() error {
	// this should close all files properly, flush the memstore and merge all sstables
	return nil
}

func (db *DB) Get(key string) (string, error) {
	keyBytes := []byte(key)

	db.rwLock.RLock()
	defer db.rwLock.RUnlock()

	// we have to read the sstable first and then augment it with
	// any changes that were reflected in the memstore
	var sstableNotFound bool
	ssTableVal, err := db.mainSSTableReader.Get(keyBytes)
	if err != nil {
		if err == sstables.NotFound {
			sstableNotFound = true
		} else {
			return "", err
		}
	}

	memStoreVal, err := db.memStore.Get(keyBytes)
	if err != nil {
		if err == memstore.KeyNotFound {
			if sstableNotFound {
				return "", NotFound
			} else {
				return string(ssTableVal), nil
			}
		} else if err == memstore.KeyTombstoned {
			// regardless of what we found on the sstable:
			// if the memstore says it's tombstoned it's considered deleted
			return "", NotFound
		} else {
			return "", err
		}
	}

	// memstore always wins if there is a value available
	return string(memStoreVal), nil
}

func (db *DB) Put(key, value string) error {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	walBytes, err := proto.Marshal(&dbproto.WalMutation{
		Mutation: &dbproto.WalMutation_Addition{
			Addition: &dbproto.UpsertMutation{
				Key:   key,
				Value: value,
			},
		},
	})
	if err != nil {
		return err
	}

	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	err = db.wal.Append(walBytes)
	if err != nil {
		return err
	}

	err = db.memStore.Upsert(keyBytes, valBytes)
	if err != nil {
		return err
	}

	// TODO below should be done in a goroutine to not affect write latency
	if db.memStore.EstimatedSizeInBytes() > MemStoreMaxSizeBytes {
		err = db.memStore.Flush(sstables.WriteBasePath(path.Join(db.basePath, "sstable_next")))
		if err != nil {
			return err
		}

		// TODO here we should already merge both sstables for ease of use
		// have a channel with capacity 1 block on this operation if the existing didn't finish yet

		db.memStore = memstore.NewMemStore()
	}

	return nil
}

func (db *DB) Delete(key string) error {
	bytes, err := proto.Marshal(&dbproto.WalMutation{
		Mutation: &dbproto.WalMutation_DeleteTombStone{
			DeleteTombStone: &dbproto.DeleteTombstoneMutation{
				Key: key,
			},
		},
	})
	if err != nil {
		return err
	}

	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	err = db.wal.Append(bytes)
	if err != nil {
		return err
	}

	err = db.memStore.Delete([]byte(key))
	if err != nil {
		if err == memstore.KeyNotFound {
			return NotFound
		}
		return err
	}
	return nil
}

// NewSimpleDB creates a new db that requires a directory that exist, it can be empty in case of existing databases.
// The error in case it doesn't exist can be checked using normal os package functions like os.IsNotExist(err)
func NewSimpleDB(basePath string) (*DB, error) {
	// validate the basePath exist
	_, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	walBasePath := path.Join(basePath, WriteAheadFolder)
	err = os.MkdirAll(walBasePath, os.ModeDir)
	if err != nil {
		return nil, err
	}

	walOpts, err := wal.NewWriteAheadLogOptions(wal.BasePath(walBasePath),
		wal.MaximumWalFileSizeBytes(WriteAheadMaxSizeBytes),
		wal.WriterFactory(func(path string) (recordio.WriterI, error) {
			return recordio.NewFileWriter(recordio.Path(path), recordio.CompressionType(recordio.CompressionTypeSnappy))
		}),
		wal.ReaderFactory(func(path string) (recordio.ReaderI, error) {
			return recordio.NewFileReaderWithPath(path)
		}),
	)

	writeAheadLog, err := wal.NewWriteAheadLog(walOpts)
	if err != nil {
		return nil, err
	}

	memStore := memstore.NewMemStore()
	rwLock := &sync.RWMutex{}

	return &DB{
		basePath:          basePath,
		memStore:          memStore,
		rwLock:            rwLock,
		wal:               writeAheadLog,
		mainSSTableReader: sstables.EmptySStableReader{},
	}, nil
}
