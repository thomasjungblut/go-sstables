package simpledb

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/recordio"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
)

const WriteAheadFolder = "wal"
const MemStoreMaxSizeBytes uint64 = 64 * 1024 * 1024   // 64mb
const WriteAheadMaxSizeBytes uint64 = 32 * 1024 * 1024 // 32mb

var NotFound = errors.New("NotFound")

type DatabaseI interface {
	recordio.OpenClosableI

	// Get returns the value for the given key. If there is no value for the given
	// key it will return NotFound as the error and an empty string value. Otherwise
	// the error will contain any other usual io error that can be expected.
	Get(key string) (string, error)

	// Put adds the given value for the given key. If this key already exists, it will
	// overwrite the already existing value with the given one.
	Put(key, value string) error

	// Delete will delete the value for the given key.
	// It will return NotFound if the key does not exist.
	Delete(key string) error
}

type DB struct {
	basePath          string
	memStore          memstore.MemStoreI
	rwLock            *sync.RWMutex
	wal               wal.WriteAheadLogI
	mainSSTableReader sstables.SSTableReaderI
	// this path is a child to the basePath, currently encodes an increasing number
	currentSSTablePath string
}

func (db *DB) Open() error {
	// TODO if files already exist we need to reconstruct from the WAL
	return nil
}

func (db *DB) Close() error {
	// this should close all files properly, TODO flush the memstore and merge all sstables
	err := db.wal.Close()
	if err != nil {
		return err
	}

	err = db.mainSSTableReader.Close()
	if err != nil {
		return err
	}

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

	// we deliberately do not append with fsync here, it's a simple db :)
	err = db.wal.Append(walBytes)
	if err != nil {
		return err
	}

	err = db.memStore.Upsert(keyBytes, valBytes)
	if err != nil {
		return err
	}

	// TODO below should be done in a goroutine to not affect write latency
	// TODO here we should already merge both sstables for ease of use
	// TODO some of the parts can be atomic swaps instead of being under the rwLock
	// have a channel with capacity 1 block on this operation if the existing didn't finish yet
	if db.memStore.EstimatedSizeInBytes() > MemStoreMaxSizeBytes {
		// normally we would flush the memstore to disk, for simplicity sake we can directly merge it with the
		// sstable that we already have and swap the reader out.
		memStoreIterator := db.memStore.SStableIterator()
		sstableIterator, err := db.mainSSTableReader.Scan()
		if err != nil {
			return err
		}

		readPath := path.Join(db.basePath, db.currentSSTablePath)
		i, _ := strconv.Atoi(db.currentSSTablePath)
		db.currentSSTablePath = strconv.Itoa(i + 1)
		writePath := path.Join(db.basePath, db.currentSSTablePath)
		err = os.MkdirAll(writePath, os.ModeDir)
		if err != nil {
			return err
		}

		writer, err := sstables.NewSSTableStreamWriter(
			sstables.WriteBasePath(writePath),
			sstables.WithKeyComparator(skiplist.BytesComparator),
			sstables.BloomExpectedNumberOfElements(uint64(db.memStore.Size())))
		if err != nil {
			return err
		}

		err = sstables.NewSSTableMerger(skiplist.BytesComparator).Merge([]sstables.SSTableIteratorI{
			memStoreIterator,
			sstableIterator,
		}, writer)
		if err != nil {
			return err
		}

		reader, err := sstables.NewSSTableReader(
			sstables.ReadBasePath(writePath),
			sstables.ReadWithKeyComparator(skiplist.BytesComparator),
		)
		if err != nil {
			return err
		}

		err = db.mainSSTableReader.Close()
		if err != nil {
			return err
		}
		err = os.RemoveAll(readPath)
		if err != nil {
			return err
		}

		db.mainSSTableReader = reader
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
		basePath:           basePath,
		memStore:           memStore,
		rwLock:             rwLock,
		wal:                writeAheadLog,
		mainSSTableReader:  sstables.EmptySStableReader{},
		currentSSTablePath: "0",
	}, nil
}
