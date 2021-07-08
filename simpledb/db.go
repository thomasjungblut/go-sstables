package simpledb

import (
	"errors"
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

const WriteAheadFolder = "wal"
const MemStoreMaxSizeBytes uint64 = 1024 * 1024 * 1024   // 1gb
const WriteAheadMaxSizeBytes uint64 = 1024 * 1024 * 1024 // 1gb

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

	// Delete will delete the value for the given key. It will ignore when a key does not exist in the database.
	// Underneath it will be tombstoned, which still store it and make it not retrievable through this interface.
	Delete(key string) error
}

type DB struct {
	basePath           string
	rwLock             *sync.RWMutex
	storeFlushChannel  chan *memstore.MemStoreI
	doneFlushChannel   chan bool
	wal                wal.WriteAheadLogI
	mainSSTableReader  sstables.SSTableReaderI
	memStore           *RWMemstore
	currentSSTablePath string
	memstoreMaxSize    uint64
}

func (db *DB) Open() error {
	// TODO if files already exist we need to reconstruct from the WAL
	// TODO we need to prune the WALs based on a watermark (run some GC)

	go flushMemstoreContinuously(db)
	// go flushMemstoreAndMergeSSTablesAsync(db)

	return nil
}

func (db *DB) Close() error {
	// this should close all files properly, TODO flush the memstore and merge all sstables
	close(db.storeFlushChannel)
	<-db.doneFlushChannel

	db.rwLock.Lock()
	defer db.rwLock.Unlock()

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
	} else if ssTableVal == nil {
		sstableNotFound = true
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
	// string to byte conversion takes 7% of the method execution time
	keyBytes := []byte(key)
	valBytes := []byte(value)
	// proto marshal takes 60%(!) of this method execution time
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

	swapRequired := false
	db.rwLock.Lock()
	err = func() error {
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

		swapRequired = db.memStore.EstimatedSizeInBytes() > db.memstoreMaxSize
		return nil
	}()

	// we do not do that inside the lock, because the flushing can cause us to deadlock under the rwLock
	if swapRequired {
		db.storeFlushChannel <- swapMemstore(db)
	}

	return nil
}

func (db *DB) Delete(key string) error {
	byteKey := []byte(key)
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

	err = db.memStore.Delete(byteKey)
	if err != nil {
		// we deliberately ignore not found errors, there might be a key to delete in the sstable
		// seeking into the sstable might be quite expensive, it will be dropped by the next merge anyway.
		// To record that this is actually being deleted we have to tombstone it:
		if err == memstore.KeyNotFound {
			return db.memStore.Tombstone(byteKey)
		}
		return err
	}
	return nil
}

// NewSimpleDB creates a new db that requires a directory that exist, it can be empty in case of existing databases.
// The error in case it doesn't exist can be checked using normal os package functions like os.IsNotExist(err)
func NewSimpleDB(basePath string, extraOptions ...SimpleDBExtraOption) (*DB, error) {
	// validate the basePath exist
	_, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	walBasePath := path.Join(basePath, WriteAheadFolder)
	err = os.MkdirAll(walBasePath, 0700)
	if err != nil {
		return nil, err
	}

	extraOpts := &SimpleDBExtraOptions{
		MemStoreMaxSizeBytes,
		WriteAheadMaxSizeBytes,
	}

	for _, extraOption := range extraOptions {
		extraOption(extraOpts)
	}

	walOpts, err := wal.NewWriteAheadLogOptions(wal.BasePath(walBasePath),
		wal.MaximumWalFileSizeBytes(extraOpts.walSizeBytes),
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

	mStore := memstore.NewMemStore()
	rwLock := &sync.RWMutex{}
	flusherChan := make(chan *memstore.MemStoreI)
	doneFlushChan := make(chan bool)

	return &DB{
		basePath:           basePath,
		memStore:           &RWMemstore{mStore, mStore},
		rwLock:             rwLock,
		storeFlushChannel:  flusherChan,
		doneFlushChannel:   doneFlushChan,
		wal:                writeAheadLog,
		mainSSTableReader:  sstables.EmptySStableReader{},
		currentSSTablePath: "",
		memstoreMaxSize:    extraOpts.memstoreSizeBytes,
	}, nil
}

// options

type SimpleDBExtraOptions struct {
	memstoreSizeBytes uint64
	walSizeBytes      uint64
}

type SimpleDBExtraOption func(options *SimpleDBExtraOptions)

func MemstoreSizeBytes(n uint64) SimpleDBExtraOption {
	return func(args *SimpleDBExtraOptions) {
		args.memstoreSizeBytes = n
	}
}

func WriteAheadLogSizeBytes(n uint64) SimpleDBExtraOption {
	return func(args *SimpleDBExtraOptions) {
		args.walSizeBytes = n
	}
}
