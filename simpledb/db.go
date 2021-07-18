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
	"sync"
)

const SSTablePrefix = "sstable"
const SSTablePattern = SSTablePrefix + "_%015d"
const WriteAheadFolder = "wal"
const MemStoreMaxSizeBytes uint64 = 1024 * 1024 * 1024 // 1gb

var NotFound = errors.New("NotFound")
var EmptyKeyValue = errors.New("neither empty keys nor values are allowed")

type DatabaseI interface {
	recordio.OpenClosableI

	// Get returns the value for the given key. If there is no value for the given
	// key it will return NotFound as the error and an empty string value. Otherwise
	// the error will contain any other usual io error that can be expected.
	Get(key string) (string, error)

	// Put adds the given value for the given key. If this key already exists, it will
	// overwrite the already existing value with the given one.
	// Unfortunately this method does not support empty keys and values, that will immediately return an error.
	Put(key, value string) error

	// Delete will delete the value for the given key. It will ignore when a key does not exist in the database.
	// Underneath it will be tombstoned, which still store it and make it not retrievable through this interface.
	Delete(key string) error
}

type memStoreFlushAction struct {
	memStore *memstore.MemStoreI
	walPath  string
}

type DB struct {
	cmp                skiplist.KeyComparator
	basePath           string
	rwLock             *sync.RWMutex
	storeFlushChannel  chan memStoreFlushAction
	doneFlushChannel   chan bool
	wal                wal.WriteAheadLogI
	sstableManager     *SSTableManager
	memStore           *RWMemstore
	currentSSTablePath string
	memstoreMaxSize    uint64
	currentGeneration  int32
}

func (db *DB) Open() error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	err := db.reconstructSSTables()
	if err != nil {
		return err
	}
	err = db.replayAndSetupWriteAheadLog(err)
	if err != nil {
		return err
	}

	go flushMemstoreContinuously(db)
	// go flushMemstoreAndMergeSSTablesAsync(db)

	return nil
}

func (db *DB) Close() error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	err := db.rotateWalAndFlushMemstore()
	if err != nil {
		return err
	}

	close(db.storeFlushChannel)
	<-db.doneFlushChannel

	err = db.wal.Close()
	if err != nil {
		return err
	}

	err = db.sstableManager.currentSSTable().Close()
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
	ssTableVal, err := db.sstableManager.currentSSTable().Get(keyBytes)
	if err != nil {
		if err == sstables.NotFound {
			sstableNotFound = true
		} else {
			return "", err
		}
	} else if ssTableVal == nil || len(ssTableVal) == 0 {
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
	if len(key) == 0 || len(value) == 0 {
		return EmptyKeyValue
	}

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

	db.rwLock.Lock()
	return func() error {
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

		if db.memStore.EstimatedSizeInBytes() > db.memstoreMaxSize {
			return db.rotateWalAndFlushMemstore()
		}
		return nil
	}()
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
func NewSimpleDB(basePath string, extraOptions ...ExtraOption) (*DB, error) {
	// validate the basePath exist
	_, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	extraOpts := &ExtraOptions{
		MemStoreMaxSizeBytes,
		10,
	}

	for _, extraOption := range extraOptions {
		extraOption(extraOpts)
	}

	cmp := skiplist.BytesComparator
	mStore := memstore.NewMemStore()
	rwLock := &sync.RWMutex{}
	flusherChan := make(chan memStoreFlushAction)
	doneFlushChan := make(chan bool)

	return &DB{
		cmp:                cmp,
		basePath:           basePath,
		memStore:           &RWMemstore{mStore, mStore},
		rwLock:             rwLock,
		storeFlushChannel:  flusherChan,
		doneFlushChannel:   doneFlushChan,
		sstableManager:     NewSSTableManager(cmp),
		currentSSTablePath: "",
		memstoreMaxSize:    extraOpts.memstoreSizeBytes,
		currentGeneration:  0,
	}, nil
}

// options

type ExtraOptions struct {
	memstoreSizeBytes   uint64
	compactionThreshold int
}

type ExtraOption func(options *ExtraOptions)

// MemstoreSizeBytes controls the size of the memstore, after this limit is hit the memstore will be written to disk.
// Default is 1 GiB.
func MemstoreSizeBytes(n uint64) ExtraOption {
	return func(args *ExtraOptions) {
		args.memstoreSizeBytes = n
	}
}

// SSTableCompactionThreshold tells how often SSTables are being compacted, this is measured in the number of SSTables.
// The default is 10, which in turn will compact into a single SSTable.
func SSTableCompactionThreshold(n int) ExtraOption {
	return func(args *ExtraOptions) {
		args.compactionThreshold = n
	}
}
