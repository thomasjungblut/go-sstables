package simpledb

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/recordio"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type DB struct {
	basePath string
	memStore *memstore.MemStore
	rwLock   *sync.RWMutex
	wal      *wal.WriteAheadLog
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
	db.rwLock.RLock()
	defer db.rwLock.RUnlock()
	// TODO we also need to include the sstable

	val, err := db.memStore.Get([]byte(key))
	if err != nil {
		if err == memstore.KeyNotFound {
			return "", NotFound
		}
		return "", err
	}

	return string(val), nil
}

func (db *DB) Put(key, value string) error {
	bytes, err := proto.Marshal(&dbproto.WalMutation{
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

	err = db.wal.Appender.AppendSync(bytes)
	if err != nil {
		return err
	}

	err = db.memStore.Upsert([]byte(key), []byte(value))
	if err != nil {
		return err
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

	err = db.wal.Appender.AppendSync(bytes)
	if err != nil {
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
		basePath: basePath,
		memStore: memStore,
		rwLock:   rwLock,
		wal:      writeAheadLog,
	}, nil
}
