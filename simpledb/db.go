package simpledb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/recordio"
	dbproto "github.com/thomasjungblut/go-sstables/simpledb/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"github.com/thomasjungblut/go-sstables/wal"
)

const SSTablePrefix = "sstable"
const SSTablePattern = SSTablePrefix + "_%015d"
const SSTableCompactionPathPrefix = SSTablePrefix + "_compaction"
const CompactionFinishedSuccessfulFileName = "compaction_successful"
const WriteAheadFolder = "wal"
const MemStoreMaxSizeBytes uint64 = 1024 * 1024 * 1024 // 1gb
const NumSSTablesToTriggerCompaction int = 10
const DefaultCompactionMaxSizeBytes uint64 = 5 * 1024 * 1024 * 1024 // 5gb
const DefaultCompactionInterval = 5 * time.Second
const DefaultCompactionRatio = float32(0.2)
const DefaultWriteBufferSizeBytes uint64 = 4 * 1024 * 1024 // 4Mb
const DefaultReadBufferSizeBytes uint64 = 4 * 1024 * 1024  // 4Mb

// DefaultGroupCommitMaxWait doesn't delay writes to wait for more concurrent writes. The group commit
// batches still grow with the load, as writes queue up while the previous batch is synced.
const DefaultGroupCommitMaxWait time.Duration = 0

var ErrNotFound = errors.New("ErrNotFound")
var ErrNotOpenedYet = errors.New("database has not been opened yet, please call Open() first")
var ErrAlreadyOpen = errors.New("database is already open")
var ErrAlreadyClosed = errors.New("database is already closed")
var ErrEmptyKeyValue = errors.New("neither empty keys nor values are allowed")

type DatabaseI interface {
	recordio.OpenClosableI

	// Get returns the value for the given key. If there is no value for the given
	// key it will return ErrNotFound as the error and an empty string value. Otherwise,
	// the error will contain any other usual io error that can be expected.
	Get(key string) (string, error)

	// GetBytes returns the value for the given key. If there is no value for the given
	// key it will return ErrNotFound as the error. Otherwise, error will contain any other usual io error
	// that can be expected.
	GetBytes(key []byte) ([]byte, error)

	// Put adds the given value for the given key. If this key already exists, it will
	// overwrite the already existing value with the given one.
	// Unfortunately this method does not support empty keys and values, that will immediately return an error.
	Put(key, value string) error

	// PutBytes adds the given value for the given key. If this key already exists, it will
	// overwrite the already existing value with the given one.
	// Unfortunately this method does not support empty keys and values, that will immediately return an error.
	PutBytes(key, value []byte) error

	// Delete will delete the value for the given key. It will ignore when a key does not exist in the database.
	// Underneath it will be tombstoned, which still stores it and makes it not retrievable through this interface.
	Delete(key string) error

	// DeleteBytes will delete the value for the given key. It will ignore when a key does not exist in the database.
	// Underneath it will be tombstoned, which still stores it and makes it not retrievable through this interface.
	DeleteBytes(key []byte) error
}

type compactionAction struct {
	pathsToCompact []string
	totalRecords   uint64
}

type memStoreFlushAction struct {
	memStore *memstore.MemStoreI
	walPath  string
}

type DB struct {
	// NOTE: the generation to be 64-bit aligned for 32-bit targets (e.g. ARM), so be careful when moving this field anywhere else.
	// read more here: https://pkg.go.dev/sync/atomic#pkg-note-BUG
	currentGeneration uint64

	cmp                     skiplist.Comparator[[]byte]
	basePath                string
	currentSSTablePath      string
	memstoreMaxSize         uint64
	compactionFileThreshold int
	compactionInterval      time.Duration
	compactionRatio         float32
	compactedMaxSizeBytes   uint64
	enableCompactions       bool
	open                    bool
	closed                  bool

	rwLock         *sync.RWMutex
	writeBatcher   *wal.Batcher[*pendingWrite]
	wal            wal.WriteAheadLogI
	sstableManager *SSTableManager
	memStore       *RWMemstore

	storeFlushChannel chan memStoreFlushAction
	doneFlushChannel  chan bool

	compactionTicker            *time.Ticker
	compactionTickerStopChannel chan interface{}
	doneCompactionChannel       chan bool

	writeBufferSizeBytes uint64
	readBufferSizeBytes  uint64
}

func (db *DB) Open() error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	if db.open {
		return ErrAlreadyOpen
	}

	err := db.repairCompactions()
	if err != nil {
		return err
	}

	err = db.reconstructSSTables()
	if err != nil {
		return err
	}
	err = db.replayAndSetupWriteAheadLog()
	if err != nil {
		return err
	}

	go flushMemstoreContinuously(db)

	if db.enableCompactions {
		db.compactionTicker = time.NewTicker(db.compactionInterval)
		go backgroundCompaction(db)
	}

	db.open = true

	return nil
}

func (db *DB) Close() error {
	err := func() error {
		db.rwLock.Lock()
		defer db.rwLock.Unlock()

		if !db.open {
			return ErrNotOpenedYet
		}

		if db.closed {
			return ErrAlreadyClosed
		}

		db.closed = true

		err := db.rotateWalAndFlushMemstore()
		if err != nil {
			return err
		}

		close(db.storeFlushChannel)
		<-db.doneFlushChannel
		return nil
	}()

	if err != nil {
		return err
	}

	// we finish the compaction outside the lock, since the compaction internally may require it
	if db.enableCompactions {
		db.compactionTicker.Stop()
		db.compactionTickerStopChannel <- true
		<-db.doneCompactionChannel
	}

	return errors.Join(db.wal.Close(), db.sstableManager.currentSSTable().Close())
}

func (db *DB) Get(key string) (string, error) {
	bytes, err := db.GetBytes([]byte(key))
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (db *DB) GetBytes(keyBytes []byte) ([]byte, error) {
	db.rwLock.RLock()
	defer db.rwLock.RUnlock()

	if !db.open {
		return nil, ErrNotOpenedYet
	}

	if db.closed {
		return nil, ErrAlreadyClosed
	}

	// we have to read the sstable first and then augment it with
	// any changes that were reflected in the memstore
	var sstableNotFound bool
	ssTableVal, err := db.sstableManager.currentSSTable().Get(keyBytes)
	if err != nil {
		if errors.Is(err, sstables.NotFound) {
			sstableNotFound = true
		} else {
			return nil, err
		}
	} else if ssTableVal == nil || len(ssTableVal) == 0 {
		sstableNotFound = true
	}

	memStoreVal, err := db.memStore.Get(keyBytes)
	if err != nil {
		if errors.Is(err, memstore.KeyNotFound) {
			if sstableNotFound {
				return nil, ErrNotFound
			} else {
				return ssTableVal, nil
			}
		} else if errors.Is(err, memstore.KeyTombstoned) {
			// regardless of what we found on the sstable:
			// if the memstore says it's tombstoned it's considered deleted
			return nil, ErrNotFound
		} else {
			return nil, err
		}
	}

	// memstore always wins if there is a value available
	return memStoreVal, nil
}

func (db *DB) Put(key, value string) error {
	if len(key) == 0 || len(value) == 0 {
		return ErrEmptyKeyValue
	}

	// string to byte conversion takes 7% of the method execution time
	keyBytes := []byte(key)
	valBytes := []byte(value)

	return db.PutBytes(keyBytes, valBytes)
}

func (db *DB) PutBytes(keyBytes, valBytes []byte) error {
	// proto marshal takes 60%(!) of this method execution time
	walBytes, err := proto.Marshal(&dbproto.WalMutation{
		Mutation: &dbproto.WalMutation_Addition{
			Addition: &dbproto.UpsertMutation{
				KeyBytes:   keyBytes,
				ValueBytes: valBytes,
			},
		},
	})
	if err != nil {
		return err
	}

	return db.syncedWrite(walBytes, func() error {
		return db.memStore.Upsert(keyBytes, valBytes)
	})
}

func (db *DB) Delete(key string) error {
	byteKey := []byte(key)
	return db.DeleteBytes(byteKey)
}

func (db *DB) DeleteBytes(byteKey []byte) error {
	bytes, err := proto.Marshal(&dbproto.WalMutation{
		Mutation: &dbproto.WalMutation_DeleteTombStone{
			DeleteTombStone: &dbproto.DeleteTombstoneMutation{
				KeyBytes: byteKey,
			},
		},
	})
	if err != nil {
		return err
	}

	return db.syncedWrite(bytes, func() error {
		return db.memStore.Delete(byteKey)
	})
}

// NewSimpleDB creates a new db that requires a directory that exist, it can be empty in case of existing databases.
// The error in case it doesn't exist can be checked using normal os package functions like os.IsNotExist(err)
func NewSimpleDB(basePath string, extraOptions ...ExtraOption) (*DB, error) {
	// validate the basePath exist
	_, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	extraOpts := defaultExtraOptions()

	for _, extraOption := range extraOptions {
		extraOption(extraOpts)
	}

	cmp := skiplist.BytesComparator{}
	mStore := memstore.NewMemStore()
	rwLock := &sync.RWMutex{}
	flusherChan := make(chan memStoreFlushAction)
	doneFlushChan := make(chan bool)
	doneCompactionChan := make(chan bool)
	compactionTimerStopChannel := make(chan interface{}, 1)

	sstableManager := NewSSTableManager(cmp, rwLock, basePath)

	db := &DB{
		currentGeneration:           uint64(0),
		cmp:                         cmp,
		basePath:                    basePath,
		currentSSTablePath:          "",
		memstoreMaxSize:             extraOpts.memstoreSizeBytes,
		compactionFileThreshold:     extraOpts.compactionFileThreshold,
		compactedMaxSizeBytes:       extraOpts.compactionMaxSizeBytes,
		enableCompactions:           extraOpts.enableCompactions,
		compactionInterval:          extraOpts.compactionRunInterval,
		compactionRatio:             extraOpts.compactionRatio,
		closed:                      false,
		rwLock:                      rwLock,
		wal:                         nil,
		sstableManager:              sstableManager,
		memStore:                    &RWMemstore{mStore, mStore},
		storeFlushChannel:           flusherChan,
		doneFlushChannel:            doneFlushChan,
		compactionTickerStopChannel: compactionTimerStopChannel,
		doneCompactionChannel:       doneCompactionChan,
		readBufferSizeBytes:         extraOpts.readBufferSizeBytes,
		writeBufferSizeBytes:        extraOpts.writeBufferSizeBytes,
	}
	// writes with a synced WAL are batched to share their fsyncs (group commit)
	db.writeBatcher = wal.NewBatcher(db.commitWrites,
		wal.MaxBatchSize(extraOpts.groupCommitMaxBatchSize), wal.MaxBatchWait(extraOpts.groupCommitMaxWait))
	return db, nil
}

// options

type ExtraOptions struct {
	memstoreSizeBytes       uint64
	enableCompactions       bool
	groupCommitMaxBatchSize int
	groupCommitMaxWait      time.Duration
	compactionFileThreshold int
	compactionMaxSizeBytes  uint64
	compactionRunInterval   time.Duration
	compactionRatio         float32
	writeBufferSizeBytes    uint64
	readBufferSizeBytes     uint64
}

type ExtraOption func(options *ExtraOptions)

func defaultExtraOptions() *ExtraOptions {
	return &ExtraOptions{
		memstoreSizeBytes:       MemStoreMaxSizeBytes,
		enableCompactions:       true,
		groupCommitMaxBatchSize: wal.DefaultMaxGroupCommitBatchSize,
		groupCommitMaxWait:      DefaultGroupCommitMaxWait,
		compactionFileThreshold: NumSSTablesToTriggerCompaction,
		compactionMaxSizeBytes:  DefaultCompactionMaxSizeBytes,
		compactionRunInterval:   DefaultCompactionInterval,
		compactionRatio:         DefaultCompactionRatio,
		writeBufferSizeBytes:    DefaultWriteBufferSizeBytes,
		readBufferSizeBytes:     DefaultReadBufferSizeBytes,
	}
}

// MemstoreSizeBytes controls the size of the memstore, after this limit is hit the memstore will be written to disk.
// Default is 1 GiB.
func MemstoreSizeBytes(n uint64) ExtraOption {
	return func(args *ExtraOptions) {
		args.memstoreSizeBytes = n
	}
}

// DisableCompactions will disable the compaction process from running. Default is enabled.
func DisableCompactions() ExtraOption {
	return func(args *ExtraOptions) {
		args.enableCompactions = false
	}
}

// GroupCommitMaxBatchSize limits how many concurrent writes are made durable together with a single fsync of the WAL
// (group commit). Default is wal.DefaultMaxGroupCommitBatchSize.
func GroupCommitMaxBatchSize(n int) ExtraOption {
	return func(args *ExtraOptions) {
		args.groupCommitMaxBatchSize = n
	}
}

// GroupCommitMaxWait is the timeout for which a write waits for more concurrent writes to share its WAL fsync. It's
// synced earlier once the batch is full (GroupCommitMaxBatchSize), but never waits longer than the timeout. This
// increases the batch sizes, but also delays every write, even a single one. Default is DefaultGroupCommitMaxWait, zero
// disables waiting.
func GroupCommitMaxWait(d time.Duration) ExtraOption {
	return func(args *ExtraOptions) {
		args.groupCommitMaxWait = d
	}
}

// CompactionRunInterval configures how often the compaction ticker tries to compact sstables.
// By default, it's every DefaultCompactionInterval.
func CompactionRunInterval(interval time.Duration) ExtraOption {
	return func(args *ExtraOptions) {
		args.compactionRunInterval = interval
	}
}

// CompactionRatio configures when a sstable is eligible for compaction through a ratio threshold, which can be used to save disk space.
// The ratio is measured as the amount of tombstoned keys divided by the overall record number in the sstable.
// This threshold must be between 0.0 and 1.0 as float32 and by default is DefaultCompactionRatio.
// So when a sstable has more than 20% of records flagged as tombstones, it will be automatically compacted.
// A value of 1.0 turns this feature off and resorts to the max size calculation, a value of 0.0 will always compact
// all files regardless of how many tombstones are in there.
func CompactionRatio(ratio float32) ExtraOption {
	if ratio < 0.0 || ratio > 1.0 {
		panic(fmt.Sprintf("invalid compaction ratio: %f, must be between 0 and 1", ratio))
	}
	return func(args *ExtraOptions) {
		args.compactionRatio = ratio
	}
}

// CompactionFileThreshold tells how often SSTables are being compacted, this is measured in the number of SSTables.
// The default is 10, which in turn will compact into a single SSTable.
func CompactionFileThreshold(n int) ExtraOption {
	return func(args *ExtraOptions) {
		args.compactionFileThreshold = n
	}
}

// CompactionMaxSizeBytes tells whether an SSTable is considered for compaction.
// This is a best-effort implementation, depending on the write/delete pattern you may need to compact bigger tables.
// Default is 5GB in DefaultCompactionMaxSizeBytes
func CompactionMaxSizeBytes(n uint64) ExtraOption {
	return func(args *ExtraOptions) {
		args.compactionMaxSizeBytes = n
	}
}

// WriteBufferSizeBytes is the write buffer size for all buffer used by simple db.
func WriteBufferSizeBytes(n uint64) ExtraOption {
	return func(args *ExtraOptions) {
		args.writeBufferSizeBytes = n
	}
}

// ReadBufferSizeBytes is the read buffer size for all buffer used by simple db.
func ReadBufferSizeBytes(n uint64) ExtraOption {
	return func(args *ExtraOptions) {
		args.readBufferSizeBytes = n
	}
}
