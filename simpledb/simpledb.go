package simpledb

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/recordio"
)

const WriteAheadFolder = "wal"
const MemStoreMaxSizeBytes uint64 = 16 * 1024 * 1024   // 16mb
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
