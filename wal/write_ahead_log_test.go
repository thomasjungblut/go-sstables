package wal

import (
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/recordio"
	"io/ioutil"
	"testing"
)

func TestWALEndToEndHappyPath(t *testing.T) {
	wal := newTestWal(t, "wal_e2e_happy_path")

	maxNum := uint64(2500)
	for i := uint64(0); i < maxNum; i++ {
		record := make([]byte, 8)
		binary.BigEndian.PutUint64(record, i)
		err := wal.AppendSync(record)
		assert.Nil(t, err)
	}

	assert.Nil(t, wal.Close())

	expected := uint64(0)
	err := wal.Replay(func(record []byte) error {
		n := binary.BigEndian.Uint64(record)
		assert.Equal(t, expected, n)
		expected++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, maxNum, expected)
}

func TestWALCrashRecovery(t *testing.T) {
	// this is a very naive test to figure out whether we can still read a partially written WAL
	// ideally we would start a new process to continuously write data into a WAL and make it crash.
	// this certainly also does not simulate when we actually have a full power-down scenario,
	// but that's the best we can do in a unit test.
	wal := newTestWal(t, "wal_e2e_crash_recovery")

	maxNum := uint64(100)
	for i := uint64(0); i < maxNum; i++ {
		record := make([]byte, 8)
		binary.BigEndian.PutUint64(record, i)
		err := wal.AppendSync(record)
		assert.Nil(t, err)

		// mind: we do not close the wal between, to test if we are actually fsync'ing properly and
		// we can always read from whatever was appended already
		expected := uint64(0)
		err = wal.Replay(func(record []byte) error {
			n := binary.BigEndian.Uint64(record)
			assert.Equal(t, expected, n)
			expected++
			return nil
		})
		assert.Equal(t, i+1, expected)
		assert.Nil(t, err)
	}
}

func TestOptionMissingBasePath(t *testing.T) {
	_, err := NewWriteAheadLogOptions(MaximumWalFileSizeBytes(TestMaxWalFileSize))
	assert.Equal(t, errors.New("basePath was not supplied"), err)
}

func newTestWal(t *testing.T, tmpDirName string) *WriteAheadLog {
	tmpDir, err := ioutil.TempDir("", tmpDirName)
	assert.Nil(t, err)

	opts, err := NewWriteAheadLogOptions(BasePath(tmpDir),
		MaximumWalFileSizeBytes(TestMaxWalFileSize),
		WriterFactory(func(path string) (recordio.WriterI, error) {
			return recordio.NewFileWriter(recordio.Path(path), recordio.CompressionType(recordio.CompressionTypeSnappy))
		}),
		ReaderFactory(func(path string) (recordio.ReaderI, error) {
			return recordio.NewFileReaderWithPath(path)
		}),
	)
	assert.Nil(t, err)

	wal, err := NewWriteAheadLog(opts)
	assert.Nil(t, err)
	t.Cleanup(func() {
		_ = wal.Close()
		_ = wal.Clean()
	})

	return wal.(*WriteAheadLog)
}
