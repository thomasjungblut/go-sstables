package wal

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

const TestMaxWalFileSize uint64 = 8 * 1024 // 8k

func TestSimpleWriteHappyPath(t *testing.T) {
	log, recorder := singleRecordWal(t, "wal_simpleWriteHappyPath")
	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestSimpleWriteWithRotationHappyPath(t *testing.T) {
	log := newTestWalAppender(t, "wal_simpleWriteWithRotationHappyPath")

	var recorder [][]byte
	assert.Equal(t, uint(1), log.nextWriterNumber)

	for i := uint64(0); i < (uint64(3) * (TestMaxWalFileSize / uint64(8))); i++ {
		record := make([]byte, 8)
		binary.BigEndian.PutUint64(record, i)
		appendAndRecord(t, log, record, &recorder)
	}

	// should have five files by now, as we did three rounds at 8K
	// plus the overhead of headers which accounts for another WAL on overflow
	// and since this is the next WAL number, it should be total of 6
	assert.Equal(t, uint(6), log.nextWriterNumber)
	err := log.Close()
	require.Nil(t, err)
	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestSimpleWriteWithRotationMoreThanHundredFiles(t *testing.T) {
	log := newTestWalAppender(t, "wal_simpleWriteWithRotationMoreThanHundred")

	var recorder [][]byte
	assert.Equal(t, uint(1), log.nextWriterNumber)

	for i := uint64(0); i < uint64(200); i++ {
		record := make([]byte, 8)
		binary.BigEndian.PutUint64(record, i)
		appendAndRecord(t, log, record, &recorder)
		_, err := log.Rotate()
		require.Nil(t, err)
	}

	assert.Equal(t, uint(201), log.nextWriterNumber)
	err := log.Close()
	require.Nil(t, err)
	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestWriteMoreThanAMillionFilesFails(t *testing.T) {
	log := newTestWalAppender(t, "wal_writeMoreThanAMillionFilesFails")
	// this is a bit hackish, but useful to exercise this path without creating a million files
	log.nextWriterNumber = 1000000
	record := make([]byte, TestMaxWalFileSize)
	err := log.AppendSync(record)
	assert.Contains(t, err.Error(), "not supporting more than one million wal files at the minute. Current limit exceeded: 1000000")
}

func TestWriteBiggerRecordThanMaxFileSize(t *testing.T) {
	log := newTestWalAppender(t, "wal_writeBiggerRecordThanMaxFileSize")
	var recorder [][]byte
	assert.Equal(t, uint(1), log.nextWriterNumber)
	bigRecord := make([]byte, TestMaxWalFileSize+5)
	for i := 0; i < len(bigRecord); i++ {
		bigRecord[i] = byte(i % math.MaxUint8)
	}
	appendAndRecord(t, log, bigRecord, &recorder)
	// this will actually cause a new WAL to be opened
	// that's OK to happen, even if that means in this case one WAL will be entirely empty
	assert.Equal(t, uint(2), log.nextWriterNumber)
	err := log.Close()
	require.Nil(t, err)
	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestForcedRotation(t *testing.T) {
	log := newTestWalAppender(t, "wal_forcedRotation")

	var recorder [][]byte
	assert.Equal(t, uint(1), log.nextWriterNumber)
	for i := 0; i < 95; i++ {
		appendAndRecord(t, log, []byte{byte(i)}, &recorder)
		_, err := log.Rotate()
		require.Nil(t, err)
		assert.Equal(t, uint(i+2), log.nextWriterNumber)
	}

	require.Nil(t, log.Close())

	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func singleRecordWal(t *testing.T, tmpDirName string) (*Appender, [][]byte) {
	log := newTestWalAppender(t, tmpDirName)

	var recorder [][]byte
	assert.Equal(t, uint(1), log.nextWriterNumber)
	appendAndRecord(t, log, []byte{1}, &recorder)
	err := log.Close()
	require.Nil(t, err)

	t.Cleanup(func() {
		_ = NewCleaner(log.walOptions).Clean()
	})

	return log, recorder
}

func newTestWalAppender(t *testing.T, tmpDirName string) *Appender {
	tmpDir, err := os.MkdirTemp("", tmpDirName)
	require.Nil(t, err)

	opts, err := NewWriteAheadLogOptions(BasePath(tmpDir), MaximumWalFileSizeBytes(TestMaxWalFileSize))
	require.Nil(t, err)

	log, err := NewAppender(opts)
	require.Nil(t, err)

	t.Cleanup(func() {
		_ = log.Close()
		_ = NewCleaner(opts).Clean()
	})

	return log.(*Appender)
}

func assertRecorderMatchesReplay(t *testing.T, opts *Options, recorder [][]byte) {
	repl, err := NewReplayer(opts)
	require.Nil(t, err)

	i := 0
	err = repl.Replay(func(record []byte) error {
		assert.Equal(t, recorder[i], record)
		i++
		return nil
	})
	require.Nil(t, err)
	assert.Equal(t, len(recorder), i)
}

func appendAndRecord(t *testing.T, wal WriteAheadLogAppendI, record []byte, recorder *[][]byte) {
	*recorder = append(*recorder, record)
	err := wal.AppendSync(record)
	require.Nil(t, err)
}
