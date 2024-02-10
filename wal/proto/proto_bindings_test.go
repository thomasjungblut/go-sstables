package proto

import (
	"github.com/stretchr/testify/assert"
	w "github.com/thomasjungblut/go-sstables/wal"
	"github.com/thomasjungblut/go-sstables/wal/test_files"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

const TestMaxWalFileSize uint64 = 8 * 1024 // 8k

func TestProtoWALEndToEndHappyPath(t *testing.T) {
	wal := newTestProtoWal(t, "wal_proto_e2e_happy_path")

	maxNum := uint64(250)
	for i := uint64(0); i < maxNum; i++ {
		msg := test_files.SequenceNumber{SequenceNumber: i}
		err := wal.AppendSync(&msg)
		assert.Nil(t, err)
	}

	assert.Nil(t, wal.Close())

	sequenceNum := &test_files.SequenceNumber{}
	msgFactory := func() proto.Message {
		return sequenceNum
	}

	expected := uint64(0)
	err := wal.Replay(msgFactory, func(msg proto.Message) error {
		assert.Equal(t, expected, msg.(*test_files.SequenceNumber).SequenceNumber)
		expected++
		return nil
	})

	assert.Nil(t, err)
	assert.Equal(t, maxNum, expected)
}

func newTestProtoWal(t *testing.T, tmpDirName string) *WriteAheadLog {
	tmpDir, err := os.MkdirTemp("", tmpDirName)
	assert.Nil(t, err)

	opts, err := w.NewWriteAheadLogOptions(
		w.BasePath(tmpDir),
		w.MaximumWalFileSizeBytes(TestMaxWalFileSize))
	assert.Nil(t, err)

	wal, err := NewProtoWriteAheadLog(opts)
	assert.Nil(t, err)
	t.Cleanup(func() {
		_ = wal.Clean()
	})

	return wal.(*WriteAheadLog)
}
