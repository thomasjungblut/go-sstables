package wal

import (
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/wal/test_files"
	"io/ioutil"
	"testing"
)

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

func newTestProtoWal(t *testing.T, tmpDirName string) *ProtoWriteAheadLog {
	tmpDir, err := ioutil.TempDir("", tmpDirName)
	assert.Nil(t, err)

	opts, err := NewWriteAheadLogOptions(BasePath(tmpDir), MaximumWalFileSizeBytes(TestMaxWalFileSize))
	assert.Nil(t, err)

	wal, err := NewProtoWriteAheadLog(opts)
	assert.Nil(t, err)
	t.Cleanup(func() {
		_ = wal.Clean()
	})

	return wal
}
