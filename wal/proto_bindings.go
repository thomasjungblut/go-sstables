package wal

import (
	"github.com/gogo/protobuf/proto"
	"github.com/thomasjungblut/go-sstables/recordio"
)

type ProtoAppenderReplayer struct {
	wal *WriteAheadLog
}

type ProtoWriteAheadLog struct {
	*ProtoAppenderReplayer
	*Cleaner
}

type ProtoWriteAheadLogReplayI interface {
	// Replays the whole WAL from start, calling the given process function
	// for each record in guaranteed order.
	// This needs a factory to create the respective message type to use for deserialization.
	Replay(messageFactory func() proto.Message, process func(record proto.Message) error) error
}

type ProtoWriteAheadLogAppendI interface {
	recordio.CloseableI
	// Appends a given record and execute fsync to guarantee the persistence of the record.
	// Has considerably less throughput than Append.
	AppendSync(record proto.Message) error
}

func (p *ProtoWriteAheadLog) Replay(messageFactory func() proto.Message, process func(record proto.Message) error) error {
	err := p.wal.Replay(func(bytes []byte) error {
		msg := messageFactory()
		err := proto.Unmarshal(bytes, msg)
		if err != nil {
			return err
		}
		return process(msg)
	})

	return err
}

func (p *ProtoWriteAheadLog) AppendSync(record proto.Message) error {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	return p.wal.AppendSync(bytes)
}

func (p *ProtoWriteAheadLog) Close() error {
	return p.wal.Close()
}

func newProtoAppenderReplayer(opts *Options) (*ProtoAppenderReplayer, error) {
	wal, err := NewWriteAheadLog(opts)
	if err != nil {
		return nil, err
	}

	return &ProtoAppenderReplayer{
		wal: wal,
	}, nil
}

func NewProtoWriteAheadLog(opts *Options) (*ProtoWriteAheadLog, error) {
	appRepl, err := newProtoAppenderReplayer(opts)
	if err != nil {
		return nil, err
	}
	return &ProtoWriteAheadLog{
		ProtoAppenderReplayer: appRepl,
		Cleaner:               NewCleaner(opts),
	}, nil
}
