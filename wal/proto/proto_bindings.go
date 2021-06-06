package proto

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	w "github.com/thomasjungblut/go-sstables/wal"
	"google.golang.org/protobuf/proto"
)

type AppenderReplayer struct {
	wal *w.WriteAheadLog
}

type WriteAheadLog struct {
	*AppenderReplayer
	*w.Cleaner
}

type WriteAheadLogReplayI interface {
	// Replays the whole WAL from start, calling the given process function
	// for each record in guaranteed order.
	// This needs a factory to create the respective message type to use for deserialization.
	Replay(messageFactory func() proto.Message, process func(record proto.Message) error) error
}

type WriteAheadLogAppendI interface {
	recordio.CloseableI
	// Appends a given record and execute fsync to guarantee the persistence of the record.
	// Has considerably less throughput than Append.
	AppendSync(record proto.Message) error
}

func (p *WriteAheadLog) Replay(messageFactory func() proto.Message, process func(record proto.Message) error) error {
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

func (p *WriteAheadLog) AppendSync(record proto.Message) error {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	return p.wal.AppendSync(bytes)
}

func (p *WriteAheadLog) Close() error {
	return p.wal.Close()
}

func newProtoAppenderReplayer(opts *w.Options) (*AppenderReplayer, error) {
	wal, err := w.NewWriteAheadLog(opts)
	if err != nil {
		return nil, err
	}

	return &AppenderReplayer{
		wal: wal,
	}, nil
}

func NewProtoWriteAheadLog(opts *w.Options) (*WriteAheadLog, error) {
	appRepl, err := newProtoAppenderReplayer(opts)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{
		AppenderReplayer: appRepl,
		Cleaner:          w.NewCleaner(opts),
	}, nil
}
