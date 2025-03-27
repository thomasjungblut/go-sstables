package sstables

import (
	"errors"
	"fmt"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"io"
)

type SkipListIndex struct {
	skiplist.MapI[[]byte, IndexVal]
	NoOpOpenClose
}

func (s *SkipListIndex) Contains(key []byte) (bool, error) {
	return s.MapI.Contains(key), nil
}

type SkipListIndexLoader struct {
	KeyComparator  skiplist.Comparator[[]byte]
	ReadBufferSize int
}

func (l *SkipListIndexLoader) Load(indexPath string, _ *proto.MetaData) (_ SortedKeyIndex, err error) {
	reader, err := rProto.NewReader(
		rProto.ReaderPath(indexPath),
		rProto.ReadBufferSizeBytes(l.ReadBufferSize),
	)
	if err != nil {
		return nil, fmt.Errorf("error while creating index reader of sstable in '%s': %w", indexPath, err)
	}

	err = reader.Open()
	if err != nil {
		return nil, fmt.Errorf("error while opening index reader of sstable in '%s': %w", indexPath, err)
	}

	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	indexMap := skiplist.NewSkipListMap[[]byte, IndexVal](l.KeyComparator)
	record := &proto.IndexEntry{}

	for {
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		indexMap.Insert(record.Key, IndexVal{
			Offset:   record.ValueOffset,
			Checksum: record.Checksum,
		})
	}

	return &SkipListIndex{indexMap, NoOpOpenClose{}}, nil
}
