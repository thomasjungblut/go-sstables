package sstables

import (
	"github.com/steakknife/bloomfilter"
	"hash/fnv"
	"errors"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"path"
	"github.com/thomasjungblut/go-sstables/recordio"
	"io"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"os"
)

type SSTableReader struct {
	opts          *SSTableReaderOptions
	bloomFilter   *bloomfilter.Filter
	keyComparator skiplist.KeyComparator
	index         *skiplist.SkipListMap // key ([]byte) to uint64 value file offset
	dataReader    *recordio.MMapProtoReader
}

func (reader *SSTableReader) Contains(key []byte) bool {
	// short-cut for the bloom filter to tell whether it's not in the set (if available)
	if reader.bloomFilter != nil {
		fnvHash := fnv.New64()
		fnvHash.Write(key)
		if !reader.bloomFilter.Contains(fnvHash) {
			return false
		}
	}

	// go back to the index/disk to see if the key is available
	return reader.index.Contains(key)
}

func (reader *SSTableReader) Get(key []byte) ([]byte, error) {
	valOffset, err := reader.index.Get(key)
	if err == skiplist.NotFound {
		return nil, NotFound
	}

	value := &proto.DataEntry{}
	_, err = reader.dataReader.ReadNextAt(value, valOffset.(uint64))
	if err != nil && err != io.EOF {
		return nil, err
	}

	return value.Value, nil
}

func (reader *SSTableReader) Close() error {
	return reader.dataReader.Close()
}

func NewSSTableReader(readerOptions ...ReadOption) (*SSTableReader, error) {

	opts := &SSTableReaderOptions{
		basePath: "",
	}

	for _, readOption := range readerOptions {
		readOption(opts)
	}

	if opts.basePath == "" {
		return nil, errors.New("basePath was not supplied")
	}

	if opts.keyComparator == nil {
		return nil, errors.New("no key comparator supplied")
	}

	index, err := readIndex(path.Join(opts.basePath, IndexFileName), opts.keyComparator)
	if err != nil {
		return nil, err
	}

	filter, err := readFilterIfExists(path.Join(opts.basePath, BloomFileName))
	if err != nil {
		return nil, err
	}

	dataReader, err := recordio.NewMMapProtoReaderWithPath(path.Join(opts.basePath, DataFileName))
	if err != nil {
		return nil, err
	}

	err = dataReader.Open()
	if err != nil {
		return nil, err
	}

	return &SSTableReader{opts: opts, bloomFilter: filter, index: index, dataReader: dataReader}, nil
}

func readIndex(indexPath string, keyComparator skiplist.KeyComparator) (*skiplist.SkipListMap, error) {
	reader, err := recordio.NewProtoReaderWithPath(indexPath)
	if err != nil {
		return nil, err
	}

	err = reader.Open()
	if err != nil {
		return nil, err
	}

	indexMap := skiplist.NewSkipListMap(keyComparator)

	for {
		record := &proto.IndexEntry{}
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		indexMap.Insert(record.Key, record.ValueOffset)
	}

	err = reader.Close()
	if err != nil {
		return nil, err
	}

	return indexMap, nil
}

func readFilterIfExists(filterPath string) (*bloomfilter.Filter, error) {
	if _, err := os.Stat(filterPath); os.IsNotExist(err) {
		return nil, nil
	}

	filter, err := bloomfilter.ReadFile(filterPath)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

// options

// read/write options
type SSTableReaderOptions struct {
	basePath      string
	keyComparator skiplist.KeyComparator
}

type ReadOption func(*SSTableReaderOptions)

func ReadBasePath(p string) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.basePath = p
	}
}

func ReadWithKeyComparator(cmp skiplist.KeyComparator) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.keyComparator = cmp
	}
}