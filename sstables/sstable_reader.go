package sstables

import (
	"errors"
	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	pb "google.golang.org/protobuf/proto"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type SSTableReader struct {
	opts          *SSTableReaderOptions
	bloomFilter   *bloomfilter.Filter
	keyComparator skiplist.KeyComparator
	index         skiplist.SkipListMapI // key ([]byte) to uint64 value file offset
	v0DataReader  rProto.ReadAtI
	dataReader    recordio.ReadAtI
	metaData      *proto.MetaData
	miscClosers   []recordio.CloseableI
}

func (reader *SSTableReader) Contains(key []byte) bool {
	// short-cut for the bloom filter to tell whether it's not in the set (if available)
	if reader.bloomFilter != nil {
		fnvHash := fnv.New64()
		_, _ = fnvHash.Write(key)
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

	return reader.getValueAtOffset(valOffset.(uint64))
}

func (reader *SSTableReader) getValueAtOffset(valOffset uint64) ([]byte, error) {
	if reader.v0DataReader != nil {
		value := &proto.DataEntry{}
		_, err := reader.v0DataReader.ReadNextAt(value, valOffset)
		if err != nil && err != io.EOF {
			return nil, err
		}

		return value.Value, nil
	} else {
		val, err := reader.dataReader.ReadNextAt(valOffset)
		if err != nil && err != io.EOF {
			return nil, err
		}

		return val, nil
	}
}

func (reader *SSTableReader) Scan() (SSTableIteratorI, error) {
	if reader.v0DataReader != nil {
		dataReader, err := rProto.NewProtoReaderWithPath(path.Join(reader.opts.basePath, DataFileName))
		if err != nil {
			return nil, err
		}

		err = dataReader.Open()
		if err != nil {
			return nil, err
		}

		reader.miscClosers = append(reader.miscClosers, dataReader)

		it, err := reader.index.Iterator()
		if err != nil {
			return nil, err
		}
		return newV0SStableFullScanIterator(it, dataReader)
	} else {
		dataReader, err := recordio.NewFileReaderWithPath(path.Join(reader.opts.basePath, DataFileName))
		if err != nil {
			return nil, err
		}
		err = dataReader.Open()
		if err != nil {
			return nil, err
		}

		reader.miscClosers = append(reader.miscClosers, dataReader)

		it, err := reader.index.Iterator()
		if err != nil {
			return nil, err
		}
		return newSStableFullScanIterator(it, dataReader)
	}
}

func (reader *SSTableReader) ScanStartingAt(key []byte) (SSTableIteratorI, error) {
	it, err := reader.index.IteratorStartingAt(key)
	if err != nil {
		return nil, err
	}
	return &SSTableIterator{reader: reader, keyIterator: it}, nil
}

func (reader *SSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error) {
	it, err := reader.index.IteratorBetween(keyLower, keyHigher)
	if err != nil {
		return nil, err
	}
	return &SSTableIterator{reader: reader, keyIterator: it}, nil
}

func (reader *SSTableReader) Close() error {
	for _, e := range reader.miscClosers {
		err := e.Close()
		if err != nil {
			return err
		}
	}

	if reader.v0DataReader != nil {
		return reader.v0DataReader.Close()
	}

	if reader.dataReader != nil {
		return reader.dataReader.Close()
	}

	return nil
}

func (reader *SSTableReader) MetaData() *proto.MetaData {
	return reader.metaData
}

// NewSSTableReader creates a new reader. The sstable base path and comparator are mandatory:
// > sstables.NewSSTableReader(sstables.ReadBasePath("some_path"), sstables.ReadWithKeyComparator(some_comp))
func NewSSTableReader(readerOptions ...ReadOption) (SSTableReaderI, error) {

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

	metaData, err := readMetaDataIfExists(path.Join(opts.basePath, MetaFileName))
	if err != nil {
		return nil, err
	}

	reader := &SSTableReader{opts: opts, bloomFilter: filter, index: index, metaData: metaData}

	if metaData.Version == 0 {
		v0DataReader, err := rProto.NewMMapProtoReaderWithPath(path.Join(opts.basePath, DataFileName))
		if err != nil {
			return nil, err
		}

		err = v0DataReader.Open()
		if err != nil {
			return nil, err
		}

		reader.v0DataReader = v0DataReader
	} else {
		dataReader, err := recordio.NewMemoryMappedReaderWithPath(path.Join(opts.basePath, DataFileName))
		if err != nil {
			return nil, err
		}

		err = dataReader.Open()
		if err != nil {
			return nil, err
		}

		reader.dataReader = dataReader
	}

	return reader, nil
}

func readIndex(indexPath string, keyComparator skiplist.KeyComparator) (skiplist.SkipListMapI, error) {
	reader, err := rProto.NewProtoReaderWithPath(indexPath)
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

	filter, _, err := bloomfilter.ReadFile(filterPath)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func readMetaDataIfExists(metaPath string) (*proto.MetaData, error) {
	md := &proto.MetaData{}
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return md, nil
	}

	mpf, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadAll(mpf)
	if err != nil {
		return nil, err
	}

	err = pb.Unmarshal(content, md)
	if err != nil {
		return nil, err
	}

	err = mpf.Close()
	if err != nil {
		return nil, err
	}

	return md, nil
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
