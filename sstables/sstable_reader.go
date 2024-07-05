package sstables

import (
	"errors"
	"fmt"
	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	pb "google.golang.org/protobuf/proto"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
)

type SSTableReader struct {
	opts          *SSTableReaderOptions
	bloomFilter   *bloomfilter.Filter
	keyComparator skiplist.Comparator[[]byte]
	index         skiplist.MapI[[]byte, uint64] // key (as []byte) to uint64 value file offset
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
	if errors.Is(err, skiplist.NotFound) {
		return nil, NotFound
	}

	return reader.getValueAtOffset(valOffset)
}

func (reader *SSTableReader) getValueAtOffset(valOffset uint64) ([]byte, error) {
	if reader.v0DataReader != nil {
		value := &proto.DataEntry{}
		_, err := reader.v0DataReader.ReadNextAt(value, valOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error in sstable '%s' while getting value at offset %d: %w", reader.opts.basePath, valOffset, err)
		}

		return value.Value, nil
	} else {
		val, err := reader.dataReader.ReadNextAt(valOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error in sstable '%s' while getting value at offset %d: %w", reader.opts.basePath, valOffset, err)
		}

		return val, nil
	}
}

func (reader *SSTableReader) Scan() (SSTableIteratorI, error) {
	if reader.v0DataReader != nil {
		dataReader, err := rProto.NewProtoReaderWithPath(filepath.Join(reader.opts.basePath, DataFileName))
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while creating a scanner: %w", reader.opts.basePath, err)
		}

		err = dataReader.Open()
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while opening a scanner: %w", reader.opts.basePath, err)
		}

		reader.miscClosers = append(reader.miscClosers, dataReader)

		it, err := reader.index.Iterator()
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while creating a scanner iterator: %w", reader.opts.basePath, err)
		}
		return newV0SStableFullScanIterator(it, dataReader)
	} else {
		dataReader, err := recordio.NewFileReaderWithPath(filepath.Join(reader.opts.basePath, DataFileName))
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while creating a scanner: %w", reader.opts.basePath, err)
		}
		err = dataReader.Open()
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while opening a scanner: %w", reader.opts.basePath, err)
		}

		reader.miscClosers = append(reader.miscClosers, dataReader)

		it, err := reader.index.Iterator()
		if err != nil {
			return nil, fmt.Errorf("error in sstable '%s' while creating a scanner iterator: %w", reader.opts.basePath, err)
		}
		return newSStableFullScanIterator(it, dataReader)
	}
}

func (reader *SSTableReader) ScanStartingAt(key []byte) (SSTableIteratorI, error) {
	it, err := reader.index.IteratorStartingAt(key)
	if err != nil {
		return nil, fmt.Errorf("error in sstable '%s' in ScanStartingAt: %w", reader.opts.basePath, err)
	}
	return &SSTableIterator{reader: reader, keyIterator: it}, nil
}

func (reader *SSTableReader) ScanRange(keyLower []byte, keyHigher []byte) (SSTableIteratorI, error) {
	it, err := reader.index.IteratorBetween(keyLower, keyHigher)
	if err != nil {
		return nil, fmt.Errorf("error in sstable '%s' in ScanRange: %w", reader.opts.basePath, err)
	}
	return &SSTableIterator{reader: reader, keyIterator: it}, nil
}

func (reader *SSTableReader) Close() error {
	for _, e := range reader.miscClosers {
		err := e.Close()
		if err != nil {
			return fmt.Errorf("error in sstable '%s' while closing miscClosers: %w", reader.opts.basePath, err)
		}
	}

	if reader.v0DataReader != nil {
		err := reader.v0DataReader.Close()
		if err != nil {
			return fmt.Errorf("error in sstable '%s' while closing dataReader: %w", reader.opts.basePath, err)
		}
	}

	if reader.dataReader != nil {
		err := reader.dataReader.Close()
		if err != nil {
			return fmt.Errorf("error in sstable '%s' while closing dataReader: %w", reader.opts.basePath, err)
		}
	}

	return nil
}

func (reader *SSTableReader) MetaData() *proto.MetaData {
	return reader.metaData
}

func (reader *SSTableReader) BasePath() string {
	return reader.opts.basePath
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
		return nil, errors.New("SSTableReader: basePath was not supplied")
	}

	if opts.keyComparator == nil {
		return nil, errors.New("SSTableReader: no key comparator supplied")
	}

	index, err := readIndex(filepath.Join(opts.basePath, IndexFileName), opts.keyComparator)
	if err != nil {
		return nil, fmt.Errorf("error while reading index of sstable in '%s': %w", opts.basePath, err)
	}

	filter, err := readFilterIfExists(filepath.Join(opts.basePath, BloomFileName))
	if err != nil {
		return nil, fmt.Errorf("error while reading filter of sstable in '%s': %w", opts.basePath, err)
	}

	metaData, err := readMetaDataIfExists(filepath.Join(opts.basePath, MetaFileName))
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata of sstable in '%s': %w", opts.basePath, err)
	}

	reader := &SSTableReader{opts: opts, bloomFilter: filter, index: index, metaData: metaData}

	if metaData.Version == 0 {
		v0DataReader, err := rProto.NewMMapProtoReaderWithPath(filepath.Join(opts.basePath, DataFileName))
		if err != nil {
			return nil, fmt.Errorf("error while creating proto data reader of sstable in '%s': %w", opts.basePath, err)
		}

		err = v0DataReader.Open()
		if err != nil {
			return nil, fmt.Errorf("error while opening proto data reader of sstable in '%s': %w", opts.basePath, err)
		}

		reader.v0DataReader = v0DataReader
	} else {
		dataReader, err := recordio.NewMemoryMappedReaderWithPath(filepath.Join(opts.basePath, DataFileName))
		if err != nil {
			return nil, fmt.Errorf("error while creating data reader of sstable in '%s': %w", opts.basePath, err)
		}

		err = dataReader.Open()
		if err != nil {
			return nil, fmt.Errorf("error while opening data reader of sstable in '%s': %w", opts.basePath, err)
		}

		reader.dataReader = dataReader
	}

	return reader, nil
}

func readIndex(indexPath string, keyComparator skiplist.Comparator[[]byte]) (skiplist.MapI[[]byte, uint64], error) {
	reader, err := rProto.NewProtoReaderWithPath(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error while creating index reader of sstable in '%s': %w", indexPath, err)
	}

	err = reader.Open()
	if err != nil {
		return nil, fmt.Errorf("error while opening index reader of sstable in '%s': %w", indexPath, err)
	}

	indexMap := skiplist.NewSkipListMap[[]byte, uint64](keyComparator)

	for {
		record := &proto.IndexEntry{}
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		indexMap.Insert(record.Key, record.ValueOffset)
	}

	err = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("error while closing index reader of sstable in '%s': %w", indexPath, err)
	}

	return indexMap, nil
}

func readFilterIfExists(filterPath string) (*bloomfilter.Filter, error) {
	if _, err := os.Stat(filterPath); os.IsNotExist(err) {
		return nil, nil
	}

	filter, _, err := bloomfilter.ReadFile(filterPath)
	if err != nil {
		return nil, fmt.Errorf("error while reading bloom filterin '%s': %w", filterPath, err)
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
		return nil, fmt.Errorf("error while opening metadata in '%s': %w", metaPath, err)
	}

	content, err := io.ReadAll(mpf)
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata in '%s': %w", metaPath, err)
	}

	err = pb.Unmarshal(content, md)
	if err != nil {
		return nil, fmt.Errorf("error while parsing metadata in '%s': %w", metaPath, err)
	}

	err = mpf.Close()
	if err != nil {
		return nil, fmt.Errorf("error while closing metadata in '%s': %w", metaPath, err)
	}

	return md, nil
}

// options

// SSTableReaderOptions contains both read/write options
type SSTableReaderOptions struct {
	basePath      string
	keyComparator skiplist.Comparator[[]byte]
}

type ReadOption func(*SSTableReaderOptions)

func ReadBasePath(p string) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.basePath = p
	}
}

func ReadWithKeyComparator(cmp skiplist.Comparator[[]byte]) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.keyComparator = cmp
	}
}
