package sstables

import (
	"errors"
	"fmt"
	"io"
	"os"

	"hash/crc64"
	"hash/fnv"

	"path/filepath"

	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	pb "google.golang.org/protobuf/proto"
)

type ChecksumError struct {
	checksum         uint64
	expectedChecksum uint64
}

func (e ChecksumError) Is(err error) bool {
	var checksumError ChecksumError
	ok := errors.As(err, &checksumError)
	return ok
}

func (e ChecksumError) Error() string {
	return fmt.Sprintf("Checksum mismatch: expected %x, got %x", e.expectedChecksum, e.checksum)
}

type SSTableReader struct {
	opts        *SSTableReaderOptions
	bloomFilter *bloomfilter.Filter

	// key (as []byte) to a struct containing the uint64 value file offset
	index        SortedKeyIndex
	v0DataReader rProto.ReadAtI
	dataReader   recordio.ReadAtI
	metaData     *proto.MetaData
	miscClosers  []recordio.CloseableI
}

func (reader *SSTableReader) Contains(key []byte) (bool, error) {
	// short-cut for the bloom filter to tell whether it's not in the set (if available)
	if reader.bloomFilter != nil {
		fnvHash := fnv.New64()
		_, err := fnvHash.Write(key)
		if err != nil {
			return false, err
		}

		if !reader.bloomFilter.Contains(fnvHash) {
			return false, nil
		}
	}

	// go back to the index/disk to see if the key is available
	return reader.index.Contains(key)
}

func (reader *SSTableReader) Get(key []byte) ([]byte, error) {
	iVal, err := reader.index.Get(key)
	if err != nil {
		if errors.Is(err, skiplist.NotFound) {
			return nil, NotFound
		}
		return nil, fmt.Errorf("error in sstable '%s' on getting key from index: %w", reader.opts.basePath, err)
	}

	return reader.getValueAtOffset(iVal, reader.opts.skipHashCheckOnRead)
}

func (reader *SSTableReader) getValueAtOffset(iVal IndexVal, skipHashCheck bool) (v []byte, err error) {
	if reader.v0DataReader != nil {
		value := &proto.DataEntry{}
		_, err := reader.v0DataReader.ReadNextAt(value, iVal.Offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error in sstable '%s' while getting value at offset %d: %w",
				reader.opts.basePath, iVal.Offset, err)
		}

		v = value.Value
	} else {
		v, err = reader.dataReader.ReadNextAt(iVal.Offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error in sstable '%s' while getting value at offset %d: %w",
				reader.opts.basePath, iVal.Offset, err)
		}
	}

	if skipHashCheck {
		return v, nil
	}

	valChecksum, err := checksumValue(v)
	if err != nil {
		return nil, err
	}

	if valChecksum != iVal.Checksum {
		// this mismatch could come from default values, reading older formats
		if iVal.Checksum == 0 {
			return v, nil
		}

		return v, fmt.Errorf("error in sstable '%s' while hashing value at offset [%d]: %w",
			reader.opts.basePath, iVal.Offset, ChecksumError{valChecksum, iVal.Checksum})
	}

	return v, nil
}

func (reader *SSTableReader) Scan() (SSTableIteratorI, error) {
	if reader.v0DataReader != nil {
		dataReader, err := rProto.NewReader(rProto.ReaderPath(filepath.Join(reader.opts.basePath, DataFileName)))
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
		dataReader, err := recordio.NewFileReader(
			recordio.ReaderPath(filepath.Join(reader.opts.basePath, DataFileName)),
			recordio.ReaderBufferSizeBytes(reader.opts.readBufferSizeBytes),
		)
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
		return newSStableFullScanIterator(it, dataReader, reader.opts.skipHashCheckOnRead)
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

func (reader *SSTableReader) Close() (err error) {
	for _, e := range reader.miscClosers {
		err = errors.Join(err, e.Close())
	}

	if reader.v0DataReader != nil {
		err = errors.Join(err, reader.v0DataReader.Close())
	}

	if reader.dataReader != nil {
		err = errors.Join(err, reader.dataReader.Close())
	}

	if reader.index != nil {
		err = errors.Join(err, reader.index.Close())
	}

	return err
}

func (reader *SSTableReader) MetaData() *proto.MetaData {
	return reader.metaData
}

func (reader *SSTableReader) BasePath() string {
	return reader.opts.basePath
}

func (reader *SSTableReader) validateDataFile() error {
	// v0 won't have the hashes, we can skip right away
	if reader.v0DataReader != nil {
		return nil
	}

	if reader.opts.skipHashCheckOnLoad {
		return nil
	}

	iterator, err := reader.index.Iterator()
	if err != nil {
		return err
	}

	for {
		k, iv, err := iterator.Next()
		if err != nil {
			if errors.Is(err, skiplist.Done) {
				break
			}

			return fmt.Errorf("validateDataFile error iterating sstable '%s' at key [%v]: %w",
				reader.opts.basePath, k, err)
		}

		if _, err := reader.getValueAtOffset(iv, false); err != nil {
			return fmt.Errorf("validateDataFile error loading value '%s' at key [%v]: %w",
				reader.opts.basePath, k, err)
		}
	}

	return nil
}

func checksumValue(value []byte) (uint64, error) {
	crc := crc64.New(crc64.MakeTable(crc64.ISO))
	_, err := crc.Write(value)
	if err != nil {
		return 0, err
	}

	return crc.Sum64(), nil
}

// NewSSTableReader creates a new reader. The sstable base path is mandatory:
// > sstables.NewSSTableReader(sstables.ReadBasePath("some_path"))
// This function will check hashes and validity of the datafile matching the index file.
func NewSSTableReader(readerOptions ...ReadOption) (SSTableReaderI, error) {
	opts := &SSTableReaderOptions{
		basePath: "",
		// by default, we validate the integrity on loading and never checking when reading.
		// Other use cases might want to rather check the integrity at runtime while reading key / value pairs.
		skipHashCheckOnLoad: false,
		skipHashCheckOnRead: true,
		readBufferSizeBytes: 4 * 1024 * 1024,
	}

	for _, readOption := range readerOptions {
		readOption(opts)
	}

	if opts.basePath == "" {
		return nil, errors.New("SSTableReader: basePath was not supplied")
	}

	if opts.keyComparator == nil {
		opts.keyComparator = skiplist.BytesComparator{}
	}

	if opts.indexLoader == nil {
		opts.indexLoader = &SkipListIndexLoader{
			KeyComparator:  opts.keyComparator,
			ReadBufferSize: opts.readBufferSizeBytes,
		}
	}

	metaData, err := readMetaDataIfExists(filepath.Join(opts.basePath, MetaFileName))
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata of sstable in '%s': %w", opts.basePath, err)
	}

	index, err := opts.indexLoader.Load(filepath.Join(opts.basePath, IndexFileName), metaData)
	if err != nil {
		return nil, fmt.Errorf("error while reading index of sstable in '%s': %w", opts.basePath, err)
	}

	err = index.Open()
	if err != nil {
		return nil, fmt.Errorf("error while opening index of sstable in '%s': %w", opts.basePath, err)
	}

	filter, err := readFilterIfExists(filepath.Join(opts.basePath, BloomFileName))
	if err != nil {
		return nil, fmt.Errorf("error while reading filter of sstable in '%s': %w", opts.basePath, err)
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

	err = reader.validateDataFile()
	if err != nil {
		if reader.v0DataReader != nil {
			err = errors.Join(err, reader.v0DataReader.Close())
		}
		if reader.dataReader != nil {
			err = errors.Join(err, reader.dataReader.Close())
		}
		return nil, err
	}

	return reader, nil
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

func readMetaDataIfExists(metaPath string) (md *proto.MetaData, err error) {
	md = &proto.MetaData{}

	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return md, nil
	}

	mpf, err := os.Open(metaPath)
	if err != nil {
		return nil, fmt.Errorf("error while opening metadata in '%s': %w", metaPath, err)
	}

	defer func() {
		err = errors.Join(err, mpf.Close())
	}()

	content, err := io.ReadAll(mpf)
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata in '%s': %w", metaPath, err)
	}

	err = pb.Unmarshal(content, md)
	if err != nil {
		return nil, fmt.Errorf("error while parsing metadata in '%s': %w", metaPath, err)
	}

	return
}

// options

// SSTableReaderOptions contains both read/write options
type SSTableReaderOptions struct {
	basePath            string
	readBufferSizeBytes int
	indexLoader         IndexLoader

	// TODO(thomas): this is a special case of the skiplist index, which could go into the loader implementation
	keyComparator skiplist.Comparator[[]byte]

	skipHashCheckOnLoad bool
	skipHashCheckOnRead bool
}

type ReadOption func(*SSTableReaderOptions)

func ReadBasePath(p string) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.basePath = p
	}
}

// ReadWithKeyComparator sets a custom comparator for the index, defaults to skiplist.BytesComparator
func ReadWithKeyComparator(cmp skiplist.Comparator[[]byte]) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.keyComparator = cmp
	}
}

// SkipHashCheckOnLoad will not check hashes against data read from the datafile when loading.
func SkipHashCheckOnLoad() ReadOption {
	return func(args *SSTableReaderOptions) {
		args.skipHashCheckOnLoad = true
	}
}

// EnableHashCheckOnReads will check data integrity everywhere the value is retrieved, e.g. when getting and scanning.
// This is off by default, in favor of checking the data integrity during load time.
func EnableHashCheckOnReads() ReadOption {
	return func(args *SSTableReaderOptions) {
		args.skipHashCheckOnRead = false
	}
}

func ReadBufferSizeBytes(size int) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.readBufferSizeBytes = size
	}
}

// ReadIndexLoader allows to create a customized index from an index file.
func ReadIndexLoader(il IndexLoader) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.indexLoader = il
	}
}
