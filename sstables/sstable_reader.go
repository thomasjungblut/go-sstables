package sstables

import (
	"errors"
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"

	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	pb "google.golang.org/protobuf/proto"
)

var ChecksumErr = ChecksumError{}

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
	iVal, err := reader.index.Get(key)
	if errors.Is(err, skiplist.NotFound) {
		return nil, NotFound
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

	skippedRecords := uint64(0)
	// TODO(thomas): how do we deal with different implementations of the interface here?
	// we could instead create a dedicated map for keys that are known to be corrupt and create a filtering on top
	indexReplacement := skiplist.NewSkipListMap[[]byte, IndexVal](reader.opts.keyComparator)
	for {
		k, iv, err := iterator.Next()
		if err != nil {
			if errors.Is(err, skiplist.Done) {
				break
			}

			return err
		}

		if _, err := reader.getValueAtOffset(iv, false); err != nil {
			if errors.Is(err, ChecksumErr) && reader.opts.skipInvalidHashesOnLoad {
				skippedRecords++
				continue
			}
			return fmt.Errorf("error loading sstable '%s' at key [%v]: %w",
				reader.opts.basePath, k, err)
		}

		if reader.opts.skipInvalidHashesOnLoad {
			indexReplacement.Insert(k, iv)
		}
	}

	if reader.opts.skipInvalidHashesOnLoad {
		reader.metaData.SkippedRecords = skippedRecords
		reader.index = indexReplacement
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
		skipInvalidHashesOnLoad: false,
		skipHashCheckOnLoad:     false,
		skipHashCheckOnRead:     true,
		readBufferSizeBytes:     4 * 1024 * 1024,
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

	index, err := opts.indexLoader.Load(filepath.Join(opts.basePath, IndexFileName))
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

	// TODO(thomas): this is a special case of the skiplist index, which should go into the loader implementation
	keyComparator skiplist.Comparator[[]byte]

	skipInvalidHashesOnLoad bool
	skipHashCheckOnLoad     bool
	skipHashCheckOnRead     bool
}

type ReadOption func(*SSTableReaderOptions)

func ReadBasePath(p string) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.basePath = p
	}
}

// Deprecated: use a custom ReadIndexLoader and supply it to the SkipListIndexLoader
func ReadWithKeyComparator(cmp skiplist.Comparator[[]byte]) ReadOption {
	return func(args *SSTableReaderOptions) {
		args.keyComparator = cmp
	}
}

// SkipInvalidHashesOnLoad will not index key/value pairs that have a hash mismatch in them.
// The database will pretend it does not know those records.
func SkipInvalidHashesOnLoad() ReadOption {
	return func(args *SSTableReaderOptions) {
		args.skipInvalidHashesOnLoad = true
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
