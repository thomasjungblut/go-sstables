package sstables

import (
	"errors"
	"fmt"
	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	sProto "github.com/thomasjungblut/go-sstables/sstables/proto"
	proto "google.golang.org/protobuf/proto"
	"hash/fnv"
	"os"
	"path"
)

type SSTableStreamWriter struct {
	opts *SSTableWriterOptions

	indexFilePath string
	dataFilePath  string
	metaFilePath  string

	indexWriter  *rProto.Writer
	dataWriter   *recordio.FileWriter
	metaDataFile *os.File

	bloomFilter *bloomfilter.Filter
	metaData    *sProto.MetaData

	lastKey []byte
}

func (writer *SSTableStreamWriter) Open() error {
	writer.indexFilePath = path.Join(writer.opts.basePath, IndexFileName)
	iWriter, err := rProto.NewWriter(
		rProto.Path(writer.indexFilePath),
		rProto.CompressionType(writer.opts.indexCompressionType),
		rProto.WriteBufferSizeBytes(writer.opts.writeBufferSizeBytes))
	if err != nil {
		return err
	}
	writer.indexWriter = iWriter

	err = writer.indexWriter.Open()
	if err != nil {
		return err
	}

	writer.dataFilePath = path.Join(writer.opts.basePath, DataFileName)
	dWriter, err := recordio.NewFileWriter(
		recordio.Path(writer.dataFilePath),
		recordio.CompressionType(writer.opts.dataCompressionType),
		recordio.BufferSizeBytes(writer.opts.writeBufferSizeBytes))
	if err != nil {
		return err
	}

	writer.dataWriter = dWriter
	err = writer.dataWriter.Open()
	if err != nil {
		return err
	}

	writer.metaFilePath = path.Join(writer.opts.basePath, MetaFileName)
	metaFile, err := os.OpenFile(writer.metaFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	writer.metaDataFile = metaFile
	writer.metaData = &sProto.MetaData{
		Version: Version,
	}

	if writer.opts.enableBloomFilter {
		bf, err := bloomfilter.NewOptimal(writer.opts.bloomExpectedNumberOfElements, writer.opts.bloomFpProbability)
		if err != nil {
			return err
		}
		writer.bloomFilter = bf
	}

	return nil
}

func (writer *SSTableStreamWriter) WriteNext(key []byte, value []byte) error {
	if writer.lastKey != nil {
		cmpResult := writer.opts.keyComparator(writer.lastKey, key)
		if cmpResult == 0 {
			return errors.New("the same key cannot be written more than once")
		} else if cmpResult > 0 {
			return errors.New("non-ascending key cannot be written")
		}

		// the size of the key may be variable, that's why we might allocate a new buffer for the last key
		if len(writer.lastKey) != len(key) {
			writer.lastKey = make([]byte, len(key))
		}
	} else {
		if writer.metaData == nil {
			return errors.New("no metadata available to write into, did you Open the writer already?")
		}

		writer.metaData.MinKey = make([]byte, len(key))
		writer.lastKey = make([]byte, len(key))
		copy(writer.metaData.MinKey, key)
	}

	copy(writer.lastKey, key)

	if writer.opts.enableBloomFilter {
		fnvHash := fnv.New64()
		_, _ = fnvHash.Write(key)
		writer.bloomFilter.Add(fnvHash)
	}

	recordOffset, err := writer.dataWriter.Write(value)
	if err != nil {
		return err
	}

	_, err = writer.indexWriter.Write(&sProto.IndexEntry{Key: key, ValueOffset: recordOffset})
	if err != nil {
		return err
	}

	writer.metaData.NumRecords += 1

	return nil
}

func (writer *SSTableStreamWriter) Close() error {
	err := writer.indexWriter.Close()
	if err != nil {
		return err
	}

	err = writer.dataWriter.Close()
	if err != nil {
		return err
	}

	if writer.opts.enableBloomFilter && writer.bloomFilter != nil {
		_, err := writer.bloomFilter.WriteFile(path.Join(writer.opts.basePath, BloomFileName))
		if err != nil {
			return err
		}
	}

	if writer.metaData != nil {
		writer.metaData.MaxKey = writer.lastKey
		writer.metaData.DataBytes = writer.dataWriter.Size()
		writer.metaData.IndexBytes = writer.indexWriter.Size()
		writer.metaData.TotalBytes = writer.metaData.DataBytes + writer.metaData.IndexBytes
		bytes, err := proto.Marshal(writer.metaData)
		if err != nil {
			return err
		}

		_, err = writer.metaDataFile.Write(bytes)
		if err != nil {
			return err
		}

		err = writer.metaDataFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type SSTableSimpleWriter struct {
	streamWriter *SSTableStreamWriter
}

func (writer *SSTableSimpleWriter) WriteSkipListMap(skipListMap *skiplist.SkipListMap) error {
	err := writer.streamWriter.Open()
	if err != nil {
		return err
	}

	it, _ := skipListMap.Iterator()
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		if err != nil {
			return err
		}

		kBytes, ok := k.([]byte)
		if !ok {
			return errors.New("key is not of type []byte")
		}

		vBytes, ok := v.([]byte)
		if !ok {
			return errors.New("value is not of type []byte")
		}

		err = writer.streamWriter.WriteNext(kBytes, vBytes)
		if err != nil {
			return err
		}
	}

	err = writer.streamWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

// NewSSTableStreamWriter creates a new streamed writer, the minimum options required are the base path and the comparator:
// > sstables.NewSSTableStreamWriter(sstables.WriteBasePath("some_existing_folder"), sstables.WithKeyComparator(some_comparator))
func NewSSTableStreamWriter(writerOptions ...WriterOption) (*SSTableStreamWriter, error) {
	opts := &SSTableWriterOptions{
		basePath:                      "",
		enableBloomFilter:             true,
		indexCompressionType:          recordio.CompressionTypeNone,
		dataCompressionType:           recordio.CompressionTypeSnappy,
		bloomFpProbability:            0.01,
		bloomExpectedNumberOfElements: 1000,
		writeBufferSizeBytes:          1024 * 1024 * 4,
		keyComparator:                 nil,
	}

	for _, writeOption := range writerOptions {
		writeOption(opts)
	}

	if opts.basePath == "" {
		return nil, errors.New("basePath was not supplied")
	}

	if opts.keyComparator == nil {
		return nil, errors.New("no key comparator supplied")
	}

	if opts.bloomExpectedNumberOfElements <= 0 {
		return nil, fmt.Errorf("unexpected number of bloom filter elements, was: %d",
			opts.bloomExpectedNumberOfElements)
	}

	return &SSTableStreamWriter{opts: opts}, nil
}

func NewSSTableSimpleWriter(writerOptions ...WriterOption) (*SSTableSimpleWriter, error) {
	writerOptions = append(writerOptions, WriteBufferSizeBytes(4096))
	writer, err := NewSSTableStreamWriter(writerOptions...)
	if err != nil {
		return nil, err
	}
	return &SSTableSimpleWriter{streamWriter: writer}, nil
}

// options

type SSTableWriterOptions struct {
	basePath                      string
	indexCompressionType          int
	dataCompressionType           int
	enableBloomFilter             bool
	bloomExpectedNumberOfElements uint64
	bloomFpProbability            float64
	writeBufferSizeBytes          int
	keyComparator                 skiplist.KeyComparator
}

type WriterOption func(*SSTableWriterOptions)

func WriteBasePath(p string) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.basePath = p
	}
}

func IndexCompressionType(p int) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.indexCompressionType = p
	}
}

func DataCompressionType(p int) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.dataCompressionType = p
	}
}

func EnableBloomFilter() WriterOption {
	return func(args *SSTableWriterOptions) {
		args.enableBloomFilter = true
	}
}

func BloomExpectedNumberOfElements(n uint64) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.bloomExpectedNumberOfElements = n
	}
}

func BloomFalsePositiveProbability(fpProbability float64) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.bloomFpProbability = fpProbability
	}
}

func WriteBufferSizeBytes(bufSizeBytes int) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.writeBufferSizeBytes = bufSizeBytes
	}
}

func WithKeyComparator(cmp skiplist.KeyComparator) WriterOption {
	return func(args *SSTableWriterOptions) {
		args.keyComparator = cmp
	}
}
