package sstables

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	"path"
	"errors"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/steakknife/bloomfilter"
	"hash/fnv"
)

type SSTableStreamWriter struct {
	opts *SSTableWriterOptions

	indexFilePath string
	dataFilePath  string

	indexWriter *recordio.ProtoWriter
	dataWriter  *recordio.ProtoWriter

	bloomFilter *bloomfilter.Filter
}

func (writer *SSTableStreamWriter) Open() error {
	if writer.opts.basePath == "" {
		return errors.New("basePath was not supplied")
	}

	writer.indexFilePath = path.Join(writer.opts.basePath, IndexFileName)
	iWriter, err := recordio.NewCompressedProtoWriterWithPath(writer.indexFilePath, writer.opts.indexCompressionType)
	if err != nil {
		return err
	}
	writer.indexWriter = iWriter

	err = writer.indexWriter.Open()
	if err != nil {
		return err
	}

	writer.dataFilePath = path.Join(writer.opts.basePath, DataFileName)
	dWriter, err := recordio.NewCompressedProtoWriterWithPath(writer.dataFilePath, writer.opts.dataCompressionType)
	if err != nil {
		return err
	}

	writer.dataWriter = dWriter
	err = writer.dataWriter.Open()
	if err != nil {
		return err
	}

	if writer.opts.enableBloomFilter {
		writer.bloomFilter = bloomfilter.NewOptimal(writer.opts.bloomExpectedNumberOfElements, writer.opts.bloomFpProbability)
	}

	return nil
}

func (writer *SSTableStreamWriter) WriteNext(key []byte, value []byte) error {
	// TODO(thomas): for safety we need to check keys are actually ascending
	// for that we would need a comparator as well

	if writer.opts.enableBloomFilter {
		fnvHash := fnv.New64()
		_, err := fnvHash.Write(key)
		if err != nil {
			return err
		}

		writer.bloomFilter.Add(fnvHash)
	}
	recordOffset, err := writer.dataWriter.Write(&proto.DataEntry{Value: value})
	if err != nil {
		return err
	}

	_, err = writer.indexWriter.Write(&proto.IndexEntry{Key: key, ValueOffset: recordOffset})
	if err != nil {
		return err
	}

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

	if writer.opts.enableBloomFilter {
		err := writer.bloomFilter.WriteFile(path.Join(writer.opts.basePath, BloomFileName))
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

	it := skipListMap.Iterator()
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

		writer.streamWriter.WriteNext(kBytes, vBytes)
	}

	err = writer.streamWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func NewSSTableStreamWriter(writerOptions ...WriterOption) *SSTableStreamWriter {
	opts := &SSTableWriterOptions{
		basePath:                      "",
		enableBloomFilter:             true,
		indexCompressionType:          recordio.CompressionTypeNone,
		dataCompressionType:           recordio.CompressionTypeSnappy,
		bloomFpProbability:            0.01,
		bloomExpectedNumberOfElements: 1000,
	}

	for _, writeOption := range writerOptions {
		writeOption(opts)
	}

	return &SSTableStreamWriter{opts: opts}
}

func NewSSTableSimpleWriter(writerOptions ...WriterOption) *SSTableSimpleWriter {
	return &SSTableSimpleWriter{streamWriter: NewSSTableStreamWriter(writerOptions...)}
}

// options

type SSTableWriterOptions struct {
	basePath                      string
	indexCompressionType          int
	dataCompressionType           int
	enableBloomFilter             bool
	bloomExpectedNumberOfElements uint64
	bloomFpProbability            float64
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
