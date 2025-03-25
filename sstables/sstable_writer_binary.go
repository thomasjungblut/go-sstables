package sstables

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"

	"github.com/steakknife/bloomfilter"
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/skiplist"
	sProto "github.com/thomasjungblut/go-sstables/sstables/proto"
	"google.golang.org/protobuf/proto"
)

type FastIndexEntry struct {
	Key         []byte
	ValueOffset uint64
	Checksum    uint64 // a golang crc-64 checksum of the respective dataEntry
	keylen      uint64
}

func (f *FastIndexEntry) marshal(buf *bytes.Buffer) error {
	buf.Reset()
	f.keylen = uint64(len(f.Key))
	err := binary.Write(buf, binary.LittleEndian, f.keylen)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, f.Key)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, f.ValueOffset)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, f.Checksum)
	if err != nil {
		return err
	}
	return err
}

func (f *FastIndexEntry) unmarshal(reader io.Reader) error {

	err := binary.Read(reader, binary.LittleEndian, &f.keylen)
	if err != nil {
		return err
	}
	if len(f.Key) != int(f.keylen) {
		f.Key = make([]byte, f.keylen)
	}
	err = binary.Read(reader, binary.LittleEndian, &f.Key)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &f.ValueOffset)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &f.Checksum)
	if err != nil {
		return err
	}
	return err
}

type SSTableStreamWriterBinary struct {
	opts *SSTableWriterOptions

	indexFilePath string
	dataFilePath  string
	metaFilePath  string

	indexWriter    *os.File
	indexBufWriter *bufio.Writer
	dataWriter     *os.File
	dataBufWriter  *bufio.Writer
	metaDataFile   *os.File

	bloomFilter *bloomfilter.Filter
	metaData    *sProto.MetaData
	indexRecord FastIndexEntry
	buf         *bytes.Buffer
	offset      uint64
	lastKey     []byte
	size        uint64
}

func (writer *SSTableStreamWriterBinary) Open() error {
	writer.indexFilePath = filepath.Join(writer.opts.basePath, IndexFileName)
	iWriter, err := os.OpenFile(writer.indexFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("error while creating index writer in '%s': %w", writer.opts.basePath, err)
	}
	writer.indexWriter = iWriter
	writer.indexBufWriter = bufio.NewWriterSize(iWriter, writer.opts.writeBufferSizeBytes)

	writer.buf = bytes.NewBuffer(nil)
	writer.dataFilePath = filepath.Join(writer.opts.basePath, DataFileName)
	dWriter, err := os.OpenFile(writer.dataFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("error while creating data writer in '%s': %w", writer.opts.basePath, err)
	}

	writer.dataWriter = dWriter
	writer.dataBufWriter = bufio.NewWriterSize(dWriter, writer.opts.writeBufferSizeBytes)

	writer.metaFilePath = filepath.Join(writer.opts.basePath, MetaFileName)
	metaFile, err := os.OpenFile(writer.metaFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("error while opening metadata file in '%s': %w", writer.opts.basePath, err)
	}
	writer.metaDataFile = metaFile
	writer.metaData = &sProto.MetaData{
		Version: Version,
	}
	writer.offset = 0
	if writer.opts.enableBloomFilter {
		bf, err := bloomfilter.NewOptimal(writer.opts.bloomExpectedNumberOfElements, writer.opts.bloomFpProbability)
		if err != nil {
			return fmt.Errorf("error while creating bloomfilter in '%s': %w", writer.opts.basePath, err)
		}
		writer.bloomFilter = bf
	}

	return nil
}

type SSTableSimpleWriterBinary struct {
	streamWriter *SSTableStreamWriterBinary
}

func newTestSSTableSimpleWriterBinary() (*SSTableSimpleWriterBinary, error) {
	tmpDir, err := os.MkdirTemp("", "sstables_Writer")
	if err != nil {
		return nil, err
	}

	return NewSSTableSimpleWriterBinary(WriteBasePath(tmpDir), WithKeyComparator(skiplist.BytesComparator{}))
}

func NewSSTableSimpleWriterBinary(writerOptions ...WriterOption) (*SSTableSimpleWriterBinary, error) {
	writerOptions = append(writerOptions, WriteBufferSizeBytes(4096))
	writer, err := NewSSTableStreamWriterBinary(writerOptions...)
	if err != nil {
		return nil, err
	}
	return &SSTableSimpleWriterBinary{streamWriter: writer}, nil
}

func (writer *SSTableSimpleWriterBinary) WriteSkipListMap(skipListMap skiplist.MapI[[]byte, []byte]) (err error) {
	err = writer.streamWriter.Open()
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, writer.streamWriter.Close())
	}()

	it, _ := skipListMap.Iterator()
	for {
		k, v, err := it.Next()
		if errors.Is(err, skiplist.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("error in getting next skiplist record in '%s': %w", writer.streamWriter.opts.basePath, err)
		}

		err = writer.streamWriter.WriteNext(k, v)
		if err != nil {
			return fmt.Errorf("error in writing skiplist record in '%s': %w", writer.streamWriter.opts.basePath, err)
		}
	}
	return nil
}

func (writer *SSTableStreamWriterBinary) WriteNext(key []byte, value []byte) error {
	if writer.lastKey != nil {
		cmpResult := writer.opts.keyComparator.Compare(writer.lastKey, key)
		if cmpResult == 0 {
			return fmt.Errorf("sstables.WriteNext '%s': the same key cannot be written more than once", writer.opts.basePath)
		} else if cmpResult > 0 {
			return fmt.Errorf("sstables.WriteNext '%s': non-ascending key cannot be written", writer.opts.basePath)
		}

		// the size of the key may be variable, that's why we might allocate a new buffer for the last key
		if len(writer.lastKey) != len(key) {
			writer.lastKey = make([]byte, len(key))
		}
	} else {
		if writer.metaData == nil {
			return fmt.Errorf("sstables.writeNext '%s': no metadata available to write into, table might not be opened yet", writer.opts.basePath)
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

	crc := crc64.New(crc64.MakeTable(crc64.ISO))
	_, err := crc.Write(value)
	if err != nil {
		return fmt.Errorf("error while writing crc64 hash in '%s': %w", writer.opts.basePath, err)
	}

	recordOffset, err := writer.dataWriter.Seek(0, 1)
	if err != nil {
		return fmt.Errorf("error writeNext data writer error in '%s': %w", writer.opts.basePath, err)
	}
	writer.buf.Reset()
	writer.size = uint64(len(value))
	err = binary.Write(writer.buf, binary.LittleEndian, writer.size)
	if err != nil {
		return fmt.Errorf("error writeNext data writer error in '%s': %w", writer.opts.basePath, err)
	}

	err = binary.Write(writer.buf, binary.LittleEndian, value)
	if err != nil {
		return fmt.Errorf("error writeNext data writer error in '%s': %w", writer.opts.basePath, err)
	}
	nn, err := writer.dataBufWriter.Write(writer.buf.Bytes())
	if err != nil {
		return fmt.Errorf("error writeNext data writer error in '%s': %w", writer.opts.basePath, err)
	}
	writer.offset += uint64(nn)
	writer.indexRecord.Key = key
	writer.indexRecord.ValueOffset = writer.offset
	writer.indexRecord.Checksum = crc.Sum64()
	err = writer.indexRecord.marshal(writer.buf)
	if err != nil {
		return err
	}

	_, err = writer.indexBufWriter.Write(writer.buf.Bytes())
	if err != nil {
		// in case of failures we need to try to rewind the data writer's offset to preWriteOffset
		_, seekErr := writer.dataWriter.Seek(recordOffset, 0)
		return fmt.Errorf("error writeNext index writer/seeker error in '%s': %w", writer.opts.basePath, errors.Join(err, seekErr))
	}

	writer.metaData.NumRecords += 1
	if value == nil {
		writer.metaData.NullValues += 1
	}

	return nil
}

func (writer *SSTableStreamWriterBinary) Close() (err error) {
	writer.indexBufWriter.Flush()
	writer.dataBufWriter.Flush()
	err = errors.Join(writer.indexWriter.Close(), writer.dataWriter.Close())

	if writer.opts.enableBloomFilter && writer.bloomFilter != nil {
		_, bErr := writer.bloomFilter.WriteFile(filepath.Join(writer.opts.basePath, BloomFileName))
		if bErr != nil {
			err = errors.Join(err, fmt.Errorf("error in writing bloom filter  in '%s': %w", writer.opts.basePath, bErr))
		}
	}

	if writer.metaData != nil && writer.metaDataFile != nil {
		defer func() {
			err = errors.Join(err, writer.metaDataFile.Close())
		}()

		writer.metaData.MaxKey = writer.lastKey
		writer.metaData.DataBytes = 0  // writer.dataWriter.Size()
		writer.metaData.IndexBytes = 0 // writer.indexWriter.Size()
		writer.metaData.TotalBytes = writer.metaData.DataBytes + writer.metaData.IndexBytes
		bytes, mErr := proto.Marshal(writer.metaData)
		if mErr != nil {
			return errors.Join(err, fmt.Errorf("error in serializing metadata in '%s': %w", writer.opts.basePath, mErr))
		}

		_, wErr := writer.metaDataFile.Write(bytes)
		if wErr != nil {
			return errors.Join(err, fmt.Errorf("error in writing metadata in '%s': %w", writer.opts.basePath, wErr))
		}
	}

	return err
}

// NewSSTableStreamWriterBinary creates a new streamed writer, the minimum options required are the base path and the comparator:
// > sstables.NewSSTableStreamWriterBinary(sstables.WriteBasePath("some_existing_folder"), sstables.WithKeyComparator(some_comparator))
func NewSSTableStreamWriterBinary(writerOptions ...WriterOption) (*SSTableStreamWriterBinary, error) {
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

	return &SSTableStreamWriterBinary{opts: opts}, nil
}
