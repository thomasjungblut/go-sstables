package recordio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	pool "github.com/libp2p/go-buffer-pool"

	"github.com/thomasjungblut/go-sstables/recordio/compressor"
)

// FileWriter defines a binary file format (little endian).
// The file header has a 32 bit version number and a 32 bit compression type enum according to the table above.
// Each record written in the file follows the following format (sequentially):
// - MagicNumber (encoding/binary/Uvarint) to separate records from each other.
// - single byte set to 1 if the record is supposed to be nil. Otherwise, 0.
// - Uncompressed data payload size (encoding/binary/Uvarint).
// - Compressed data payload size (encoding/binary/Uvarint), or 0 if the data is not compressed.
// - Payload as plain bytes, possibly compressed
type FileWriter struct {
	open   bool
	closed bool

	file      *os.File
	bufWriter WriteSeekerCloserFlusher
	// largestOffset tracks the largest currentOffset that was returned so far
	// this is important in scenarios when we seek back in the file, but are not writing past largestOffset
	// which causes some lingering bytes that are not full records
	largestOffset uint64
	currentOffset uint64
	headerOffset  uint64

	compressionType    int
	compressor         compressor.CompressionI
	recordHeaderCache  []byte
	bufferPool         *pool.BufferPool
	alignedBlockWrites bool
}

var DirectIOSyncWriteErr = errors.New("currently not supporting directIO with sync writing")

func (w *FileWriter) Open() error {
	if w.open {
		return fmt.Errorf("file writer for '%s' is already opened", w.file.Name())
	}

	if w.closed {
		return fmt.Errorf("file writer for '%s' is already closed", w.file.Name())
	}

	offset, err := writeFileHeader(w)
	if err != nil {
		return fmt.Errorf("writing header in file at '%s' failed with %w", w.file.Name(), err)
	}

	w.compressor, err = NewCompressorForType(w.compressionType)
	if err != nil {
		return fmt.Errorf("creating compressor with type '%d' in file at '%s' failed with %w", w.compressionType, w.file.Name(), err)
	}

	w.currentOffset = uint64(offset)
	w.largestOffset = w.currentOffset
	w.headerOffset = w.currentOffset
	w.open = true
	w.recordHeaderCache = make([]byte, RecordHeaderV3MaxSizeBytes)
	w.bufferPool = new(pool.BufferPool)

	// we flush early to get a valid file with header written, this is important in crash scenarios
	// when directIO is enabled however, we can't write misaligned blocks - thus this is not executed
	if !w.alignedBlockWrites {
		err = w.bufWriter.Flush()
		if err != nil {
			return fmt.Errorf("flushing header in file at '%s' failed with %w", w.file.Name(), err)
		}
	}

	return nil
}

func writeFileHeader(writer *FileWriter) (int, error) {
	written, err := writer.bufWriter.Write(fileHeaderAsByteSlice(uint32(writer.compressionType)))
	if err != nil {
		return 0, err
	}

	return written, nil
}

func fileHeaderAsByteSlice(compressionType uint32) []byte {
	// 4 byte version number, 4 byte compression code = 8 bytes
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes[0:4], CurrentVersion)
	binary.LittleEndian.PutUint32(bytes[4:8], compressionType)
	return bytes
}

// for legacy reference still around, main paths unused - mostly for tests writing old versions
// noinspection GoUnusedFunction
func writeRecordHeaderV1(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (int, error) {
	// 4 byte magic number, 8 byte uncompressed size, 8 bytes for compressed size = 20 bytes
	bytes := make([]byte, RecordHeaderSizeBytesV1V2)
	binary.LittleEndian.PutUint32(bytes[0:4], MagicNumberSeparator)
	binary.LittleEndian.PutUint64(bytes[4:12], payloadSizeUncompressed)
	binary.LittleEndian.PutUint64(bytes[12:20], payloadSizeCompressed)
	written, err := writer.bufWriter.Write(bytes)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func fillRecordHeaderV2(bytes []byte, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) []byte {
	off := binary.PutUvarint(bytes, MagicNumberSeparatorLong)
	off += binary.PutUvarint(bytes[off:], payloadSizeUncompressed)
	off += binary.PutUvarint(bytes[off:], payloadSizeCompressed)
	return bytes[:off]
}

// for legacy reference still around, main paths unused - mostly for tests writing old versions
// noinspection GoUnusedFunction
func writeRecordHeaderV2(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (int, error) {
	header := fillRecordHeaderV2(writer.recordHeaderCache, payloadSizeUncompressed, payloadSizeCompressed)
	written, err := writer.bufWriter.Write(header)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func fillRecordHeaderV3(bytes []byte, payloadSizeUncompressed uint64, payloadSizeCompressed uint64, recordNil bool) []byte {
	off := binary.PutUvarint(bytes, MagicNumberSeparatorLong)
	if recordNil {
		bytes[off] = 1
	} else {
		bytes[off] = 0
	}
	off += 1
	off += binary.PutUvarint(bytes[off:], payloadSizeUncompressed)
	off += binary.PutUvarint(bytes[off:], payloadSizeCompressed)

	return bytes[:off]
}

func writeRecordHeaderV3(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64, recordNil bool) (int, error) {
	header := fillRecordHeaderV3(writer.recordHeaderCache, payloadSizeUncompressed, payloadSizeCompressed, recordNil)
	written, err := writer.bufWriter.Write(header)
	if err != nil {
		return 0, err
	}

	return written, nil
}

// Write appends a record of bytes, returns the current offset this item was written to
func (w *FileWriter) Write(record []byte) (uint64, error) {
	if !w.open || w.closed {
		return 0, errors.New("writer was either not opened yet or is closed already")
	}

	recordToWrite := record
	uncompressedSize := uint64(len(recordToWrite))
	compressedSize := uint64(0)

	if w.compressor != nil {
		poolBuffer := w.bufferPool.Get(int(uncompressedSize))
		defer w.bufferPool.Put(poolBuffer)

		compressedRecord, err := w.compressor.CompressWithBuf(recordToWrite, poolBuffer)
		if err != nil {
			return 0, fmt.Errorf("failed to compress record in file at '%s' failed with %w", w.file.Name(), err)
		}
		recordToWrite = compressedRecord
		compressedSize = uint64(len(compressedRecord))
	}

	prevOffset := w.currentOffset
	headerBytesWritten, err := writeRecordHeaderV3(w, uncompressedSize, compressedSize, record == nil)
	if err != nil {
		return 0, fmt.Errorf("failed to write record header in file at '%s' failed with %w", w.file.Name(), err)
	}

	if record == nil {
		w.currentOffset = prevOffset + uint64(headerBytesWritten)
		return prevOffset, nil
	}

	recordBytesWritten, err := w.bufWriter.Write(recordToWrite)
	if err != nil {
		return 0, fmt.Errorf("failed to write record in file at '%s' failed with %w", w.file.Name(), err)
	}

	if recordBytesWritten != len(recordToWrite) {
		return 0, fmt.Errorf("mismatch in written record len for file '%s', expected %d but were %d", w.file.Name(), recordToWrite, recordBytesWritten)
	}

	w.currentOffset = prevOffset + uint64(headerBytesWritten) + uint64(recordBytesWritten)
	w.largestOffset = max(w.largestOffset, w.currentOffset)
	return prevOffset, nil
}

// WriteSync appends a record of bytes and forces a disk sync, returns the current offset this item was written to.
// When directIO is enabled however, we can't write misaligned blocks and immediately returns DirectIOSyncWriteErr
func (w *FileWriter) WriteSync(record []byte) (uint64, error) {
	if w.alignedBlockWrites {
		return 0, DirectIOSyncWriteErr
	}

	offset, err := w.Write(record)
	if err != nil {
		return 0, fmt.Errorf("failed to write record to file at '%s' failed with %w", w.file.Name(), err)
	}

	err = w.bufWriter.Flush()
	if err != nil {
		return 0, fmt.Errorf("failed to flush sync in file at '%s' failed with %w", w.file.Name(), err)
	}

	err = w.file.Sync()
	if err != nil {
		return 0, fmt.Errorf("failed to sync file at '%s' failed with %w", w.file.Name(), err)
	}

	return offset, nil
}

func (w *FileWriter) Close() error {
	w.closed = true
	w.open = false
	err := w.bufWriter.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush close in file at '%s' failed with %w", w.file.Name(), err)
	}

	// when we have previously written past the currentOffset because of seeks, we need to truncate the file again to
	// avoid reading partial records
	if w.largestOffset > w.currentOffset {
		err = w.file.Truncate(int64(w.currentOffset))
		if err != nil {
			return fmt.Errorf("failed to truncate file at '%s' failed with %w", w.file.Name(), err)
		}
	}

	err = w.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file at '%s' failed with %w", w.file.Name(), err)
	}
	return nil
}

func (w *FileWriter) Size() uint64 {
	return w.currentOffset
}

func (w *FileWriter) Seek(offset uint64) error {
	if offset < w.headerOffset {
		return fmt.Errorf("can't seek into the header range, supplied: %d header: %d", offset, w.headerOffset)
	}
	if offset > w.Size() {
		return fmt.Errorf("can't seek past file size, supplied: %d header: %d", offset, w.Size())
	}

	newOffset, err := w.bufWriter.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}
	w.largestOffset = max(w.largestOffset, w.currentOffset)
	w.currentOffset = uint64(newOffset)
	return nil
}

// options

type FileWriterOptions struct {
	path            string
	file            *os.File
	compressionType int
	bufferSizeBytes int
	enableDirectIO  bool
}

type FileWriterOption func(*FileWriterOptions)

// Path defines the file path where to write the recordio file into. Path will create a new file if it doesn't exist yet,
// it will not create any parent directories. Either this or File must be supplied.
func Path(p string) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.path = p
	}
}

// File uses the given os.File as the sink to write into. The code manages the given file lifecycle (ie closing).
// Either this or Path must be supplied
func File(p *os.File) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.file = p
	}
}

// CompressionType sets the record compression for the given file, the types are all prefixed with CompressionType*.
// Valid values for example are CompressionTypeNone, CompressionTypeSnappy, CompressionTypeGZIP.
func CompressionType(p int) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.compressionType = p
	}
}

// BufferSizeBytes sets the write buffer size, by default it uses DefaultBufferSize.
// This is the internal memory buffer before it's written to disk.
func BufferSizeBytes(p int) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.bufferSizeBytes = p
	}
}

// DirectIO is experimental: this flag enables DirectIO while writing. This has some limitation when writing headers and
// disables the ability to use WriteSync.
func DirectIO() FileWriterOption {
	return func(args *FileWriterOptions) {
		args.enableDirectIO = true
	}
}

// NewFileWriter creates a new writer with the given options, either Path or File must be supplied, compression is optional.
func NewFileWriter(writerOptions ...FileWriterOption) (WriterI, error) {
	opts := &FileWriterOptions{
		path:            "",
		file:            nil,
		compressionType: CompressionTypeNone,
		bufferSizeBytes: DefaultBufferSize,
		enableDirectIO:  false,
	}

	for _, writeOption := range writerOptions {
		writeOption(opts)
	}

	if (opts.file == nil) == (opts.path == "") {
		return nil, errors.New("NewFileWriter: either os.File or string path must be supplied, never both")
	}

	if opts.path == "" {
		opts.path = opts.file.Name()
	}

	var factory ReaderWriterCloserFactory
	if opts.enableDirectIO {
		factory = DirectIOFactory{}
	} else {
		factory = BufferedIOFactory{}
	}

	// we have to close the passed file handle because we're going to create a new one based on paths
	if opts.file != nil {
		err := opts.file.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close existing file handle at '%s' failed with %w", opts.path, err)
		}
	}

	file, writer, err := factory.CreateNewWriter(opts.path, opts.bufferSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create new Writer at '%s' failed with %w", opts.path, err)
	}
	return newCompressedFileWriterWithFile(file, writer, opts.compressionType, opts.enableDirectIO)
}

// creates a new writer with the given os.File, with the desired compression
func newCompressedFileWriterWithFile(file *os.File, bufWriter WriteSeekerCloserFlusher, compType int, alignedBlockWrites bool) (WriterI, error) {
	return &FileWriter{
		file:               file,
		bufWriter:          bufWriter,
		alignedBlockWrites: alignedBlockWrites,
		open:               false,
		closed:             false,
		compressionType:    compType,
		currentOffset:      0,
	}, nil
}
