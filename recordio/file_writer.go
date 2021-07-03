package recordio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio/compressor"
	"os"
)

/*
 * This type defines a binary file format (little endian).
 * The file header has a 32 bit version number and a 32 bit compression type enum according to the table above.
 * Each record written in the file follows the following format (sequentially):
 * - MagicNumber (encoding/binary/Uvarint) to separate records from each other.
 * - Uncompressed data payload size (encoding/binary/Uvarint).
 * - Compressed data payload size (encoding/binary/Uvarint), or 0 if the data is not compressed.
 * - Payload as plain bytes, possibly compressed
 */
type FileWriter struct {
	open              bool
	closed            bool
	file              *os.File
	bufWriter         *bufio.Writer
	currentOffset     uint64
	compressionType   int
	compressor        compressor.CompressionI
	recordHeaderCache []byte
}

func (w *FileWriter) Open() error {
	if w.open {
		return errors.New("already opened")
	}

	if w.closed {
		return errors.New("already closed")
	}

	stat, err := w.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() != 0 {
		return errors.New("file is not empty")
	}

	// make sure we are at the start of the file
	newOffset, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}

	if newOffset != 0 {
		return fmt.Errorf("seek did not return offset 0, it was: %d", newOffset)
	}

	w.bufWriter.Reset(w.file)

	offset, err := writeFileHeader(w)
	if err != nil {
		return err
	}

	w.compressor, err = NewCompressorForType(w.compressionType)
	if err != nil {
		return err
	}

	w.currentOffset = uint64(offset)
	w.open = true
	w.recordHeaderCache = make([]byte, RecordHeaderV2MaxSizeBytes)

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
//noinspection GoUnusedFunction
func writeRecordHeaderV1(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (int, error) {
	// 4 byte magic number, 8 byte uncompressed size, 8 bytes for compressed size = 20 bytes
	bytes := make([]byte, RecordHeaderSizeBytes)
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

func writeRecordHeaderV2(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (int, error) {
	header := fillRecordHeaderV2(writer.recordHeaderCache, payloadSizeUncompressed, payloadSizeCompressed)
	written, err := writer.bufWriter.Write(header)
	if err != nil {
		return 0, err
	}

	return written, nil
}

// Appends a record of bytes, returns the current offset this item was written to
func (w *FileWriter) Write(record []byte) (uint64, error) {
	return writeInternal(w, record, false)
}

// Appends a record of bytes and forces a disk sync, returns the current offset this item was written to
func (w *FileWriter) WriteSync(record []byte) (uint64, error) {
	return writeInternal(w, record, true)
}

func writeInternal(w *FileWriter, record []byte, sync bool) (uint64, error) {
	if !w.open || w.closed {
		return 0, errors.New("writer was either not opened yet or is closed already")
	}

	var recordToWrite []byte
	recordToWrite = record

	uncompressedSize := uint64(len(recordToWrite))
	compressedSize := uint64(0)

	if w.compressor != nil {
		// TODO(thomas): we can try to buffer pool this compression as well
		compressedRecord, err := w.compressor.Compress(record)
		if err != nil {
			return 0, err
		}
		recordToWrite = compressedRecord
		compressedSize = uint64(len(compressedRecord))
	}

	prevOffset := w.currentOffset
	headerBytesWritten, err := writeRecordHeaderV2(w, uncompressedSize, compressedSize)
	if err != nil {
		return 0, err
	}

	recordBytesWritten, err := w.bufWriter.Write(recordToWrite)
	if err != nil {
		return 0, err
	}

	if recordBytesWritten != len(recordToWrite) {
		return 0, errors.New("mismatch in written record len")
	}

	if sync {
		err = w.bufWriter.Flush()
		if err != nil {
			return 0, err
		}

		err = w.file.Sync()
		if err != nil {
			return 0, err
		}
	}

	w.currentOffset = prevOffset + uint64(headerBytesWritten) + uint64(recordBytesWritten)
	return prevOffset, nil
}

func (w *FileWriter) Close() error {
	w.closed = true
	w.open = false
	err := w.bufWriter.Flush()
	if err != nil {
		return err
	}
	return w.file.Close()
}

func (w *FileWriter) Size() uint64 {
	return w.currentOffset
}

// options

type FileWriterOptions struct {
	path            string
	file            *os.File
	compressionType int
	bufferSizeBytes int
}

type FileWriterOption func(*FileWriterOptions)

func Path(p string) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.path = p
	}
}

func File(p *os.File) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.file = p
	}
}

func CompressionType(p int) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.compressionType = p
	}
}

func BufferSizeBytes(p int) FileWriterOption {
	return func(args *FileWriterOptions) {
		args.bufferSizeBytes = p
	}
}

// creates a new writer with the given options, either Path or File must be supplied, compression is optional.
func NewFileWriter(writerOptions ...FileWriterOption) (*FileWriter, error) {
	opts := &FileWriterOptions{
		path:            "",
		file:            nil,
		compressionType: CompressionTypeNone,
		bufferSizeBytes: DefaultBufferSize,
	}

	for _, writeOption := range writerOptions {
		writeOption(opts)
	}

	if (opts.file != nil) && (opts.path != "") {
		return nil, errors.New("either os.File or string path must be supplied, never both")
	}

	if opts.file == nil {
		if opts.path == "" {
			return nil, errors.New("path was not supplied")
		}
		f, err := os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		opts.file = f
	}

	bufWriter := bufio.NewWriterSize(opts.file, opts.bufferSizeBytes)
	return newCompressedFileWriterWithFile(opts.file, bufWriter, opts.compressionType)
}

// creates a new writer with the given os.File, with the desired compression
func newCompressedFileWriterWithFile(file *os.File, bufWriter *bufio.Writer, compType int) (*FileWriter, error) {
	return &FileWriter{
		file:            file,
		bufWriter:       bufWriter,
		open:            false,
		closed:          false,
		compressionType: compType,
		currentOffset:   0,
	}, nil
}
