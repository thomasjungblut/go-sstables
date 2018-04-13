package recordio

import (
	"os"
	"errors"
	"strconv"
	"encoding/binary"
)

/*
 * This type defines a binary file format (little endian).
 * The file header has a 32 bit version number and a 32 bit compression type enum according to the table above.
 * Each record written in the file follows the following format (sequentially):
 * - MagicNumber (32 bits) to separate records from each other.
 * - Uncompressed data payload size (64 bits).
 * - Compressed data payload size (64 bits), or 0 if the data is not compressed.
 * - Payload as plain bytes, possibly compressed
 */
type FileWriter struct {
	open            bool
	closed          bool
	file            *os.File
	currentOffset   uint64
	compressionType int
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
		return errors.New("seek did not return offset 0, it was: " + strconv.FormatInt(newOffset, 10))
	}

	offset, err := writeFileHeader(w)
	if err != nil {
		return err
	}

	w.currentOffset = uint64(offset)
	w.open = true

	return nil
}

func writeFileHeader(writer *FileWriter) (int, error) {
	// 4 byte version number, 4 byte compression code = 8 bytes
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes[0:4], Version)
	binary.LittleEndian.PutUint32(bytes[4:8], uint32(writer.compressionType))
	written, err := writer.file.Write(bytes)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func writeRecordHeader(writer *FileWriter, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (int, error) {
	// 4 byte magic number, 8 byte uncompressed size, 8 bytes for compressed size = 20 bytes
	// TODO(thomas): this can be improved with vint compression
	bytes := make([]byte, HeaderSizeBytes)
	binary.LittleEndian.PutUint32(bytes[0:4], MagicNumberSeparator)
	binary.LittleEndian.PutUint64(bytes[4:12], payloadSizeUncompressed)
	binary.LittleEndian.PutUint64(bytes[12:20], payloadSizeCompressed)
	written, err := writer.file.Write(bytes)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func (w *FileWriter) Write(record []byte) (uint64, error) {
	return writeInternal(w, record, false)
}

func (w *FileWriter) WriteSync(record []byte) (uint64, error) {
	return writeInternal(w, record, true)
}

func writeInternal(w *FileWriter, record []byte, sync bool) (uint64, error) {
	if !w.open || w.closed {
		return 0, errors.New("writer was either not opened yet or is closed already")
	}

	prevOffset := w.currentOffset
	headerBytesWritten, err := writeRecordHeader(w, uint64(len(record)), 0)
	if err != nil {
		return 0, err
	}

	recordBytesWritten, err := w.file.Write(record)
	if err != nil {
		return 0, err
	}

	if sync {
		err = w.file.Sync()
		if err != nil {
			return 0, err
		}
	}

	w.currentOffset = prevOffset + uint64(headerBytesWritten) + uint64(recordBytesWritten)
	return prevOffset, nil
}

func (w *FileWriter) Close() (error) {
	w.closed = true
	w.open = false
	return w.file.Close()
}

func NewFileWriterWithPath(path string) (*FileWriter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewFileWriterWithFile(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewFileWriterWithFile(file *os.File) (*FileWriter, error) {
	return &FileWriter{
		file:            file,
		open:            false,
		closed:          false,
		compressionType: CompressionTypeNone,
		currentOffset:   0,
	}, nil
}
