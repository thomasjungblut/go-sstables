package recordio

import (
	"testing"
	"io/ioutil"
	"github.com/stretchr/testify/assert"
	"os"
	"errors"
	"math/rand"
	"io"
)

func TestWriterHappyPathOpenWriteClose(t *testing.T) {
	writer := singleWrite(t)
	defer os.Remove(writer.file.Name())

	reader := newReaderOnTopOfWriter(t, writer)
	defer reader.Close()

	readNextExpectAscendingBytesOfLen(t, reader, 13)
	readNextExpectEOF(t, reader)
}

func TestWriterMultiRecordWriteOffsetCheck(t *testing.T) {
	writer := newOpenedWriter(t)
	defer os.Remove(writer.file.Name())

	offset, err := writer.Write(randomRecordOfSize(5))
	assert.Equal(t, uint64(8), offset)
	assert.Nil(t, err)

	offset, err = writer.Write(randomRecordOfSize(10))
	assert.Equal(t, uint64(33), offset)
	assert.Nil(t, err)

	offset, err = writer.Write(randomRecordOfSize(25))
	assert.Equal(t, uint64(63), offset)
	assert.Nil(t, err)

	assert.Equal(t, uint64(108), writer.currentOffset)

	err = writer.Close()
	assert.Nil(t, err)

	stat, err := os.Stat(writer.file.Name())
	assert.Nil(t, err)
	assert.Equal(t, int64(108), stat.Size())

	reader := newReaderOnTopOfWriter(t, writer)
	defer reader.Close()

	readNextExpectRandomBytesOfLen(t, reader, 5)
	readNextExpectRandomBytesOfLen(t, reader, 10)
	readNextExpectRandomBytesOfLen(t, reader, 25)
	readNextExpectEOF(t, reader)
}

func TestWriterForbidsClosedWrites(t *testing.T) {
	writer := singleWrite(t)
	defer os.Remove(writer.file.Name())
	// file is closed, should not allow us to write anymore
	offset, err := writer.Write(make([]byte, 0))
	assert.Equal(t, uint64(0), offset)
	assert.Equal(t, errors.New("writer was either not opened yet or is closed already"), err)
	err = writer.Open()
	assert.Equal(t, errors.New("already closed"), err)
}

func TestWriterForbidsDoubleOpens(t *testing.T) {
	writer := newOpenedWriter(t)
	defer os.Remove(writer.file.Name())
	err := writer.Open()
	assert.Equal(t, errors.New("already opened"), err)
}

func TestWriterForbidsWritesOnUnopenedFiles(t *testing.T) {
	writer, err := newUncompressedTestWriter()
	defer os.Remove(writer.file.Name())
	assert.Nil(t, err)
	_, err = writer.Write(make([]byte, 0))
	assert.Equal(t, errors.New("writer was either not opened yet or is closed already"), err)
}

func TestUnsupportedCompressionType(t *testing.T) {
	w, err := newCompressedTestWriter(5)
	assert.Nil(t, err)
	err = w.Open()
	assert.Equal(t, errors.New("unsupported compression type 5"), err)
}

func TestWriterOpenNonEmptyFile(t *testing.T) {
	writer := singleWrite(t)
	stat, err := os.Stat(writer.file.Name())
	assert.Nil(t, err)
	assert.NotEqual(t, 8, stat.Size())
	defer os.Remove(writer.file.Name())

	writer, err = NewFileWriterWithPath(writer.file.Name())
	assert.Nil(t, err)

	err = writer.Open()
	assert.Equal(t, errors.New("file is not empty"), err)
}

func newUncompressedTestWriter() (*FileWriter, error) {
	tmpFile, err := ioutil.TempFile("", "recordio_UncompressedWriter")
	if err != nil {
		return nil, err
	}

	r, err := NewFileWriterWithFile(tmpFile)

	if err != nil {
		return nil, err
	}

	return r, nil
}

func newCompressedTestWriter(compType int) (*FileWriter, error) {
	tmpFile, err := ioutil.TempFile("", "recordio_CompressedWriter")
	if err != nil {
		return nil, err
	}

	r, err := NewCompressedFileWriterWithFile(tmpFile, compType)

	if err != nil {
		return nil, err
	}

	return r, nil
}

func randomRecordOfSize(l int) []byte {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(rand.Intn(255))
	}

	return bytes
}

func singleWrite(t *testing.T) *FileWriter {
	return simpleWriteBytes(t, ascendingBytes(13))
}

func assertAscendingBytes(t *testing.T, buf []byte, expectedLen int) {
	assert.Equal(t, expectedLen, len(buf))
	for i := 0; i < expectedLen; i++ {
		assert.Equal(t, buf[i], byte(i))
	}
}

func ascendingBytes(l int) []byte {
	buf := make([]byte, l)
	for i := 0; i < l; i++ {
		buf[i] = byte(i)
	}
	return buf
}

func simpleWriteBytes(t *testing.T, record []byte) *FileWriter {
	writer := newOpenedWriter(t)
	offset, err := writer.Write(record)
	// first offset should always be 8 bytes (for version and compression type)
	assert.Equal(t, uint64(8), offset)
	assert.Nil(t, err)
	err = writer.Close()
	assert.Nil(t, err)
	return writer
}
func newOpenedWriter(t *testing.T) (*FileWriter) {
	writer, err := newUncompressedTestWriter()
	assert.Nil(t, err)
	err = writer.Open()
	assert.Nil(t, err)
	return writer
}

func readNextExpectRandomBytesOfLen(t *testing.T, reader *FileReader, expectedLen int) {
	buf, err := reader.ReadNext()
	assert.Nil(t, err)
	assert.Equal(t, expectedLen, len(buf))
}

func readNextExpectAscendingBytesOfLen(t *testing.T, reader *FileReader, expectedLen int) {
	buf, err := reader.ReadNext()
	assert.Nil(t, err)
	assertAscendingBytes(t, buf, expectedLen)
}

func readNextExpectEOF(t *testing.T, reader *FileReader) {
	buf, err := reader.ReadNext()
	assert.Nil(t, buf)
	assert.Equal(t, io.EOF, err)
}

func newReaderOnTopOfWriter(t *testing.T, writer *FileWriter) *FileReader {
	reader, err := NewFileReaderWithPath(writer.file.Name())
	assert.Nil(t, err)
	assert.Nil(t, reader.Open())
	return reader
}
