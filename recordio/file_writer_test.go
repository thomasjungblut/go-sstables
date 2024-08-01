package recordio

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterHappyPathOpenWriteClose(t *testing.T) {
	writer := singleWrite(t)
	defer removeFileWriterFile(t, writer)

	reader := newReaderOnTopOfWriter(t, writer)
	defer closeFileReader(t, reader)

	readNextExpectAscendingBytesOfLen(t, reader, 13)
	readNextExpectEOF(t, reader)
}

func TestWriterWriteNil(t *testing.T) {
	writer := simpleWriteBytes(t, nil)
	defer removeFileWriterFile(t, writer)

	reader := newReaderOnTopOfWriter(t, writer)
	defer closeFileReader(t, reader)

	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assert.Equal(t, []byte{}, buf)
	readNextExpectEOF(t, reader)
}

func TestSingleWriteSize(t *testing.T) {
	writer := singleWrite(t)
	defer removeFileWriterFile(t, writer)

	size := writer.Size()
	assert.Equal(t, uint64(0x1a), size)
	stat, err := os.Stat(writer.file.Name())
	require.Nil(t, err)
	assert.Equal(t, int64(0x1a), stat.Size())
	assert.Equal(t, size, uint64(stat.Size()))
}

func TestWriterMultiRecordWriteOffsetCheck(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	offset, err := writer.Write(randomRecordOfSize(5))
	assert.Equal(t, uint64(FileHeaderSizeBytes), offset)
	assert.Equal(t, uint64(0x12), writer.Size())
	require.Nil(t, err)

	offset, err = writer.Write(randomRecordOfSize(10))
	assert.Equal(t, uint64(0x12), offset)
	assert.Equal(t, uint64(0x21), writer.Size())
	require.Nil(t, err)

	offset, err = writer.Write(randomRecordOfSize(25))
	assert.Equal(t, uint64(0x21), offset)
	assert.Equal(t, uint64(0x3f), writer.Size())
	require.Nil(t, err)

	assert.Equal(t, uint64(0x3f), writer.currentOffset)
	assert.Equal(t, uint64(0x3f), writer.Size())

	err = writer.Close()
	require.Nil(t, err)

	stat, err := os.Stat(writer.file.Name())
	require.Nil(t, err)
	assert.Equal(t, int64(63), stat.Size())

	reader := newReaderOnTopOfWriter(t, writer)
	defer closeFileReader(t, reader)

	readNextExpectRandomBytesOfLen(t, reader, 5)
	readNextExpectRandomBytesOfLen(t, reader, 10)
	readNextExpectRandomBytesOfLen(t, reader, 25)
	readNextExpectEOF(t, reader)
}

func TestWriterForbidsClosedWrites(t *testing.T) {
	writer := singleWrite(t)
	defer removeFileWriterFile(t, writer)
	// file is closed, should not allow us to write anymore
	offset, err := writer.Write(make([]byte, 0))
	assert.Equal(t, uint64(0), offset)
	assert.Contains(t, err.Error(), "writer was either not opened yet or is closed already")
	err = writer.Open()
	assert.Contains(t, err.Error(), "already closed")
}

func TestWriterForbidsDoubleOpens(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)
	defer closeFileWriter(t, writer)

	err := writer.Open()
	assert.Contains(t, err.Error(), "already opened")
}

func TestWriterForbidsWritesOnUnopenedFiles(t *testing.T) {
	writer, err := newUncompressedTestWriter()
	defer removeFileWriterFile(t, writer)
	defer closeFileWriter(t, writer)

	require.Nil(t, err)
	_, err = writer.Write(make([]byte, 0))
	assert.Equal(t, errors.New("writer was either not opened yet or is closed already"), err)
}

func TestUnsupportedCompressionType(t *testing.T) {
	w, err := newCompressedTestWriter(5)
	require.Nil(t, err)
	defer removeFileWriterFile(t, w)
	defer closeFileWriter(t, w)

	err = w.Open()
	assert.Equal(t, errors.New("unsupported compression type 5"), errors.Unwrap(err))
}

func TestWriterOpenNonEmptyFile(t *testing.T) {
	writer := singleWrite(t)
	stat, err := os.Stat(writer.file.Name())
	require.Nil(t, err)
	assert.NotEqual(t, 8, stat.Size())
	defer removeFileWriterFile(t, writer)

	w, err := NewFileWriter(Path(writer.file.Name()))
	require.NoError(t, err)
	writer = w.(*FileWriter)

	require.Nil(t, err)
	defer closeFileWriter(t, writer)

	err = writer.Open()
	require.NoError(t, err)
}

func TestWriterDoublePathFileInit(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_UncompressedWriter")
	require.Nil(t, err)
	defer closeCleanFile(t, tmpFile)

	_, err = NewFileWriter(Path("/tmp/abc"), File(tmpFile))
	assert.Equal(t, errors.New("NewFileWriter: either os.File or string path must be supplied, never both"), err)
}

func TestWriterInitNoPath(t *testing.T) {
	_, err := NewFileWriter()
	assert.Equal(t, errors.New("NewFileWriter: either os.File or string path must be supplied, never both"), err)
}

func TestWriterCrashCreatesValidHeader(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_CrashCreatesValidHeader")
	require.Nil(t, err)
	defer closeCleanFile(t, tmpFile)

	w, err := NewFileWriter(Path(tmpFile.Name()))
	require.NoError(t, err)
	fw := w.(*FileWriter)

	require.NoError(t, w.Open())
	require.NoError(t, fw.file.Close())

	// this should yield a valid file with no record
	reader := newReaderOnTopOfWriter(t, fw)
	defer closeFileReader(t, reader)

	readNextExpectEOF(t, reader)
}

func TestWriterCrashCreatesNoValidHeaderWithDirectIO(t *testing.T) {
	ok, err := IsDirectIOAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("directio not available here")
		return
	}

	tmpFile, err := os.CreateTemp("", "recordio_CrashCreatesValidHeaderDirectIO")
	require.Nil(t, err)
	defer closeCleanFile(t, tmpFile)

	w, err := NewFileWriter(Path(tmpFile.Name()), DirectIO())
	require.NoError(t, err)
	require.NoError(t, w.Open())
	require.NoError(t, w.(*FileWriter).file.Close())

	reader, err := NewFileReaderWithPath(tmpFile.Name())
	require.Nil(t, err)
	defer closeOpenClosable(t, reader)

	require.ErrorIs(t, reader.Open(), io.EOF)
}

func TestWriterNotAllowsSyncsWithDirectIO(t *testing.T) {
	ok, err := IsDirectIOAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("directio not available here")
		return
	}

	tmpFile, err := os.CreateTemp("", "recordio_WriterNotAllowsSyncsWithDirectIO")
	require.Nil(t, err)
	defer closeCleanFile(t, tmpFile)

	w, err := NewFileWriter(Path(tmpFile.Name()), DirectIO())
	require.NoError(t, err)
	defer closeOpenClosable(t, w)

	require.NoError(t, w.Open())
	_, err = w.WriteSync([]byte{1})
	require.ErrorIs(t, err, DirectIOSyncWriteErr)
}

func TestWriterSeekHappyPath(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	previousOffset := writer.Size()
	offset, err := writer.Write([]byte{12, 13, 14, 15, 16})
	assert.Equal(t, uint64(FileHeaderSizeBytes), offset)
	assert.Equal(t, uint64(0x12), writer.Size())
	require.Nil(t, err)

	require.NoError(t, writer.Seek(previousOffset))

	offset, err = writer.Write([]byte{1, 2, 3, 4, 5})
	assert.Equal(t, uint64(FileHeaderSizeBytes), offset)
	assert.Equal(t, uint64(0x12), writer.Size())
	require.Nil(t, err)

	require.NoError(t, writer.Close())
	reader := newReaderOnTopOfWriter(t, writer)
	defer closeFileReader(t, reader)

	// this should only be exactly one record
	next, err := reader.ReadNext()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, next)

	readNextExpectEOF(t, reader)
}

func TestWriterSeekOutOfBounds(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	_, err := writer.Write(ascendingBytes(5))
	require.NoError(t, err)

	require.Error(t, writer.Seek(0))
	require.Error(t, writer.Seek(writer.headerOffset-1))
	require.NoError(t, writer.Seek(writer.headerOffset))
	require.Error(t, writer.Seek(writer.Size()+1))
	require.NoError(t, writer.Seek(writer.Size()))
	require.NoError(t, writer.Close())
}

func TestWriterSeekShorterReplacementWrite(t *testing.T) {
	writer := newOpenedWriter(t)
	defer removeFileWriterFile(t, writer)

	o, err := writer.Write(ascendingBytes(5))
	require.NoError(t, err)
	require.NoError(t, writer.Seek(o))

	// this should create a two byte suffix to the file that should not be readable as a record
	_, err = writer.Write(ascendingBytes(3))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader := newReaderOnTopOfWriter(t, writer)
	defer func() {
		require.NoError(t, reader.Close())
	}()
	readNextExpectAscendingBytesOfLen(t, reader, 3)

	// TODO(thomas): can we wipe the remainder of the file?
	// there's a more general concern about having short replacement writes within a record though
	// maybe we need to leave a marker to skip until the next record / EOF
	readNextExpectMagicNumberMismatch(t, reader)
}

func newUncompressedTestWriter() (*FileWriter, error) {
	tmpFile, err := os.CreateTemp("", "recordio_UncompressedWriter")
	if err != nil {
		return nil, err
	}

	r, err := NewFileWriter(File(tmpFile), BufferSizeBytes(1024))

	if err != nil {
		return nil, err
	}

	return r.(*FileWriter), nil
}

func newCompressedTestWriter(compType int) (*FileWriter, error) {
	tmpFile, err := os.CreateTemp("", "recordio_CompressedWriter")
	if err != nil {
		return nil, err
	}

	r, err := NewFileWriter(File(tmpFile), BufferSizeBytes(1024), CompressionType(compType))

	if err != nil {
		return nil, err
	}

	return r.(*FileWriter), nil
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
	// first offset should always be 8 bytes (for version and compression type in the file header)
	assert.Equal(t, uint64(FileHeaderSizeBytes), offset)
	require.Nil(t, err)
	require.Nil(t, writer.Close())
	return writer
}

func newOpenedWriter(t *testing.T) *FileWriter {
	writer, err := newUncompressedTestWriter()
	require.Nil(t, err)
	err = writer.Open()
	require.Nil(t, err)
	return writer
}

func readNextExpectRandomBytesOfLen(t *testing.T, reader *FileReader, expectedLen int) {
	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assert.Equal(t, expectedLen, len(buf))
}

func readNextExpectAscendingBytesOfLen(t *testing.T, reader *FileReader, expectedLen int) {
	buf, err := reader.ReadNext()
	require.Nil(t, err)
	assertAscendingBytes(t, buf, expectedLen)
}

func readNextExpectEOF(t *testing.T, reader *FileReader) {
	buf, err := reader.ReadNext()
	require.Nil(t, buf)
	assert.Equal(t, io.EOF, errors.Unwrap(err))
}

func readNextExpectMagicNumberMismatch(t *testing.T, reader *FileReader) {
	buf, err := reader.ReadNext()
	require.Nil(t, buf)
	require.ErrorIs(t, err, MagicNumberMismatchErr)
}

func newReaderOnTopOfWriter(t *testing.T, writer *FileWriter) *FileReader {
	reader, err := NewFileReaderWithPath(writer.file.Name())
	require.Nil(t, err)
	require.Nil(t, reader.Open())
	return reader.(*FileReader)
}
