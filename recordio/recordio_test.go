package recordio

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// containing all the end to end tests

func TestReadWriteEndToEnd(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEnd")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile))
	require.NoError(t, err)

	endToEndReadWrite(writer, openedReaderFunc(t, tmpFile), t, tmpFile)
}

func TestReadWriteEndToEndGzip(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndGzip")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile), CompressionType(CompressionTypeGZIP))
	require.NoError(t, err)

	endToEndReadWrite(writer, openedReaderFunc(t, tmpFile), t, tmpFile)
}

func TestReadWriteEndToEndSnappy(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndSnappy")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile), CompressionType(CompressionTypeSnappy))
	require.NoError(t, err)

	endToEndReadWrite(writer, openedReaderFunc(t, tmpFile), t, tmpFile)
}

func TestReadWriteEndToEndDirectIO(t *testing.T) {
	available, err := IsDirectIOAvailable()
	require.NoError(t, err)
	if !available {
		t.SkipNow()
	}

	tmpFile, err := ioutil.TempFile("", "recordio_EndToEnd")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile), DirectIO())
	require.NoError(t, err)

	reader := func() ReaderI {
		reader, err := newFileReaderWithFactory(tmpFile.Name(), DirectIOFactory{})
		require.NoError(t, err)
		require.NoError(t, reader.Open())
		return reader
	}

	endToEndReadWrite(writer, reader, t, tmpFile)
}

func endToEndReadWrite(writer WriterI, readerFunc func() ReaderI, t *testing.T, tmpFile *os.File) {
	// we're reading the file line by line and try to read it back and assert the same content
	inFile, err := os.Open("test_files/berlin52.tsp")
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		_, err = writer.Write([]byte(scanner.Text()))
		require.NoError(t, err)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, writer.Close())
	require.NoError(t, inFile.Close())

	reader := readerFunc()

	inFile, err = os.Open("test_files/berlin52.tsp")
	require.NoError(t, err)
	scanner = bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	numRead = 0
	for scanner.Scan() {
		bytes, err := reader.ReadNext()
		require.NoError(t, err)
		assert.Equal(t, scanner.Text(), string(bytes))
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	// ensure that the reader does not find anything beyond this
	_, err = reader.ReadNext()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, reader.Close())
	require.NoError(t, inFile.Close())
}

func closeFileWriter(t *testing.T, writer *FileWriter) {
	func() { require.NoError(t, writer.Close()) }()
}

func closeOpenClosable(t *testing.T, oc OpenClosableI) {
	func() { require.NoError(t, oc.Close()) }()
}

func closeFileReader(t *testing.T, reader *FileReader) {
	func() { require.NoError(t, reader.Close()) }()
}

func closeMMapReader(t *testing.T, reader *MMapReader) {
	func() { require.NoError(t, reader.Close()) }()
}

func removeFileWriterFile(t *testing.T, writer *FileWriter) {
	func() { require.NoError(t, os.Remove(writer.file.Name())) }()
}

func openedReaderFunc(t *testing.T, tmpFile *os.File) func() ReaderI {
	return func() ReaderI {
		reader, err := NewFileReaderWithPath(tmpFile.Name())
		require.NoError(t, err)
		require.NoError(t, reader.Open())
		return reader
	}
}
