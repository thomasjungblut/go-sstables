package recordio

import (
	"bufio"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

// containing all the end to end tests

func TestReadWriteEndToEnd(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEnd")
	assert.Nil(t, err)
	defer func() { assert.Nil(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile))
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func TestReadWriteEndToEndGzip(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndGzip")
	assert.Nil(t, err)
	defer func() { assert.Nil(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile), CompressionType(CompressionTypeGZIP))
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func TestReadWriteEndToEndSnappy(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndSnappy")
	assert.Nil(t, err)
	defer func() { assert.Nil(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewFileWriter(File(tmpFile), CompressionType(CompressionTypeSnappy))
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func endToEndReadWrite(writer WriterI, t *testing.T, tmpFile *os.File) {
	// we're reading the file line by line and try to read it back and assert the same content
	inFile, err := os.Open("test_files/berlin52.tsp")
	assert.Nil(t, err)
	assert.Nil(t, writer.Open())

	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		_, err = writer.Write([]byte(scanner.Text()))
		assert.Nil(t, err)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, writer.Close())
	assert.Nil(t, inFile.Close())

	reader, err := NewFileReaderWithPath(tmpFile.Name())
	assert.Nil(t, err)
	assert.Nil(t, reader.Open())

	inFile, err = os.Open("test_files/berlin52.tsp")
	assert.Nil(t, err)
	scanner = bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	numRead = 0
	for scanner.Scan() {
		bytes, err := reader.ReadNext()
		assert.Nil(t, err)
		assert.Equal(t, scanner.Text(), string(bytes))
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, reader.Close())
	assert.Nil(t, inFile.Close())
}

func closeFileWriter(t *testing.T, writer *FileWriter) {
	func() { assert.Nil(t, writer.Close()) }()
}

func closeOpenClosable(t *testing.T, oc OpenClosableI) {
	func() { assert.Nil(t, oc.Close()) }()
}

func closeFileReader(t *testing.T, reader *FileReader) {
	func() { assert.Nil(t, reader.Close()) }()
}

func closeMMapReader(t *testing.T, reader *MMapReader) {
	func() { assert.Nil(t, reader.Close()) }()
}

func removeFileWriterFile(t *testing.T, writer *FileWriter) {
	func() { assert.Nil(t, os.Remove(writer.file.Name())) }()
}
