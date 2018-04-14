package recordio

import (
	"io/ioutil"
	"github.com/stretchr/testify/assert"
	"os"
	"bufio"
	"testing"
)

// containing all the end to end tests

func TestReadWriteEndToEnd(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEnd")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewFileWriterWithFile(tmpFile)
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func TestReadWriteEndToEndGzip(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndGzip")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewCompressedFileWriterWithFile(tmpFile, CompressionTypeGZIP)
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func TestReadWriteEndToEndSnappy(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndSnappy")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewCompressedFileWriterWithFile(tmpFile, CompressionTypeSnappy)
	assert.Nil(t, err)

	endToEndReadWrite(writer, t, tmpFile)
}

func endToEndReadWrite(writer *FileWriter, t *testing.T, tmpFile *os.File) {
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
