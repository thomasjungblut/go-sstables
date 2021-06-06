package proto

import (
	"bufio"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/recordio/test_files"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

const TestFile = "../test_files/berlin52.tsp"

func TestReadWriteEndToEndProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile))
	assert.Nil(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func TestReadWriteEndToEndGzipProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndGzipProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	assert.Nil(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func TestReadWriteEndToEndSnappyProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndSnappyProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	assert.Nil(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func endToEndReadWriteProtobuf(writer *Writer, t *testing.T, tmpFile *os.File) {
	// we're reading the file line by line and try to read it back and assert the same content
	inFile, err := os.Open(TestFile)
	assert.Nil(t, err)
	assert.Nil(t, writer.Open())

	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		msg := test_files.TextLine{LineNumber: int32(numRead), Line: scanner.Text()}
		_, err = writer.Write(&msg)
		assert.Nil(t, err)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, writer.Close())
	assert.Nil(t, inFile.Close())

	reader, err := NewProtoReaderWithPath(tmpFile.Name())
	assert.Nil(t, err)
	assert.Nil(t, reader.Open())

	inFile, err = os.Open(TestFile)
	assert.Nil(t, err)
	scanner = bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	numRead = 0
	for scanner.Scan() {
		textLine := &test_files.TextLine{}
		_, err := reader.ReadNext(textLine)
		assert.Nil(t, err)
		assert.Equal(t, numRead, int(textLine.LineNumber))
		assert.Equal(t, scanner.Text(), textLine.Line)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, reader.Close())
	assert.Nil(t, inFile.Close())
}

func TestRandomReadWriteEndToEndProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile))
	assert.Nil(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndGzipProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndGzipProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	assert.Nil(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndSnappyProto(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "recordio_EndToEndSnappyProto")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	assert.Nil(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func endToEndRandomReadWriteProtobuf(writer *Writer, t *testing.T, tmpFile *os.File) {
	// same idea as above, but we're testing the random read via mmap
	inFile, err := os.Open(TestFile)
	assert.Nil(t, err)
	assert.Nil(t, writer.Open())

	var lines []string
	offsetMap := make(map[string]uint64)
	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		msg := test_files.TextLine{LineNumber: int32(numRead), Line: line}
		offset, err := writer.Write(&msg)
		offsetMap[line] = offset
		lines = append(lines, line)
		assert.Nil(t, err)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, writer.Close())
	assert.Nil(t, inFile.Close())

	reader, err := NewMMapProtoReaderWithPath(tmpFile.Name())
	assert.Nil(t, err)
	assert.Nil(t, reader.Open())

	// we shuffle the lines, so we can test the actual random read behaviour
	rand.Shuffle(len(lines), func(i, j int) {
		lines[i], lines[j] = lines[j], lines[i]
	})

	numRead = 0
	for _, s := range lines {
		offset := offsetMap[s]
		textLine := &test_files.TextLine{}
		_, err := reader.ReadNextAt(textLine, offset)
		assert.Nil(t, err)
		assert.Equal(t, s, textLine.Line)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	assert.Nil(t, reader.Close())
}
