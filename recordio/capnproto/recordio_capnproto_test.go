package capnproto

import (
	"bufio"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/recordio/test_files"

	"capnproto.org/go/capnp/v3"
)

const TestFile = "../test_files/berlin52.tsp"

func TestReadWriteEndToEndCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile))
	require.NoError(t, err)

	endToEndReadWriteCapnProto(writer, t, tmpFile)
}

func TestReadWriteEndToEndGzipCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndGzipCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	require.NoError(t, err)

	endToEndReadWriteCapnProto(writer, t, tmpFile)
}

func TestReadWriteEndToEndSnappyCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndSnappyCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	require.NoError(t, err)

	endToEndReadWriteCapnProto(writer, t, tmpFile)
}

func endToEndReadWriteCapnProto(writer WriterI, t *testing.T, tmpFile *os.File) {
	// we're reading the file line by line and try to read it back and assert the same content
	inFile, err := os.Open(TestFile)
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	arena := capnp.SingleSegment(nil)
	msg, seg, err := capnp.NewMessage(arena)
	require.NoError(t, err)
	for scanner.Scan() {
		lineMsg, err := test_files.NewRootTextLineCapnProto(seg)
		require.NoError(t, err)
		lineMsg.SetLineNumber(int32(numRead))
		require.NoError(t, lineMsg.SetLine(scanner.Text()))
		_, err = writer.Write(msg)
		require.NoError(t, err)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, writer.Close())
	require.NoError(t, inFile.Close())

	reader, err := NewReader(ReaderPath(tmpFile.Name()))
	require.NoError(t, err)
	require.NoError(t, reader.Open())

	inFile, err = os.Open(TestFile)
	require.NoError(t, err)
	scanner = bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	numRead = 0
	for scanner.Scan() {
		msg, err := reader.ReadNext()
		require.NoError(t, err)

		textLine, err := test_files.ReadRootTextLineCapnProto(msg)
		require.NoError(t, err)

		line, err := textLine.Line()
		require.NoError(t, err)
		assert.Equal(t, numRead, int(textLine.LineNumber()))
		assert.Equal(t, scanner.Text(), line)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, reader.Close())
	require.NoError(t, inFile.Close())
}

func TestRandomReadWriteEndToEndCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile))
	require.NoError(t, err)

	endToEndRandomReadWriteCapnProto(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndGzipCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndGzipCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	require.NoError(t, err)

	endToEndRandomReadWriteCapnProto(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndSnappyCapnProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndSnappyCapnProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	require.NoError(t, err)

	endToEndRandomReadWriteCapnProto(writer, t, tmpFile)
}

func endToEndRandomReadWriteCapnProto(writer WriterI, t *testing.T, tmpFile *os.File) {
	// same idea as above, but we're testing the random read via mmap
	inFile, err := os.Open(TestFile)
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	var lines []string
	offsetMap := make(map[string]uint64)
	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	arena := capnp.SingleSegment(nil)
	msg, seg, err := capnp.NewMessage(arena)
	require.NoError(t, err)
	for scanner.Scan() {
		line := scanner.Text()
		lineMsg, err := test_files.NewRootTextLineCapnProto(seg)
		require.NoError(t, err)
		lineMsg.SetLineNumber(int32(numRead))
		require.NoError(t, lineMsg.SetLine(scanner.Text()))
		offset, err := writer.Write(msg)
		offsetMap[line] = offset
		lines = append(lines, line)
		require.NoError(t, err)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, writer.Close())
	require.NoError(t, inFile.Close())

	reader, err := NewMMapCapnProtoReaderWithPath(tmpFile.Name())
	require.NoError(t, err)
	require.NoError(t, reader.Open())

	// we shuffle the lines, so we can test the actual random read behaviour
	rand.Shuffle(len(lines), func(i, j int) {
		lines[i], lines[j] = lines[j], lines[i]
	})

	numRead = 0
	for _, s := range lines {
		offset := offsetMap[s]
		msg, err := reader.ReadNextAt(offset)
		require.NoError(t, err)
		textLine, err := test_files.ReadRootTextLineCapnProto(msg)
		require.NoError(t, err)
		line, err := textLine.Line()
		assert.Equal(t, s, line)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	require.NoError(t, reader.Close())
}
