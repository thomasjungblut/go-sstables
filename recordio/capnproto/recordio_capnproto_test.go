package capnproto

import (
	"bufio"
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
