package proto

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
	"github.com/thomasjungblut/go-sstables/recordio/test_files"
)

const TestFile = "../test_files/berlin52.tsp"

func TestReadWriteEndToEndProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile))
	require.NoError(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func TestReadWriteEndToEndGzipProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndGzipProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	require.NoError(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func TestReadWriteEndToEndSnappyProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndSnappyProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	require.NoError(t, err)

	endToEndReadWriteProtobuf(writer, t, tmpFile)
}

func endToEndReadWriteProtobuf(writer WriterI, t *testing.T, tmpFile *os.File) {
	// we're reading the file line by line and try to read it back and assert the same content
	inFile, err := os.Open(TestFile)
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		msg := test_files.TextLine{LineNumber: int32(numRead), Line: scanner.Text()}
		_, err = writer.Write(&msg)
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
		textLine := &test_files.TextLine{}
		_, err := reader.ReadNext(textLine)
		require.NoError(t, err)
		assert.Equal(t, numRead, int(textLine.LineNumber))
		assert.Equal(t, scanner.Text(), textLine.Line)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, reader.Close())
	require.NoError(t, inFile.Close())
}

func TestRandomReadWriteEndToEndProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile))
	require.NoError(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndGzipProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndGzipProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeGZIP))
	require.NoError(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func TestRandomReadWriteEndToEndSnappyProto(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "recordio_EndToEndSnappyProto")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(tmpFile.Name())) }()
	writer, err := NewWriter(File(tmpFile), CompressionType(recordio.CompressionTypeSnappy))
	require.NoError(t, err)

	endToEndRandomReadWriteProtobuf(writer, t, tmpFile)
}

func endToEndRandomReadWriteProtobuf(writer WriterI, t *testing.T, tmpFile *os.File) {
	// same idea as above, but we're testing the random read via mmap
	inFile, err := os.Open(TestFile)
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	var lines []string
	var offsets []uint64
	offsetMap := make(map[string]uint64)
	numRead := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		msg := test_files.TextLine{LineNumber: int32(numRead), Line: line}
		offset, err := writer.Write(&msg)
		offsetMap[line] = offset
		offsets = append(offsets, offset)
		lines = append(lines, line)
		require.NoError(t, err)
		numRead++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 59, numRead)
	require.NoError(t, writer.Close())
	require.NoError(t, inFile.Close())

	reader, err := NewMMapProtoReaderWithPath(tmpFile.Name())
	require.NoError(t, err)
	require.NoError(t, reader.Open())

	j := 0
	for i := uint64(0); i < reader.Size(); i++ {
		textLine := &test_files.TextLine{}
		offset, _, err := reader.SeekNext(textLine, i)
		if j == len(offsets) {
			require.ErrorIs(t, err, io.EOF)
		} else {
			require.NoError(t, err)
			expectedLine := lines[j]
			require.Equal(t, expectedLine, textLine.Line)
			require.Equal(t, offsets[j], offset)
			if i >= offsets[j] {
				j++
			}
		}
	}

	// we shuffle the lines, so we can test the actual random read behaviour
	rand.Shuffle(len(lines), func(i, j int) {
		lines[i], lines[j] = lines[j], lines[i]
	})

	numRead = 0
	for _, s := range lines {
		offset := offsetMap[s]
		textLine := &test_files.TextLine{}
		_, err := reader.ReadNextAt(textLine, offset)
		require.NoError(t, err)
		assert.Equal(t, s, textLine.Line)
		numRead++
	}
	assert.Equal(t, 59, numRead)
	require.NoError(t, reader.Close())
}
