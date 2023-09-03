//go:build linux

package recordio

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestAsyncWriter_HappyPath(t *testing.T) {
	ok, err := IsIOUringAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("iouring not available here")
		return
	}

	temp, err := ioutil.TempFile("", "TestAsyncWriter_HappyPath")
	require.NoError(t, err)
	defer closeCleanFile(t, temp)

	writer, file, err := NewAsyncWriter(temp.Name(), 4)
	require.NoError(t, err)
	require.NotNil(t, file)

	var expected []byte
	for i := 0; i < 100; i++ {
		s := randomRecordOfSize(10)
		_, err = writer.Write(s)
		require.NoError(t, err)
		expected = append(expected, s...)
	}

	require.NoError(t, writer.Close())
	fileContentEquals(t, file, expected)
}

func TestAsyncWriter_GuardAgainstBufferReuse(t *testing.T) {
	ok, err := IsIOUringAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("iouring not available here")
		return
	}

	temp, err := ioutil.TempFile("", "TestAsyncWriter_GuardAgainstBufferReuse")
	require.NoError(t, err)
	defer closeCleanFile(t, temp)

	writer, file, err := NewAsyncWriter(temp.Name(), 4)
	require.NoError(t, err)
	require.NotNil(t, file)

	reusedSlice := []byte{13, 06, 91}
	// we are writing the same slice, three times before a forced flush due to capacity
	writeBuf(t, writer, reusedSlice)
	writeBuf(t, writer, reusedSlice)
	writeBuf(t, writer, reusedSlice)
	// fourth time we change the slice in-place
	reusedSlice[0] = 29
	writeBuf(t, writer, reusedSlice)
	writeBuf(t, writer, reusedSlice)
	require.NoError(t, writer.Close())

	fileContentEquals(t, file, []byte{
		13, 06, 91,
		13, 06, 91,
		13, 06, 91,
		29, 06, 91,
		29, 06, 91,
	})
}

func fileContentEquals(t *testing.T, file *os.File, expectedContent []byte) {
	f, err := os.Open(file.Name())
	require.NoError(t, err)
	all, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, expectedContent, all)
}

func writeBuf(t *testing.T, writer WriteCloserFlusher, buf []byte) {
	o, err := writer.Write(buf)
	require.NoError(t, err)
	assert.Equal(t, len(buf), o)
}
