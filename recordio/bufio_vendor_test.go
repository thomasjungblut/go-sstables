package recordio

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type closingWriter struct {
	buf []byte
}

func (w *closingWriter) Write(p []byte) (n int, err error) {
	copy(w.buf, p) // simply overwrite for testing purposes
	return len(p), nil
}

func (*closingWriter) Close() error {
	return nil
}

func TestCreateNewBufferWithSlice(t *testing.T) {
	sink := &closingWriter{make([]byte, 6)}
	wBuf := NewWriterBuf(sink, make([]byte, 4))
	assert.Equal(t, 4, wBuf.Size())

	_, err := wBuf.Write([]byte{13, 6, 91, 22})
	require.NoError(t, err)
	// buffer should not been flushed so far
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0}, sink.buf)
	require.NoError(t, wBuf.Flush())
	assert.Equal(t, []byte{13, 6, 91, 22, 0, 0}, sink.buf)
}

func TestCreateNewBufferWithAlignedSlice(t *testing.T) {
	sink := &closingWriter{make([]byte, 8)}
	wBuf := NewAlignedWriterBuf(sink, make([]byte, 4))
	assert.Equal(t, 4, wBuf.Size())

	_, err := wBuf.Write([]byte{13, 6, 91})
	require.NoError(t, err)
	// buffer should not been flushed so far
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, sink.buf)
	require.NoError(t, wBuf.Flush())
	assert.Equal(t, []byte{13, 6, 91, 0, 0, 0, 0, 0}, sink.buf)
}

func TestCreateNewBufferWithAlignedSliceZerosBuffer(t *testing.T) {
	sink := &closingWriter{make([]byte, 8)}
	dirtyBuf := []byte{1, 1, 1, 1}
	wBuf := NewAlignedWriterBuf(sink, dirtyBuf)
	assert.Equal(t, 4, wBuf.Size())

	_, err := wBuf.Write([]byte{13, 6})
	require.NoError(t, err)
	// buffer should not been flushed so far
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, sink.buf)
	require.NoError(t, wBuf.Flush())
	assert.Equal(t, []byte{13, 6, 0, 0, 0, 0, 0, 0}, sink.buf)
}
