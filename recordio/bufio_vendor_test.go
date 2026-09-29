package recordio

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

type closingWriter struct {
	buf []byte
}

func (w *closingWriter) Write(p []byte) (n int, err error) {
	copy(w.buf, p) // simply overwrite for testing purposes
	return len(p), nil
}

func (w *closingWriter) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
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

func TestCreateNewBufferCloseFlushes(t *testing.T) {
	sink := &closingWriter{make([]byte, 6)}
	wBuf := NewWriterBuf(sink, make([]byte, 4))
	assert.Equal(t, 4, wBuf.Size())

	_, err := wBuf.Write([]byte{13, 6, 91, 22})
	require.NoError(t, err)
	// buffer should not been flushed so far
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0}, sink.buf)
	require.NoError(t, wBuf.Close())
	assert.Equal(t, []byte{13, 6, 91, 22, 0, 0}, sink.buf)
}

func TestSeekFlushes(t *testing.T) {
	sink := &closingWriter{make([]byte, 6)}
	wBuf := NewWriterBuf(sink, make([]byte, 4))
	assert.Equal(t, 4, wBuf.Size())

	_, err := wBuf.Write([]byte{13, 6, 91, 22})
	require.NoError(t, err)
	// buffer should not been flushed so far
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0}, sink.buf)

	_, err = wBuf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, []byte{13, 6, 91, 22, 0, 0}, sink.buf)

	require.NoError(t, wBuf.Close())
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

// recordingReader returns data in chunks of at most chunkSize and records the buffer length of every Read call.
type recordingReader struct {
	data      []byte
	chunkSize int
	err       error
	readLens  []int
}

func (r *recordingReader) Read(p []byte) (int, error) {
	r.readLens = append(r.readLens, len(p))
	if len(r.data) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := copy(p[:min(len(p), r.chunkSize)], r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestPeekBufferedDoesNotAdvance(t *testing.T) {
	r := NewReaderBuf(bytes.NewReader([]byte{1, 2, 3, 4, 5}), make([]byte, 16))

	assert.Equal(t, []byte{1, 2, 3}, r.PeekBuffered(3))
	assert.Equal(t, []byte{1, 2, 3}, r.PeekBuffered(3))
	// peeking more than buffered only returns what's there
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, r.PeekBuffered(100))
	assert.Equal(t, 5, r.Buffered())

	b, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(1), b)
	assert.Equal(t, []byte{2, 3}, r.PeekBuffered(2))
}

func TestPeekBufferedOnlyFillsEmptyBuffer(t *testing.T) {
	src := &recordingReader{data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, chunkSize: 4}
	r := NewReaderBuf(src, make([]byte, 8))

	// the empty buffer is filled from its start with the whole buffer, which keeps reads aligned for DirectIO
	assert.Equal(t, []byte{1, 2, 3, 4}, r.PeekBuffered(8))
	assert.Equal(t, []int{8}, src.readLens)

	// a partially consumed buffer is never refilled by peeking, fewer bytes than requested are returned instead
	r.DiscardBuffered(2)
	assert.Equal(t, []byte{3, 4}, r.PeekBuffered(8))
	assert.Equal(t, []int{8}, src.readLens)

	// only once it's empty the next peek reads again, into the whole buffer
	r.DiscardBuffered(2)
	assert.Equal(t, []byte{5, 6, 7, 8}, r.PeekBuffered(8))
	assert.Equal(t, []int{8, 8}, src.readLens)
}

func TestDiscardBuffered(t *testing.T) {
	r := NewReaderBuf(bytes.NewReader([]byte{1, 2, 3, 4, 5}), make([]byte, 16))
	require.Len(t, r.PeekBuffered(5), 5)

	r.DiscardBuffered(3)
	assert.Equal(t, 2, r.Buffered())
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte{4, 5}, rest)
}

func TestPeekBufferedAtEOF(t *testing.T) {
	r := NewReaderBuf(bytes.NewReader([]byte{1}), make([]byte, 16))
	require.Equal(t, []byte{1}, r.PeekBuffered(4))
	r.DiscardBuffered(1)

	assert.Empty(t, r.PeekBuffered(4))
	// the EOF stays available to the next read
	_, err := r.ReadByte()
	assert.ErrorIs(t, err, io.EOF)
}

func TestPeekBufferedKeepsReadError(t *testing.T) {
	readErr := errors.New("disk on fire")
	r := NewReaderBuf(&recordingReader{chunkSize: 4, err: readErr}, make([]byte, 16))

	assert.Empty(t, r.PeekBuffered(4))
	_, err := r.ReadByte()
	assert.ErrorIs(t, err, readErr)
}

func TestCountingReaderCountsDiscardedBytesOnly(t *testing.T) {
	r := NewCountingByteReader(NewReaderBuf(bytes.NewReader([]byte{1, 2, 3, 4, 5}), make([]byte, 16)))

	assert.Equal(t, []byte{1, 2, 3}, r.PeekBuffered(3))
	assert.Equal(t, uint64(0), r.Count())

	r.DiscardBuffered(2)
	assert.Equal(t, uint64(2), r.Count())
	assert.Equal(t, 3, r.Buffered())

	b, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(3), b)
	assert.Equal(t, uint64(3), r.Count())
}
