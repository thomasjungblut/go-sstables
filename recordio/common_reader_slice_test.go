package recordio

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thomasjungblut/go-sstables/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// the slice-based decoder must behave exactly like the byte-by-byte one, including on truncated input
func TestReadRecordHeaderV4FromSliceMatchesByteReader(t *testing.T) {
	headers := [][]byte{
		fillRecordHeaderV4(make([]byte, RecordHeaderV4MaxSizeBytes), 0, 0, true),
		fillRecordHeaderV4(make([]byte, RecordHeaderV4MaxSizeBytes), 42, 0, false),
		fillRecordHeaderV4(make([]byte, RecordHeaderV4MaxSizeBytes), 1<<40, 1<<20, false),
		{0, 0, 0, 0},
		{0x05, 0, 0, 0},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}
	corrupted := bytes.Clone(headers[2])
	corrupted[5] ^= 0xff
	headers = append(headers, corrupted)

	for _, header := range headers {
		for l := 0; l <= len(header); l++ {
			buf := header[:l]
			expU, expC, expNil, expErr := readRecordHeaderV4(newChecksumByteReader(bytes.NewReader(buf), make([]byte, RecordHeaderV4MaxSizeBytes)))
			u, c, isNil, n, err := readRecordHeaderV4FromSlice(buf)
			assert.Equal(t, expU, u)
			assert.Equal(t, expC, c)
			assert.Equal(t, expNil, isNil)
			if expErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, l, n)
			} else {
				assert.Equal(t, expErr.Error(), err.Error(), "header %x", buf)
				assert.Equal(t, errors.Unwrap(expErr), errors.Unwrap(err))
			}
		}
	}
}

// small buffers force record headers to straddle buffer boundaries, which exercises the byte-by-byte fallback
func TestFileReaderSmallBuffersHeadersAcrossBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small_buffers.rio")
	w, err := NewFileWriter(Path(path))
	require.NoError(t, err)
	require.NoError(t, w.Open())

	var expected [][]byte
	for i := range 500 {
		var record []byte
		switch i % 3 {
		case 0:
			record = nil
		case 1:
			record = testutil.Bytes(nil, i%17)
		default:
			record = testutil.Bytes(nil, i)
		}
		_, err := w.Write(record)
		require.NoError(t, err)
		expected = append(expected, record)
	}
	require.NoError(t, w.Close())

	for _, bufSize := range []int{1, 7, 36, 37, 53, 128, 4096} {
		reader, err := NewFileReader(ReaderPath(path), ReaderBufferSizeBytes(bufSize))
		require.NoError(t, err)
		require.NoError(t, reader.Open())

		for i, exp := range expected {
			actual, err := reader.ReadNext()
			require.NoError(t, err, "buffer size %d, record %d", bufSize, i)
			if exp == nil {
				assert.Nil(t, actual)
			} else {
				assert.Equal(t, exp, actual)
			}
		}
		_, err = reader.ReadNext()
		assert.ErrorIs(t, err, io.EOF)
		require.NoError(t, reader.Close())
	}
	require.NoError(t, os.Remove(path))
}

// mixes SkipNext and ReadNext, small buffers exercise discarding within the buffer as well as seeking past it
func TestFileReaderSkipNextAcrossBufferSizes(t *testing.T) {
	for _, compType := range []int{CompressionTypeNone, CompressionTypeSnappy} {
		path := filepath.Join(t.TempDir(), "skip.rio")
		w, err := NewFileWriter(Path(path), CompressionType(compType))
		require.NoError(t, err)
		require.NoError(t, w.Open())

		var expected [][]byte
		for i := 0; i < 400; i++ {
			var record []byte
			switch i % 4 {
			case 0:
				record = nil
			case 1:
				record = testutil.Bytes(nil, i%13)
			case 2:
				record = testutil.Bytes(nil, i*3)
			default:
				record = testutil.Bytes(nil, 5000+i)
			}
			_, err := w.Write(record)
			require.NoError(t, err)
			expected = append(expected, record)
		}
		require.NoError(t, w.Close())

		for _, bufSize := range []int{1, 37, 100, 1024, 8192, DefaultBufferSize} {
			for _, skipEvery := range []int{2, 3} {
				reader, err := NewFileReader(ReaderPath(path), ReaderBufferSizeBytes(bufSize))
				require.NoError(t, err)
				require.NoError(t, reader.Open())

				for i, exp := range expected {
					if i%skipEvery == 0 {
						require.NoError(t, reader.SkipNext(), "comp %d, buffer size %d, record %d", compType, bufSize, i)
						continue
					}
					actual, err := reader.ReadNext()
					require.NoError(t, err, "comp %d, buffer size %d, record %d", compType, bufSize, i)
					if exp == nil {
						assert.Nil(t, actual)
					} else {
						assert.Equal(t, exp, actual)
					}
				}
				_, err = reader.ReadNext()
				assert.ErrorIs(t, err, io.EOF)
				assert.ErrorIs(t, reader.SkipNext(), io.EOF)
				require.NoError(t, reader.Close())
			}
		}
	}
}
