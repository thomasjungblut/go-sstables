package recordio

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

var testBuf = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

func TestReaderHappyCountingSingleBytes(t *testing.T) {
	reader := testReader()

	idx := 0
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}

		assert.Nil(t, err)
		assert.Equal(t, testBuf[idx], b)
		idx++
	}

	assert.Equal(t, len(testBuf), idx)
	assert.Equal(t, len(testBuf), int(reader.count))
}

func TestReaderHappyCountingBufferedBytes(t *testing.T) {
	reader := testReader()

	buf := make([]byte, 5)
	idx := 0
	for {
		read, err := reader.Read(buf)
		if err == io.EOF {
			break
		}

		assert.Nil(t, err)
		for i := 0; i < read; i++ {
			assert.Equal(t, testBuf[idx], buf[i])
			idx++
		}
	}

	assert.Equal(t, len(testBuf), idx)
	assert.Equal(t, len(testBuf), int(reader.count))
}

func TestReaderHappyResetAndRead(t *testing.T) {
	reader := testReader()

	idx := 0
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}

		assert.Nil(t, err)
		assert.Equal(t, testBuf[idx], b)
		idx++

		// this basically simulates how we reset from offset 5 to 8 (eg in seeking)
		if b == 5 {
			reader.Reset(bufio.NewReader(bytes.NewReader(testBuf[8:])))
			idx = 8
		}
	}

	assert.Equal(t, len(testBuf), idx)
	assert.Equal(t, 12, int(reader.count)) // 12 because we have been skipping that many bytes in reading
}

func testReader() *CountingBufferedReader {
	return NewCountingByteReader(bufio.NewReader(bytes.NewReader(testBuf)))
}
