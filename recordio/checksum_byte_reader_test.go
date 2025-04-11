package recordio

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestChecksumHappyPath(t *testing.T) {
	reader := newChecksumByteReader(bytes.NewReader(MagicNumberSeparatorLongBytes),
		make([]byte, len(MagicNumberSeparatorLongBytes)))
	var actual []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		actual = append(actual, b)
	}

	require.Equal(t, MagicNumberSeparatorLongBytes, actual)
	checksum, err := reader.Checksum()
	require.NoError(t, err)
	require.Equal(t, uint64(0x967294b), checksum)

	reader.Reset()
	checksum, err = reader.Checksum()
	require.NoError(t, err)
	require.Equal(t, uint64(0), checksum)
}

func TestChecksumOutOfRange(t *testing.T) {
	reader := newChecksumByteReader(bytes.NewReader(MagicNumberSeparatorLongBytes),
		make([]byte, 0))

	_, err := reader.ReadByte()
	require.Equal(t, fmt.Errorf("checksum byte reader out of range: 0, only have 0"), err)
}
