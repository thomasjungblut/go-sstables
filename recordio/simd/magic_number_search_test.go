package simd

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMagicNumberSearchHappyPath(t *testing.T) {
	if !AVXSupported() {
		t.Skip()
	}

	data := make([]byte, 10000)

	data[10000-300] = 145
	data[10000-299] = 141
	data[10000-298] = 76

	data[10000-3] = 145
	data[10000-2] = 141
	data[10000-1] = 76

	index := FindFirstMagicNumber(data)
	require.Equal(t, 10000-300, index)
	index = FindMagicNumber(data, 0)
	require.Equal(t, 10000-300, index)

	ix := FindFirstMagicNumber(data[9701:])
	require.Equal(t, 296, ix)
	ix = FindMagicNumber(data, 9701)
	require.Equal(t, 10000-3, ix)
}

func TestMagicNumberSearchBoundary(t *testing.T) {
	require.Equal(t, -1, FindFirstMagicNumber([]byte{0, 1}))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1}, 0))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, 3))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, 4))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, -1))
}
