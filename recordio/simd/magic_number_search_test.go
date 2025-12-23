package simd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMagicNumberSearchHappyPath(t *testing.T) {
	data := make([]byte, 10000)

	firstMarker := 10000 - 300
	data[firstMarker] = 145
	data[firstMarker+1] = 141
	data[firstMarker+2] = 76

	secondMarker := 10000 - 3
	data[secondMarker] = 145
	data[secondMarker+1] = 141
	data[secondMarker+2] = 76

	// check the entire range
	for i := 0; i < len(data); i++ {
		actualResult := FindMagicNumber(data, i)
		expectedResult := firstMarker
		if i >= firstMarker+1 {
			expectedResult = secondMarker
		}

		if i > secondMarker {
			expectedResult = -1
		}

		require.Equalf(t, expectedResult, actualResult, "unexpected result at offset %d", i)
	}
}

func TestMagicNumberSearchBoundary(t *testing.T) {
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1}, 0))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, 3))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, 4))
	require.Equal(t, -1, FindMagicNumber([]byte{0, 1, 3, 4}, -1))
}
