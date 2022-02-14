package iouring

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsIOUringAvailable_HappyPath(t *testing.T) {
	ok, err := IsIOUringAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("iouring not available here")
		return
	}
}
