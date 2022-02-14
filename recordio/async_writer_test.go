//go:build linux

package recordio

import (
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio/iouring"
	"io/ioutil"
	"testing"
)

func TestAsyncWriter_HappyPath(t *testing.T) {
	ok, err := iouring.IsIOUringAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("iouring not available here")
		return
	}

	temp, err := ioutil.TempFile("", "TestAsyncWriter_HappyPath")
	require.NoError(t, err)
	defer closeCleanFile(t, temp)

	writer, err := NewAsyncWriter(temp.Name(), 4)
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		_, err = writer.Write(randomRecordOfSize(1024))
		require.NoError(t, err)
	}

	require.NoError(t, writer.Close())
}
