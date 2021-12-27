package recordio

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestBufferedIOFactory_CreateNewReader(t *testing.T) {
	temp, err := ioutil.TempFile("", "TestBufferedIOFactory_CreateNewReader")
	require.NoError(t, err)
	require.NoError(t, temp.Close())

	f := BufferedIOFactory{}
	file, buf, err := f.CreateNewReader(temp.Name(), 4096)
	require.NoError(t, err)
	defer closeCleanFile(t, file)

	assert.Equal(t, 4096, buf.Size())
}

func TestBufferedIOFactory_CreateNewWriter(t *testing.T) {
	temp, err := ioutil.TempFile("", "TestBufferedIOFactory_CreateNewWriter")
	require.NoError(t, err)
	require.NoError(t, temp.Close())

	f := BufferedIOFactory{}
	file, buf, err := f.CreateNewWriter(temp.Name(), 4096)
	require.NoError(t, err)
	defer closeCleanFile(t, file)

	assert.Equal(t, 4096, buf.Size())
}
