package recordio

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/fs"
	"io/ioutil"
	"testing"
)

func TestDirectIOFactory_CreateNewReader(t *testing.T) {
	ok, err := IsDirectIOAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("directio not available here")
		return
	}

	temp, err := ioutil.TempFile("", "TestDirectIOFactory_CreateNewReader")
	require.NoError(t, err)
	require.NoError(t, temp.Close())

	f := DirectIOFactory{}
	file, buf, err := f.CreateNewReader(temp.Name(), 4096)
	require.NoError(t, err)
	defer closeCleanFile(t, file)

	assert.Equal(t, 4096, buf.Size())
	stat, err := file.Stat()
	require.NoError(t, err)
	assert.Equal(t, fs.FileMode(0666), stat.Mode())
}

func TestDirectIOFactory_CreateNewWriter(t *testing.T) {
	ok, err := IsDirectIOAvailable()
	require.NoError(t, err)
	if !ok {
		t.Skip("directio not available here")
		return
	}

	temp, err := ioutil.TempFile("", "TestDirectIOFactory_CreateNewWriter")
	require.NoError(t, err)
	require.NoError(t, temp.Close())

	f := DirectIOFactory{}
	file, buf, err := f.CreateNewWriter(temp.Name(), 4096)
	require.NoError(t, err)
	defer closeCleanFile(t, file)

	assert.Equal(t, 4096, buf.Size())
	stat, err := file.Stat()
	require.NoError(t, err)
	assert.Equal(t, fs.FileMode(0666), stat.Mode())
}
