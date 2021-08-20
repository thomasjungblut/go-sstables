package wal

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// we largely exercise replays in wal_appender_test already, here we focus only on edge cases that cause errors

func TestReplayFileFails(t *testing.T) {
	file, err := ioutil.TempFile("", "wal_replayfilefails")
	require.Nil(t, err)
	opts, err := NewWriteAheadLogOptions(BasePath(file.Name()))
	require.Nil(t, err)
	_, err = NewReplayer(opts)
	assert.Equal(t, fmt.Errorf("given base path %s is not a directory", file.Name()), err)
}

func TestReplayFolderDoesNotExist(t *testing.T) {
	opts, err := NewWriteAheadLogOptions(BasePath("somepaththathopefullydoesnotexistanywhere"))
	require.Nil(t, err)
	_, err = NewReplayer(opts)
	assert.NotNil(t, err)
}

func TestReplayerIgnoresNonWalFiles(t *testing.T) {
	log, recorder := singleRecordWal(t, "wal_replayignorewal")

	err := ioutil.WriteFile(filepath.Join(log.walOptions.basePath, "some-not-so-wal-file"), []byte{1, 2, 3}, os.ModePerm)
	require.Nil(t, err)

	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestReplayHonorsCallbackErrors(t *testing.T) {
	log, _ := singleRecordWal(t, "wal_replayhonorscallback")
	repl, err := NewReplayer(log.walOptions)
	require.Nil(t, err)
	testErr := errors.New("test")
	err = repl.Replay(func(record []byte) error {
		return testErr
	})
	assert.Equal(t, testErr, errors.Unwrap(err))
}
