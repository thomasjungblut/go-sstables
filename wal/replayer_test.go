package wal

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

// we largely exercise replays in wal_appender_test already, here we focus only on edge cases that cause errors

func TestReplayFileFails(t *testing.T) {
	file, err := ioutil.TempFile("", "wal_replayfilefails")
	assert.Nil(t, err)
	opts, err := NewWriteAheadLogOptions(BasePath(file.Name()))
	assert.Nil(t, err)
	_, err = NewReplayer(opts)
	assert.Equal(t, fmt.Errorf("given base path %s is not a directory", file.Name()), err)
}

func TestReplayFolderDoesNotExist(t *testing.T) {
	opts, err := NewWriteAheadLogOptions(BasePath("somepaththathopefullydoesnotexistanywhere"))
	assert.Nil(t, err)
	_, err = NewReplayer(opts)
	assert.NotNil(t, err)
}

func TestReplayerIgnoresNonWalFiles(t *testing.T) {
	log, recorder := singleRecordWal(t, "wal_replayignorewal")

	err := ioutil.WriteFile(path.Join(log.walOptions.basePath, "some-not-so-wal-file"), []byte{1, 2, 3}, os.ModePerm)
	assert.Nil(t, err)

	assertRecorderMatchesReplay(t, log.walOptions, recorder)
}

func TestReplayHonorsCallbackErrors(t *testing.T) {
	log, _ := singleRecordWal(t, "wal_replayhonorscallback")
	repl, err := NewReplayer(log.walOptions)
	assert.Nil(t, err)
	testErr := errors.New("test")
	err = repl.Replay(func(record []byte) error {
		return testErr
	})
	assert.Equal(t, testErr, err)
}
