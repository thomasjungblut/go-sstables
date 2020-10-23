package wal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSimpleDeleteHappyPath(t *testing.T) {
	log, _ := singleRecordWal(t, "wal_simpleWriteHappyPath")
	info, err := os.Stat(log.walOptions.basePath)
	assert.Nil(t, err)
	assert.True(t, info.IsDir())
	err = NewCleaner(log.walOptions).Clean()
	assert.Nil(t, err)
	_, err = os.Stat(log.walOptions.basePath)
	assert.NotNil(t, err)
}
