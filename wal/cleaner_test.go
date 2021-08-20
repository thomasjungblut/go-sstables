package wal

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSimpleDeleteHappyPath(t *testing.T) {
	log, _ := singleRecordWal(t, "wal_simpleDeleteHappyPath")
	info, err := os.Stat(log.walOptions.basePath)
	require.Nil(t, err)
	assert.True(t, info.IsDir())
	err = NewCleaner(log.walOptions).Clean()
	require.Nil(t, err)
	_, err = os.Stat(log.walOptions.basePath)
	assert.NotNil(t, err)
}
