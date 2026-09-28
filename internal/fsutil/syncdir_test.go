package fsutil

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncDir(t *testing.T) {
	assert.NoError(t, SyncDir(t.TempDir()))
}

func TestSyncDirMissing(t *testing.T) {
	assert.Error(t, SyncDir(filepath.Join(t.TempDir(), "missing")))
}
