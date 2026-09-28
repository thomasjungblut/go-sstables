//go:build !windows

package fsutil

import (
	"errors"
	"os"
)

// SyncDir fsyncs the directory at path, which makes creating, renaming and removing its entries durable.
// Syncing a file only persists its contents, the directory entry pointing to a new file needs to be synced separately.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
