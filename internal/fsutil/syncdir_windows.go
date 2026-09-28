package fsutil

// SyncDir is a no-op on Windows: directory handles can't be flushed there and NTFS journals its metadata.
func SyncDir(_ string) error {
	return nil
}
