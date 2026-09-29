//go:build !linux

package recordio

import "os"

// newDontCacheFile is not supported on this platform, the written data stays in the page cache.
func newDontCacheFile(f *os.File) WriteSeekerCloser {
	return f
}

func evictFromPageCache(_ *os.File) error {
	return nil
}
