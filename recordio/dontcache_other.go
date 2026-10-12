//go:build !linux

package recordio

import "os"

// newDontCacheFile is not supported on this platform, the written data stays in the page cache.
func newDontCacheFile(f *os.File) writableFile {
	return f
}
