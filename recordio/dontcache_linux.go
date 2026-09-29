//go:build linux

package recordio

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// dontCacheFile writes with RWF_DONTCACHE (uncached buffered IO), the kernel drops the written pages from the page cache
// once they are written back. Filesystems that don't support it (e.g. btrfs before Linux 7.3, or kernels before 6.14)
// fall back to plain writes on the first write, their pages are evicted after every sync instead, see
// evictFromPageCache.
type dontCacheFile struct {
	*os.File
	fd          int
	unsupported bool
}

func newDontCacheFile(f *os.File) WriteSeekerCloser {
	return &dontCacheFile{File: f, fd: int(f.Fd())}
}

func (f *dontCacheFile) Write(p []byte) (int, error) {
	written := 0
	for !f.unsupported && written < len(p) {
		// an offset of -1 writes at, and advances, the current file offset, like a plain write does
		n, err := unix.Pwritev2(f.fd, [][]byte{p[written:]}, -1, unix.RWF_DONTCACHE)
		switch {
		case errors.Is(err, unix.EOPNOTSUPP):
			f.unsupported = true
		case errors.Is(err, unix.EINTR):
		case err != nil:
			return written, err
		default:
			written += n
		}
	}

	if written < len(p) {
		n, err := f.File.Write(p[written:])
		return written + n, err
	}
	return written, nil
}

// evictFromPageCache drops the clean pages of the file from the page cache, which requires a sync before.
func evictFromPageCache(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
