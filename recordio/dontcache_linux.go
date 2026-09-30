//go:build linux

package recordio

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// dontCacheFile keeps the written data out of the page cache. It writes with RWF_DONTCACHE (uncached buffered IO),
// where the kernel drops the written pages once they are written back. Filesystems that don't support it (e.g. btrfs
// before Linux 7.3, or kernels before 6.14) reject the first write with it. From then on, the file writes without the
// flag and Sync evicts the synced pages instead.
type dontCacheFile struct {
	*os.File
	fd int
	// evictOnSync is set once RWF_DONTCACHE turned out to be unsupported
	evictOnSync bool
}

func newDontCacheFile(f *os.File) writableFile {
	return &dontCacheFile{File: f, fd: int(f.Fd())}
}

func (f *dontCacheFile) Write(p []byte) (int, error) {
	written := 0
	for !f.evictOnSync && written < len(p) {
		// an offset of -1 writes at, and advances, the current file offset, like a plain write does
		n, err := unix.Pwritev2(f.fd, [][]byte{p[written:]}, -1, unix.RWF_DONTCACHE)
		switch {
		case errors.Is(err, unix.EOPNOTSUPP):
			f.evictOnSync = true
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

// Sync flushes the file to disk. Without RWF_DONTCACHE, it evicts the now clean pages from the page cache afterwards.
func (f *dontCacheFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	if !f.evictOnSync {
		return nil
	}
	return unix.Fadvise(f.fd, 0, 0, unix.FADV_DONTNEED)
}
