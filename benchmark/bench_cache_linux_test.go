//go:build linux

package benchmark

import (
	"os"

	"golang.org/x/sys/unix"
)

// evictFromPageCache uses posix_fadvise(POSIX_FADV_DONTNEED), which drops clean pages of the file without root.
func evictFromPageCache(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
