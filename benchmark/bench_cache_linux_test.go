//go:build linux && (amd64 || arm64)

package benchmark

import (
	"os"
	"syscall"
)

const fadvDontNeed = 4 // POSIX_FADV_DONTNEED

// evictFromPageCache uses posix_fadvise(POSIX_FADV_DONTNEED), which drops clean pages of the file without root.
func evictFromPageCache(f *os.File) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, fadvDontNeed, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
