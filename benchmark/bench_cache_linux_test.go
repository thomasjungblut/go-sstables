//go:build linux

package benchmark

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// evictFromPageCache uses posix_fadvise(POSIX_FADV_DONTNEED), which drops clean pages of the file without root.
func evictFromPageCache(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

// cachedFraction returns the fraction of the file's pages that are in the page cache, between 0 and 1.
func cachedFraction(path string) (float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil || stat.Size() == 0 {
		return 0, err
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	defer func() { _ = unix.Munmap(data) }()

	pageSize := os.Getpagesize()
	vec := make([]byte, (len(data)+pageSize-1)/pageSize)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return 0, errno
	}
	cached := 0
	for _, v := range vec {
		cached += int(v & 1)
	}
	return float64(cached) / float64(len(vec)), nil
}
