//go:build !linux

package benchmark

import "os"

// evictFromPageCache is not supported on this platform, reads in benchmarks may be served from the page cache.
func evictFromPageCache(_ *os.File) error {
	return nil
}

// cachedFraction is not supported on this platform and always returns -1.
func cachedFraction(_ string) (float64, error) {
	return -1, nil
}
