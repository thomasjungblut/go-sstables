//go:build !(linux && (amd64 || arm64))

package benchmark

import "os"

// evictFromPageCache is not supported on this platform, reads in benchmarks may be served from the page cache.
func evictFromPageCache(_ *os.File) error {
	return nil
}
