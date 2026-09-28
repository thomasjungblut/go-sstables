package benchmark

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// benchDirEnv overrides the directory in which benchmark files are created, it defaults to the package directory.
// The OS temp directory is deliberately not used, it's often a tmpfs and would benchmark memory instead of disk.
const benchDirEnv = "GO_SSTABLES_BENCH_DIR"

// benchDir creates a fresh directory for benchmark files, which is removed when the benchmark finishes.
func benchDir(tb testing.TB) string {
	base := os.Getenv(benchDirEnv)
	if base == "" {
		base = "."
	}
	dir, err := os.MkdirTemp(base, "bench-")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// dropPageCache evicts the given file, or all files below the given directory, from the page cache so that
// subsequent reads come from disk. This is a no-op on platforms without support, see evictFromPageCache.
func dropPageCache(tb testing.TB, path string) {
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		// dirty pages can't be evicted
		if err := f.Sync(); err != nil {
			return err
		}
		return evictFromPageCache(f)
	})
	require.NoError(tb, err)
}
