//go:build linux

package recordio

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/thomasjungblut/go-sstables/internal/testutil"
)

// writeWithDontCache writes records through a FileWriter with DontCache, optionally forcing the fadvise fallback.
func writeWithDontCache(t *testing.T, path string, forceFallback bool, records [][]byte) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	require.NoError(t, err)
	file := &dontCacheFile{File: f, fd: int(f.Fd()), evictOnSync: forceFallback}
	w := newCompressedFileWriterWithFile(file, NewWriterBuf(file, make([]byte, 4096)), CompressionTypeNone)
	require.NoError(t, w.Open())

	for i, record := range records {
		if i%2 == 0 {
			_, err = w.WriteSync(record)
		} else {
			_, err = w.Write(record)
		}
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
}

func TestDontCacheRoundTrip(t *testing.T) {
	for _, forceFallback := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "dontcache.rio")
		var records [][]byte
		for i := 0; i < 100; i++ {
			records = append(records, testutil.Bytes(i*97))
		}
		writeWithDontCache(t, path, forceFallback, records)

		reader, err := NewFileReaderWithPath(path)
		require.NoError(t, err)
		require.NoError(t, reader.Open())
		for i, expected := range records {
			actual, err := reader.ReadNext()
			require.NoError(t, err, "fallback %v, record %d", forceFallback, i)
			assert.Equal(t, expected, actual)
		}
		require.NoError(t, reader.Close())
	}
}

func TestDontCacheWithSeek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dontcache_seek.rio")
	w, err := NewFileWriter(Path(path), DontCache())
	require.NoError(t, err)
	require.NoError(t, w.Open())
	_, err = w.Write([]byte{1})
	require.NoError(t, err)
	offset, err := w.Write([]byte{2})
	require.NoError(t, err)
	// the file offset used by pwritev2 must follow the seek, like a plain write
	require.NoError(t, w.Seek(offset))
	_, err = w.Write([]byte{3})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reader, err := NewFileReaderWithPath(path)
	require.NoError(t, err)
	require.NoError(t, reader.Open())
	defer func() { require.NoError(t, reader.Close()) }()
	for _, expected := range [][]byte{{1}, {3}} {
		actual, err := reader.ReadNext()
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

// residentPages counts the pages of the file that are in the page cache.
func residentPages(t *testing.T, path string) int {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	require.NoError(t, err)

	data, err := unix.Mmap(int(f.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
	require.NoError(t, err)
	defer func() { _ = unix.Munmap(data) }()

	pageSize := os.Getpagesize()
	vec := make([]byte, (len(data)+pageSize-1)/pageSize)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0])))
	require.Zero(t, errno, "mincore failed: %v", errno)
	resident := 0
	for _, v := range vec {
		resident += int(v & 1)
	}
	return resident
}

func TestDontCacheEvictsFromPageCache(t *testing.T) {
	dir := t.TempDir()
	var statfs unix.Statfs_t
	require.NoError(t, unix.Statfs(dir, &statfs))
	if statfs.Type == unix.TMPFS_MAGIC {
		t.Skip("tmpfs keeps its data in the page cache, run with TMPDIR on a disk")
	}

	var records [][]byte
	for i := 0; i < 64; i++ {
		records = append(records, testutil.Bytes(64*1024))
	}

	// without DontCache, the written data stays in the page cache
	cachedPath := filepath.Join(dir, "cached.rio")
	w, err := NewFileWriter(Path(cachedPath))
	require.NoError(t, err)
	require.NoError(t, w.Open())
	for _, record := range records {
		_, err = w.Write(record)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.Greater(t, residentPages(t, cachedPath), 0)

	for _, forceFallback := range []bool{false, true} {
		path := filepath.Join(dir, "dontcache.rio")
		writeWithDontCache(t, path, forceFallback, records)
		resident := residentPages(t, path)
		if forceFallback {
			// fadvise evicts synchronously after the sync
			assert.Equal(t, 0, resident, "fallback %v", forceFallback)
		} else {
			// with RWF_DONTCACHE, the kernel drops the pages when their writeback completes, which can be deferred to
			// after the sync returned. Some pages may thus still be around right after close (e.g. 128 of 1024 on
			// ext4 in GitHub Actions), but by far most of them must be gone.
			stat, err := os.Stat(path)
			require.NoError(t, err)
			totalPages := int(stat.Size()) / os.Getpagesize()
			assert.LessOrEqual(t, resident, totalPages/4, "fallback %v", forceFallback)
		}
		require.NoError(t, os.Remove(path))
	}
}
