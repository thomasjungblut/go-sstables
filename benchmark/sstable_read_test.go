package benchmark

import (
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"testing"
)

// we're writing a gig worth of data and test how long it takes to read the index + all data
func BenchmarkSSTableRead(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "sstable_BenchRead")
	assert.Nil(b, err)
	defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()

	cmp := skiplist.BytesComparator{}
	writer, err := sstables.NewSSTableStreamWriter(sstables.WriteBasePath(tmpDir), sstables.WithKeyComparator(cmp))
	assert.Nil(b, err)
	assert.Nil(b, writer.Open())

	bytes := randomRecordOfSize(1024)
	for i := 0; i < len(bytes)*1024; i++ {
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k, uint32(i))
		assert.Nil(b, writer.WriteNext(k, bytes))
	}

	assert.Nil(b, writer.Close())
	fullScanTable(b, tmpDir, cmp)
}

func fullScanTable(b *testing.B, tmpDir string, cmp skiplist.Comparator[[]byte]) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := sstables.NewSSTableReader(sstables.ReadBasePath(tmpDir), sstables.ReadWithKeyComparator(cmp))
		assert.Nil(b, err)
		scanner, err := reader.Scan()
		assert.Nil(b, err)
		for {
			_, _, err := scanner.Next()
			if errors.Is(err, sstables.Done) {
				break
			}
		}
		b.SetBytes(int64(reader.MetaData().TotalBytes))
		assert.Nil(b, reader.Close())
	}
}
