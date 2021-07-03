package benchmark

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkSSTableRead(b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "sstable_BenchRead")
	assert.Nil(b, err)
	defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()

	cmp := skiplist.BytesComparator
	writer, err := sstables.NewSSTableStreamWriter(sstables.WriteBasePath(tmpDir), sstables.WithKeyComparator(cmp))
	assert.Nil(b, err)
	assert.Nil(b, writer.Open())

	bytes := randomRecordOfSize(1024)
	// we're writing a gig worth of data
	for i := 0; i < len(bytes)*1024; i++ {
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k, uint32(i))
		assert.Nil(b, writer.WriteNext(k, bytes))
	}

	assert.Nil(b, writer.Close())

	reader, err := sstables.NewSSTableReader(sstables.ReadBasePath(tmpDir), sstables.ReadWithKeyComparator(cmp))
	assert.Nil(b, err)
	defer func(reader sstables.SSTableReaderI) {
		assert.Nil(b, reader.Close())
	}(reader)

	fullScanTable(b, reader)
}

func fullScanTable(b *testing.B, reader sstables.SSTableReaderI) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := reader.Scan()
		assert.Nil(b, err)
		for {
			_, _, err := scanner.Next()
			if err == sstables.Done {
				break
			}
		}
		b.SetBytes(int64(reader.MetaData().TotalBytes))
	}
}
