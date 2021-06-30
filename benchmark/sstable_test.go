package benchmark

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"os"
	"testing"
)

func BenchmarkSimpleSSTableCopy(b *testing.B) {

	cmp := skiplist.BytesComparator
	reader, err := sstables.NewSSTableReader(sstables.ReadBasePath("C:\\Users\\thomas.jungblut\\Downloads\\sstable303505830"), sstables.ReadWithKeyComparator(cmp))
	assert.Nil(b, err)
	defer reader.Close()

	os.RemoveAll("C:\\Users\\thomas.jungblut\\Downloads\\sstableout")
	os.Mkdir("C:\\Users\\thomas.jungblut\\Downloads\\sstableout", os.ModePerm)
	writer, err := sstables.NewSSTableStreamWriter(sstables.WriteBasePath("C:\\Users\\thomas.jungblut\\Downloads\\sstableout"), sstables.WithKeyComparator(cmp))
	assert.Nil(b, err)
	assert.Nil(b, writer.Open())
	defer writer.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := reader.Scan()
		assert.Nil(b, err)
		for {
			_, _, err := scanner.Next()
			if err == sstables.Done {
				break
			} else {
				assert.Nil(b, err)
			}
			//assert.Nil(b, writer.WriteNext(k, v))
		}
		b.SetBytes(int64(reader.MetaData().TotalBytes))
	}
}
