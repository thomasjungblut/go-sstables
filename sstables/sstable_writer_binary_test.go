package sstables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkipListSimpleWriteBinaryHappyPath(t *testing.T) {
	writer, err := newTestSSTableSimpleWriterBinary()
	require.Nil(t, err)
	// TODO defer cleanWriterDir(t, writer.streamWriter)

	list := TEST_ONLY_NewSkipListMapWithElements([]int{1, 2, 3, 4, 5, 6, 7})
	err = writer.WriteSkipListMap(list)
	require.Nil(t, err)

	idxLoader := SortedMapIndexLoader{ReadBufferSize: 4096, Binary: true}
	reader, err := NewSSTableReader(
		ReadBasePath(writer.streamWriter.opts.basePath),
		ReadWithKeyComparator(writer.streamWriter.opts.keyComparator),
		WithDataLoader(NewBinaryDataLoader()),
		ReadIndexLoader(&idxLoader),
	)
	require.Nil(t, err)
	defer closeReader(t, reader)
	assert.Equal(t, 7, int(reader.MetaData().NumRecords))
	assert.Equal(t, 0, int(reader.MetaData().NullValues))
	assertContentMatchesSkipList(t, reader, list)
}
