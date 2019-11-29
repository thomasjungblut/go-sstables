package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"os"
	"sort"
	"testing"
)

// we only do some e2e tests with randoms here, the main logic is tested in the priority queue and its unit tests

func TestSSTableMergeSingleFileEndToEnd(t *testing.T) {
	writeFilesMergeAndCheck(t, 1, 1000)
}

func TestSSTableMergeTwoFilesEndToEnd(t *testing.T) {
	writeFilesMergeAndCheck(t, 2, 500)
}

func TestSSTableMergeThreeFilesEndToEnd(t *testing.T) {
	writeFilesMergeAndCheck(t, 3, 300)
}

func TestSSTableMergeFourFilesEndToEnd(t *testing.T) {
	writeFilesMergeAndCheck(t, 4, 250)
}

func TestSSTableMergeFiveFilesEndToEnd(t *testing.T) {
	writeFilesMergeAndCheck(t, 5, 200)
}

func writeFilesMergeAndCheck(t *testing.T, numFiles int, numElementsPerFile int) {

	var expectedNumbers []int
	var iterators []SSTableIteratorI

	for i := 0; i < numFiles; i++ {
		writer, err := newTestSSTableStreamWriter()
		assert.Nil(t, err)
		defer os.RemoveAll(writer.opts.basePath)
		expectedNumbers = append(expectedNumbers, streamedWriteElements(t, writer, numElementsPerFile)...)
		iterators = append(iterators, getFullScanIterator(t, writer.opts.basePath))
	}

	outWriter, err := newTestSSTableStreamWriter()
	assert.Nil(t, err)
	defer os.RemoveAll(outWriter.opts.basePath)

	merger := NewSSTableMerger(skiplist.BytesComparator)
	err = merger.Merge(iterators, outWriter)
	assert.Nil(t, err)
	sort.Ints(expectedNumbers)
	assertRandomAndSequentialRead(t, outWriter.opts.basePath, expectedNumbers)
}
