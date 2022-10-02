package sstables

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
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
	var iterators []SSTableMergeIteratorContext
	for i := 0; i < numFiles; i++ {
		writer, err := newTestSSTableStreamWriter()
		require.Nil(t, err)
		defer cleanWriterDir(t, writer)
		expectedNumbers = append(expectedNumbers, streamedWriteElements(t, writer, numElementsPerFile)...)
		reader, iterator := getFullScanIterator(t, writer.opts.basePath)
		defer closeReader(t, reader)
		iterators = append(iterators, NewMergeIteratorContext(i, iterator))
	}

	outWriter, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, outWriter)

	merger := NewSSTableMerger(skiplist.BytesComparator{})
	err = merger.Merge(iterators, outWriter)
	require.Nil(t, err)
	sort.Ints(expectedNumbers)
	assertRandomAndSequentialRead(t, outWriter.opts.basePath, expectedNumbers)
}

func TestSSTableMergeAndCompactSingleFileEndToEnd(t *testing.T) {
	writeMergeCompactAndCheck(t, 1, 1000)
}

func TestSSTableMergeAndCompactTwoFilesEndToEnd(t *testing.T) {
	writeMergeCompactAndCheck(t, 2, 500)
}

func TestSSTableMergeAndCompactThreeFilesEndToEnd(t *testing.T) {
	writeMergeCompactAndCheck(t, 3, 300)
}

func TestSSTableMergeAndCompactFourFilesEndToEnd(t *testing.T) {
	writeMergeCompactAndCheck(t, 4, 250)
}

func TestSSTableMergeAndCompactFiveFilesEndToEnd(t *testing.T) {
	writeMergeCompactAndCheck(t, 5, 200)
}

func writeMergeCompactAndCheck(t *testing.T, numFiles int, numElementsPerFile int) {
	var writersToClean []*SSTableStreamWriter
	defer cleanWriterDirs(t, &writersToClean)
	var expectedNumbers []int
	var iterators []SSTableMergeIteratorContext

	for i := 0; i < numFiles; i++ {
		writer, err := newTestSSTableStreamWriter()
		require.Nil(t, err)
		writersToClean = append(writersToClean, writer)

		// all numbers returned here should be the exact same
		expectedNumbers = streamedWriteAscendingIntegers(t, writer, numElementsPerFile)
		reader, iterator := getFullScanIterator(t, writer.opts.basePath)
		defer closeReader(t, reader)
		iterators = append(iterators, NewMergeIteratorContext(i, iterator))
	}

	outWriter, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	writersToClean = append(writersToClean, outWriter)

	merger := NewSSTableMerger(skiplist.BytesComparator{})
	err = merger.MergeCompact(iterators, outWriter,
		func(key []byte, values [][]byte, context []int) ([]byte, []byte) {
			// there should be as many values as we have files
			assert.Equal(t, numFiles, len(values))
			assert.Equal(t, numFiles, len(context))
			// always pick the first one
			return key, values[0]
		})
	require.Nil(t, err)
	sort.Ints(expectedNumbers)
	assertRandomAndSequentialRead(t, outWriter.opts.basePath, expectedNumbers)
}

func TestOverlappingMergeAndCompact(t *testing.T) {
	expectedNumbersUnique := make(map[int]interface{})
	var expectedNumbers []int
	var iterators []SSTableMergeIteratorContext

	numFiles := 5
	numElementsPerFile := 250

	for i := 0; i < numFiles; i++ {
		writer, err := newTestSSTableStreamWriter()
		require.Nil(t, err)
		defer cleanWriterDir(t, writer)

		// since the ranges overlap we have to make them unique to get our final expected set of numbers
		currentNumbers := streamedWriteAscendingIntegersWithStart(t, writer, i*25, numElementsPerFile)
		for _, e := range currentNumbers {
			if _, ok := expectedNumbersUnique[e]; !ok {
				expectedNumbers = append(expectedNumbers, e)
				expectedNumbersUnique[e] = e
			}
		}
		reader, iterator := getFullScanIterator(t, writer.opts.basePath)
		defer closeReader(t, reader)
		iterators = append(iterators, NewMergeIteratorContext(i, iterator))
	}

	outWriter, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, outWriter)

	reduceFunc := func(key []byte, values [][]byte, context []int) ([]byte, []byte) {
		// always pick the first one
		return key, values[0]
	}

	merger := NewSSTableMerger(skiplist.BytesComparator{})
	err = merger.MergeCompact(iterators, outWriter, reduceFunc)
	require.Nil(t, err)
	sort.Ints(expectedNumbers)
	assertRandomAndSequentialRead(t, outWriter.opts.basePath, expectedNumbers)
}

func TestMergeAndCompactEmptyResult(t *testing.T) {
	var iterators []SSTableMergeIteratorContext

	numFiles := 5
	numElementsPerFile := 250
	for i := 0; i < numFiles; i++ {
		writer, err := newTestSSTableStreamWriter()
		require.Nil(t, err)
		defer cleanWriterDir(t, writer)

		streamedWriteAscendingIntegersWithStart(t, writer, i*25, numElementsPerFile)
		reader, iterator := getFullScanIterator(t, writer.opts.basePath)
		defer closeReader(t, reader)
		iterators = append(iterators, NewMergeIteratorContext(i, iterator))
	}

	outWriter, err := newTestSSTableStreamWriter()
	require.Nil(t, err)
	defer cleanWriterDir(t, outWriter)

	reduceFunc := func(key []byte, values [][]byte, context []int) ([]byte, []byte) {
		// ignoring all, should result in an empty merged file
		return nil, nil
	}

	merger := NewSSTableMerger(skiplist.BytesComparator{})
	err = merger.MergeCompact(iterators, outWriter, reduceFunc)
	require.Nil(t, err)
	assertRandomAndSequentialRead(t, outWriter.opts.basePath, []int{})
}
