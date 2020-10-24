package main

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
)

func sstable_main() {
	path := "/tmp/sstable_example/"
	os.MkdirAll(path, 0777)
	defer os.RemoveAll(path)

	mainWriteSimple(path)

	mainSimpleRead(path)
}

func mainSimpleRead(path string) {
	reader, err := sstables.NewSSTableReader(
		sstables.ReadBasePath("/tmp/sstable_example/"),
		sstables.ReadWithKeyComparator(skiplist.BytesComparator))
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer reader.Close()

	metadata := reader.MetaData()
	log.Printf("reading table with %d records, minKey %d and maxKey %d", metadata.NumRecords, metadata.MinKey, metadata.MaxKey)

	contains := reader.Contains([]byte{1})
	val, err := reader.Get([]byte{1})
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	log.Printf("table contains value for key? %t = %d", contains, val)

	it, err := reader.ScanRange([]byte{1}, []byte{2})
	for {
		k, v, err := it.Next()
		// io.EOF signals that no records are left to be read
		if err == sstables.Done {
			break
		}

		if err != nil {
			log.Fatalf("error: %v", err)
		}

		log.Printf("%d = %d", k, v)
	}
}

func mainWriteSimple(path string) {
	writer, err := sstables.NewSSTableSimpleWriter(
		sstables.WriteBasePath(path),
		sstables.WithKeyComparator(skiplist.BytesComparator))
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	skipListMap := skiplist.NewSkipListMap(skiplist.BytesComparator)
	skipListMap.Insert([]byte{1}, []byte{1})
	skipListMap.Insert([]byte{2}, []byte{2})
	skipListMap.Insert([]byte{3}, []byte{3})

	err = writer.WriteSkipListMap(skipListMap)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func mainWriteStreaming() {
	path := "/tmp/sstable_example/"
	os.MkdirAll(path, 0777)
	defer os.RemoveAll(path)

	writer, err := sstables.NewSSTableStreamWriter(
		sstables.WriteBasePath(path),
		sstables.WithKeyComparator(skiplist.BytesComparator))
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = writer.Open()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = writer.WriteNext([]byte{1}, []byte{1})
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = writer.WriteNext([]byte{2}, []byte{2})
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = writer.WriteNext([]byte{3}, []byte{3})
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = writer.Close()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}
