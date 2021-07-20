package main

import (
	"github.com/thomasjungblut/go-sstables/memstore"
	"github.com/thomasjungblut/go-sstables/sstables"
	"log"
	"os"
)

func main() {
	path := "/tmp/sstable-ms-ex/"
	defer os.RemoveAll(path)

	ms := memstore.NewMemStore()
	ms.Add([]byte{1}, []byte{1})
	ms.Add([]byte{2}, []byte{2})
	ms.Upsert([]byte{1}, []byte{2})
	ms.Delete([]byte{2})
	ms.DeleteIfExists([]byte{3})
	value, _ := ms.Get([]byte{1})
	log.Printf("value for key 1: %d", value) // yields 2

	size := ms.EstimatedSizeInBytes()
	log.Printf("memstore size is %d bytes", size) // yields 3

	ms.Flush(sstables.WriteBasePath(path))

}
