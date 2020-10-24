## Using MemStore

Memstore acts like a sorted dictionary that can be flushed into an SSTable representation on disk. 
It allows you to add, update, retrieve and delete elements by their key, of which both are represented by byte slices.

A simple example below illustrates all functionality of the memstore: 

```go
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
log.Printf("memstore size in bytes: %d", size) // yields 3

ms.Flush(sstables.WriteBasePath(path))
``` 

You can get the full example from [examples/memstore.go](/examples/memstore.go).
