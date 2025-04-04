## Using SSTables

SSTables allow you to store a large amount of key/value data on disk and query it efficiently by key or by key ranges. Unsurprisingly, this very format is at the heart of many NoSQL databases (i.e. HBase and Cassandra).

The flavor that is implemented in this library favours small keys and large values (eg. images), since it stores the key index in memory and the values remain on disk. 
A fully out-of-core version or secondary indices are currently not implemented. Features like bloom filter for faster key look-ups are already in place, so it is not too difficult to add later on.

### Writing an SSTable

All files (key index, bloom filter, metadata info) that are necessary to store an SSTable are found under a given `basePath` in your filesystem.
Which means that we can just start writing by creating a directory and appending some key/value pairs. 

In the previous section we already saw how to transform a `memstore` into a sstable.   
This example shows how to stream already sorted data into a file:

```go

path := "/tmp/sstable_example/"
os.MkdirAll(path, 0777)
defer os.RemoveAll(path)

writer, err := sstables.NewSSTableStreamWriter(
    sstables.WriteBasePath(path),
    sstables.WithKeyComparator(skiplist.BytesComparator{}))
if err != nil { log.Fatalf("error: %v", err) }

err = writer.Open()
if err != nil { log.Fatalf("error: %v", err) }

// error checks omitted
err = writer.WriteNext([]byte{1}, []byte{1})
err = writer.WriteNext([]byte{2}, []byte{2})
err = writer.WriteNext([]byte{3}, []byte{3})

err = writer.Close()
if err != nil { log.Fatalf("error: %v", err) }

```

Keep in mind that streaming data requires a comparator (for safety), which will error on writes that are out of order.

Since that is somewhat cumbersome, you can also directly write a full skip list using the `SimpleWriter`:

```go
path := "/tmp/sstable_example/"
os.MkdirAll(path, 0777)
defer os.RemoveAll(path)

writer, err := sstables.NewSSTableSimpleWriter(
    sstables.WriteBasePath(path),
    sstables.WithKeyComparator(skiplist.BytesComparator{}))
if err != nil { log.Fatalf("error: %v", err) }

skipListMap := skiplist.NewSkipListMap(skiplist.BytesComparator{})
skipListMap.Insert([]byte{1}, []byte{1})
skipListMap.Insert([]byte{2}, []byte{2})
skipListMap.Insert([]byte{3}, []byte{3})

err = writer.WriteSkipListMap(skipListMap)
if err != nil { log.Fatalf("error: %v", err) }
```
 
### Reading an SSTable

Reading can be done by only passing a path to the sstable reader. 
Below example will show what metadata is available, how to get values and check if they exist and how to do a range scan.

```go
reader, err := sstables.NewSSTableReader(sstables.ReadBasePath("/tmp/sstable_example/"))
if err != nil { log.Fatalf("error: %v", err) }
defer reader.Close()

metadata := reader.MetaData()
log.Printf("reading table with %d records, minKey %d and maxKey %d", metadata.NumRecords, metadata.MinKey, metadata.MaxKey)

contains := reader.Contains([]byte{1})
val, err := reader.Get([]byte{1})
if err != nil { log.Fatalf("error: %v", err) }
log.Printf("table contains value for key? %t = %d", contains, val)

it, err := reader.ScanRange([]byte{1}, []byte{2})
for {
    k, v, err := it.Next()
    // io.EOF signals that no records are left to be read
    if errors.is(err, sstables.Done) {
        break
    }
    if err != nil { log.Fatalf("error: %v", err) }

    log.Printf("%d = %d", k, v)
}

```

You can get the full example from [examples/sstables.go](/_examples/sstables.go).

### Index Types

Recently, we have been introducing different types of indices to facilitate faster loading and lookup times. You can now supply a `loader` when creating a reader using:

```go

reader, err := sstables.NewSSTableReader(
    ReadBasePath(sstablePath),
    ReadWithKeyComparator(skiplist.BytesComparator{}),
    ReadIndexLoader(&sstables.SliceKeyIndexLoader{ReadBufferSize: 4096}))
```

This allows you to trade-off several factors, here's the current available set of index loaders:
* SkipListIndexLoader - current default, loads slowly, high memory usage, quick range scans, O(log n) key lookups
* SliceKeyIndexLoader - loads quickly, compact but high memory usage, quick range scans, O(log n) key lookups
* MapKeyIndexLoader - loads quickly, very high memory usage, quick range scans, O(1) amortized key lookups
* DiskIndexLoader (EXPERIMENTAL and under further development) - loads instantly, no additional memory usage, slow range scans, slow key lookups

Implementing your own loader also allows you to create a new type of index yourself, that suits your requirements the best.

### Merging two (or more) SSTables

One of the great features of SSTables is that you can merge them in linear time and in a sequential fashion, which needs only constant amount of space.  

In this library, this can be easily composed here via full-table scanners and a writer to output the resulting merged table: 

```go
var iterators []sstables.SSTableMergeIteratorContext
for i := 0; i < numFiles; i++ {
    reader, err := sstables.NewSSTableReader(
            ReadBasePath(sstablePath),
            ReadWithKeyComparator(skiplist.BytesComparator{}))
    if err != nil { log.Fatalf("error: %v", err) }
    defer reader.Close()
    
    it, err := reader.Scan()
    if err != nil { log.Fatalf("error: %v", err) }

    iterators = append(iterators, sstables.NewMergeIteratorContext(i, it))
}

writer, err := sstables.NewSSTableSimpleWriter(
    sstables.WriteBasePath(path),
    sstables.WithKeyComparator(skiplist.BytesComparator{}))
if err != nil { log.Fatalf("error: %v", err) }

merger := NewSSTableMerger(skiplist.BytesComparator{})
// merge takes care of opening/closing itself
err = merger.Merge(iterators, outWriter)

if err != nil { log.Fatalf("error: %v", err) }

// do something with the merged sstable
```

The merge logic itself is based on a heap, so it can scale to thousands of files easily.

There might be some cases where you want to have the ability to compact while you're merging the files. This is where `MergeCompact` comes in handy, there you can supply a simple reduce function to directly compact the values for a given key. Below example illustrates this functionality:

```go
reduceFunc := func(key []byte, values [][]byte, context []int) ([]byte, []byte) {
    // always pick the first one
    return key, values[0]
}

merger := sstables.NewSSTableMerger(skiplist.BytesComparator{})
err = merger.MergeCompact(iterators, outWriter, reduceFunc)
```

The context gives you the ability to figure out which value originated from which file/iterator. The context slice is parallel to the values slice, so the value at index 0 originated from the context at index 0.
