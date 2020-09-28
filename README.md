[![Build Status](https://travis-ci.org/thomasjungblut/go-sstables.svg?branch=master)](https://travis-ci.org/thomasjungblut/go-sstables)

## go-sstables

`go-sstables` is a Go library that contains NoSQL database building blocks like a sequential record format (recordio),
a sorted string table (sstable) and a memory store (memstore) that stores key/value pairs in memory using a skip list.

While plain `[]byte` are at the core of this library, there are wrappers and bindings for protobuf to enable more convenient serialization. 

## Installation

> go get -d github.com/thomasjungblut/go-sstables

## Using SkipListMap

Whenever you find yourself in need of a sorted list/map for range scans or ordered iteration, you can resort to a `SkipList`. 
The `SkipListMap` in this project is based on [LevelDBs skiplist](https://github.com/google/leveldb/blob/master/db/skiplist.h) and super easy to use:

```go
skipListMap := skiplist.NewSkipListMap(skiplist.IntComparator)
skipListMap.Insert(13, 91)
skipListMap.Insert(3, 1)
skipListMap.Insert(5, 2)
log.Printf("size: %d", skipListMap.Size())

it, _ := skipListMap.Iterator()
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}

log.Printf("starting at key: %d", 5)
it, _ = skipListMap.IteratorStartingAt(5)
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}

log.Printf("between: %d and %d", 8, 50)
it, _ = skipListMap.IteratorBetween(8, 50)
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}
```

You can supply any kind of element and comparator to sort arbitrary structs and primitives. 
You can get the full example from [examples/skiplist.go](examples/skiplist.go).
 
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

You can get the full example from [examples/memstore.go](examples/memstore.go).

## Using SSTables

SSTables allow you to store a large amount of key/value data on disk and query it efficiently by key or by key ranges. Unsurprisingly, this very format is at the heart of many NoSQL databases (i.e. HBase and Cassandra).

The flavor that is implemented in this library favours small keys and large values (eg. images), since it stores the key index in memory and the values remain on disk. 
A fully out-of-core version or secondary indices are currently not implemented. Features like bloom filter for faster key look-ups are already in place, so it is not too difficult to add later on.

### Writing an SSTable

All files (key index, bloom filter, metadata info) that are necessary to store an SSTable are found under a given `basePath` in your filesystem.
Which means that we can just start writing by creating a directory and appending some key/value pairs. 

In the previous section we already saw how to transform a `memstore` into an sstable.   
This example shows how to stream already sorted data into a file:

```go

path := "/tmp/sstable_example/"
os.MkdirAll(path, 0777)
defer os.RemoveAll(path)

writer, err := sstables.NewSSTableStreamWriter(
    sstables.WriteBasePath(path),
    sstables.WithKeyComparator(skiplist.BytesComparator))
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
    sstables.WithKeyComparator(skiplist.BytesComparator))
if err != nil { log.Fatalf("error: %v", err) }

skipListMap := skiplist.NewSkipListMap(skiplist.BytesComparator)
skipListMap.Insert([]byte{1}, []byte{1})
skipListMap.Insert([]byte{2}, []byte{2})
skipListMap.Insert([]byte{3}, []byte{3})

err = writer.WriteSkipListMap(skipListMap)
if err != nil { log.Fatalf("error: %v", err) }
```
 
### Reading an SSTable

Reading can be done by using having a path and the respective comparator. 
Below example will show what metadata is available, how to get values and check if they exist and how to do a range scan.

```go
reader, err := sstables.NewSSTableReader(
    sstables.ReadBasePath("/tmp/sstable_example/"),
    sstables.ReadWithKeyComparator(skiplist.BytesComparator))
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
    if err == sstables.Done {
        break
    }
    if err != nil { log.Fatalf("error: %v", err) }

    log.Printf("%d = %d", k, v)
}

```

You can get the full example from [examples/sstables.go](examples/sstables.go).

### Merging two (or more) SSTables

One of the great features of SSTables is that you can merge them in linear time and in a sequential fashion, which needs only constant amount of space.  

In this library, this can be easily composed here via full-table scanners and and a writer to output the resulting merged table: 

```go
var iterators []SSTableIteratorI
for i := 0; i < numFiles; i++ {
	reader, err := NewSSTableReader(
    		ReadBasePath(sstablePath),
    		ReadWithKeyComparator(skiplist.BytesComparator))
	if err != nil { log.Fatalf("error: %v", err) }
    defer reader.Close()
	
	it, err := reader.Scan()
	if err != nil { log.Fatalf("error: %v", err) }
	
    iterators = append(iterators, it)   
}

writer, err := sstables.NewSSTableSimpleWriter(
    sstables.WriteBasePath(path),
    sstables.WithKeyComparator(skiplist.BytesComparator))
if err != nil { log.Fatalf("error: %v", err) }

merger := NewSSTableMerger(skiplist.BytesComparator)
// merge takes care of opening/closing itself
err = merger.Merge(iterators, writer)  
if err != nil { log.Fatalf("error: %v", err) }

// do something with the merged sstable
```

The merge logic itself is based on a heap, so it can scale to thousands of files easily.

## Using RecordIO

RecordIO allows you to write sequential key/value entities into a flat file and is heavily inspired by [Hadoop's SequenceFile](https://wiki.apache.org/hadoop/SequenceFile). 
Writing a `recordio` file using Protobuf and snappy compression can be done as follows. Here's the simple proto file we use:

```protobuf
message HelloWorld {
    string message = 1;
}
```

Writing in Go then becomes this:

```go
writer, err := recordio.NewCompressedProtoWriterWithPath(path, recordio.CompressionTypeSnappy)
if err != nil { log.Fatalf("error: %v", err) }

err = writer.Open()
if err != nil { log.Fatalf("error: %v", err) }

record := &proto.HelloWorld{Message: "Hello World"}
recordOffset, err := writer.Write(record)
if err != nil { log.Fatalf("error: %v", err) }

log.Printf("wrote a record at offset of %d bytes", recordOffset)

err = writer.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

Reading the same file can be done like this:

```go
reader, err := recordio.NewProtoReaderWithPath(path)
if err != nil { log.Fatalf("error: %v", err) }

err = reader.Open()
if err != nil { log.Fatalf("error: %v", err) }

for {
    record := &proto.HelloWorld{}
    _, err := reader.ReadNext(record)
    // io.EOF signals that no records are left to be read
    if err == io.EOF {
        break
    }

    if err != nil {
        log.Fatalf("error: %v", err)
    }

    log.Printf("%s", record.GetMessage())
}

err = reader.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

SSTables support random reads of backing values, thus recordio also supports it using its `mmap` implementation:

```go
reader, err := recordio.NewMMapProtoReaderWithPath(path)
if err != nil { log.Fatalf("error: %v", err) }

err = reader.Open()
if err != nil { log.Fatalf("error: %v", err) }

record := &proto.HelloWorld{}
_, err = reader.ReadNextAt(record, 8)
if err != nil { log.Fatalf("error: %v", err) }

log.Printf("Reading message at offset 8: %s", record.GetMessage())

err = reader.Close()
if err != nil { log.Fatalf("error: %v", err) }
``` 

You can get the full example from [examples/recordio.go](examples/recordio.go).


### RecordIO Benchmark 

Here's a simple write benchmark on a NVME SSD.
Basically writing a thousand records of varying sizes, with normal buffered writes and sync writes after each record.

Keep in mind that compression should not save any IO, since we're compressing random data.
So the below table actually measures the algorithmic overhead (plus the inefficiency of encoding random data).

```
$ make bench
go test -v -benchmem -bench=. ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkWriteRecordSize1k-12                        100          15419600 ns/op          67.71 MB/s      228427 B/op       3032 allocs/op
BenchmarkWriteRecordSize10k-12                        50          22147332 ns/op         463.26 MB/s      228542 B/op       3031 allocs/op
BenchmarkWriteRecordSize100k-12                       20          69468215 ns/op        1474.34 MB/s      233684 B/op       3032 allocs/op
BenchmarkWriteRecordSize1m-12                          3         410104933 ns/op        2556.90 MB/s      577805 B/op       3031 allocs/op

BenchmarkWriteGzipRecordSize1k-12                      5         215676720 ns/op           4.97 MB/s    815375665 B/op     23191 allocs/op
BenchmarkWriteGzipRecordSize10k-12                     3         447410966 ns/op          22.99 MB/s    825096056 B/op     23036 allocs/op
BenchmarkWriteGzipRecordSize100k-12                    1        2685462700 ns/op          38.16 MB/s    1012975064 B/op    25035 allocs/op
BenchmarkWriteGzipRecordSize1m-12                      1        26986574400 ns/op         38.87 MB/s    4913367912 B/op    29992 allocs/op

BenchmarkWriteSnappyRecordSize1k-12                  100          16192124 ns/op          64.79 MB/s     1508384 B/op       4032 allocs/op
BenchmarkWriteSnappyRecordSize10k-12                  50         118395154 ns/op          86.70 MB/s    12516719 B/op       4034 allocs/op
BenchmarkWriteSnappyRecordSize100k-12                 20          87928185 ns/op        1164.92 MB/s    123115587 B/op      4062 allocs/op
BenchmarkWriteSnappyRecordSize1m-12                    2         657253450 ns/op        1595.50 MB/s    1229583544 B/op     4541 allocs/op

BenchmarkWriteSyncRecordSize1k-12                      1        1224008600 ns/op           0.85 MB/s      229560 B/op       3036 allocs/op
BenchmarkWriteSyncRecordSize10k-12                     1        1588437800 ns/op           6.46 MB/s      238712 B/op       3034 allocs/op
BenchmarkWriteSyncRecordSize100k-12                    1        1495825600 ns/op          68.47 MB/s      334968 B/op       3034 allocs/op
BenchmarkWriteSyncRecordSize1m-12                      1        2219904700 ns/op         472.36 MB/s     1277048 B/op       3034 allocs/op

PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 58.505s
```

We can now compare this against the V2 format where I implemented vint compression for the record headers. For very small records that are also uncompressed this reduced the static 20 byte size down to 5 bytes. The drawback is with bigger compressed and uncompressed sizes of a record the encoding can take up to 30 bytes - you have to encode several gigabytes, if not terabytes, as a single record for it to reach that state. Additionally the record header buffer is now preserved between calls, which saves us roughly 1000 allocs/op in our benchmark.

Here's the benchmark on the exact same hardware for the V2 format:

```
$ make bench
go test -v -benchmem -bench=. ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkWriteRecordSize1k-12                        200          10895390 ns/op          94.54 MB/s      192021 B/op       2000 allocs/op
BenchmarkWriteRecordSize10k-12                       100          14459813 ns/op         708.59 MB/s      192134 B/op       2000 allocs/op
BenchmarkWriteRecordSize100k-12                       30          50120730 ns/op        2043.21 MB/s      195642 B/op       2000 allocs/op
BenchmarkWriteRecordSize1m-12                          5         309121100 ns/op        3392.14 MB/s      402289 B/op       2004 allocs/op

BenchmarkWriteGzipRecordSize1k-12                      5         209883920 ns/op           5.05 MB/s    815340088 B/op     22159 allocs/op
BenchmarkWriteGzipRecordSize10k-12                     3         349456566 ns/op          29.40 MB/s    825060482 B/op     22009 allocs/op
BenchmarkWriteGzipRecordSize100k-12                    1        2615651500 ns/op          39.17 MB/s    1012941928 B/op    24030 allocs/op
BenchmarkWriteGzipRecordSize1m-12                      1        26212039500 ns/op         40.02 MB/s    4913287544 B/op    28209 allocs/op

BenchmarkWriteSnappyRecordSize1k-12                  100          10432194 ns/op          99.31 MB/s     1472051 B/op       3000 allocs/op
BenchmarkWriteSnappyRecordSize10k-12                 100          21735654 ns/op         471.67 MB/s    12480251 B/op       3002 allocs/op
BenchmarkWriteSnappyRecordSize100k-12                 20          65215745 ns/op        1570.45 MB/s    123078350 B/op      3015 allocs/op
BenchmarkWriteSnappyRecordSize1m-12                    2         562678400 ns/op        1863.65 MB/s    1229549780 B/op     3542 allocs/op

BenchmarkWriteSyncRecordSize1k-12                      1        1168090500 ns/op           0.88 MB/s      196072 B/op       2026 allocs/op
BenchmarkWriteSyncRecordSize10k-12                     1        1250856200 ns/op           8.19 MB/s      205304 B/op       2025 allocs/op
BenchmarkWriteSyncRecordSize100k-12                    1        1414026800 ns/op          72.42 MB/s      301480 B/op       2024 allocs/op
BenchmarkWriteSyncRecordSize1m-12                      1        2119873300 ns/op         494.64 MB/s     1243560 B/op       2024 allocs/op

PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 60.939s
```

### Updating dependencies through Go Modules

[General overview of modules](https://github.com/golang/go/wiki/Modules)

One can update dependencies via:

```
go get -u <repo url>
go mod vendor
```

### Releasing the Go Module

[General Guidance](https://github.com/golang/go/wiki/Modules#releasing-modules-all-versions)

In short, run these commands:

```
make unit-test
make release
git push --tags 
```
