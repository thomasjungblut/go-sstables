## go-sstables

`go-sstables` is a Go library that contains NoSQL database building blocks like a sequential record format (recordio) and a sorted string table (sstables, indexed using a skip list).

It will come with some protobuf convenience bindings.

## Installation

> go get -d github.com/thomasjungblut/go-sstables

## Using SSTables

coming soon...

## Using RecordIO

Writing a `recordio` file with snappy compression can be done like this:

```go

writer, err := recordio.NewCompressedFileWriterWithPath(path, recordio.CompressionTypeSnappy)
if err != nil {
    log.Fatalf("error: %v", err)
}
if writer.Open() != nil {
    log.Fatalf("error: %v", err)
}
recordOffset, err := writer.Write([]byte("Hello World!"))
if err != nil {
    log.Fatalf("error: %v", err)
}
log.Printf("wrote a record at offset of %d bytes", recordOffset)
if writer.Close() != nil {
    log.Fatalf("error: %v", err)
}

```

Reading the same file can be done like this:

```go

reader, err := recordio.NewFileReaderWithPath(path)
if err != nil {
    log.Fatalf("error: %v", err)
}

if reader.Open() != nil {
    log.Fatalf("error: %v", err)
}

for {
    record, err := reader.ReadNext()
    // io.EOF signals that no records are left to be read
    if err == io.EOF {
        break
    }

    if err != nil {
        log.Fatalf("error: %v", err)
    }

    log.Printf("%s", string(record))
}

if reader.Close() != nil {
    log.Fatalf("error: %v", err)
}

```

You can get the full example from [examples/recordio.go](examples/recordio.go).


### Benchmark 

Here's a simple write benchmark on a SSD.
Basically writing a thousand records of varying sizes, with normal buffered writes and sync writes after each record.

Keep in mind that compression should not save IO, since we're compressing random data.
So the below table actually measures the algorithmic overhead (plus the inefficiency of encoding random data).

```
$ make bench
go test -v -benchmem -bench=. ./benchmark
BenchmarkWriteRecordSize1k-12                        100          11239780 ns/op          92.89 MB/s        2873 B/op         21 allocs/op
BenchmarkWriteRecordSize10k-12                       100          17620454 ns/op         582.28 MB/s        2934 B/op         20 allocs/op
BenchmarkWriteRecordSize100k-12                       20         155644855 ns/op         658.04 MB/s        8092 B/op         20 allocs/op
BenchmarkWriteRecordSize1m-12                          2        1675481850 ns/op         625.85 MB/s      527056 B/op         20 allocs/op

BenchmarkWriteGzipRecordSize1k-12                      5         216805000 ns/op           4.94 MB/s    815139985 B/op     19026 allocs/op
BenchmarkWriteGzipRecordSize10k-12                     3         360006633 ns/op          28.58 MB/s    824870253 B/op     19021 allocs/op
BenchmarkWriteGzipRecordSize100k-12                    1        2665999400 ns/op          38.44 MB/s    1012751032 B/op    21025 allocs/op
BenchmarkWriteGzipRecordSize1m-12                      1        26641003900 ns/op         39.37 MB/s    4913092120 B/op    25133 allocs/op

BenchmarkWriteSnappyRecordSize1k-12                  100          12236774 ns/op          85.73 MB/s     1282845 B/op       1020 allocs/op
BenchmarkWriteSnappyRecordSize10k-12                 100          24819458 ns/op         413.59 MB/s    12291027 B/op       1022 allocs/op
BenchmarkWriteSnappyRecordSize100k-12                 20         168408515 ns/op         608.22 MB/s    122889036 B/op      1034 allocs/op
BenchmarkWriteSnappyRecordSize1m-12                    2        1534538000 ns/op         683.36 MB/s    1229337144 B/op     1176 allocs/op

BenchmarkWriteSyncRecordSize1k-12                      1        3023668200 ns/op           0.35 MB/s        3864 B/op         23 allocs/op
BenchmarkWriteSyncRecordSize10k-12                     1        3145649300 ns/op           3.26 MB/s       13016 B/op         21 allocs/op
BenchmarkWriteSyncRecordSize100k-12                    1        4400795000 ns/op          23.27 MB/s      109272 B/op         21 allocs/op
BenchmarkWriteSyncRecordSize1m-12                      1        10895929300 ns/op         96.24 MB/s     1051352 B/op         21 allocs/op
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 75.228s
```
