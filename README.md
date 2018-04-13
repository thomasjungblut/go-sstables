## go-sstables

`go-sstables` is a Go library that contains NoSQL database building blocks like a sequential record format (recordio) and a sorted string table (sstables, indexed using a skip list).

It will come with some protobuf convenience bindings and compression support.

## Installation

> go get -d github.com/thomasjungblut/go-sstables

## Using SSTables

coming soon...

## Using RecordIO

coming soon...

### Benchmark 

For writes, here's a simple write benchmark on a SSD.
Basically writing a thousand records of varying sizes, with normal buffered writes and sync writes after each record.

```
$ make bench
go test -v -benchmem -bench=. ./benchmark
BenchmarkWriteRecordSize1k-12                100          10245670 ns/op         101.90 MB/s        2873 B/op         21 allocs/op
BenchmarkWriteRecordSize10k-12               100          21366487 ns/op         480.19 MB/s        2934 B/op         20 allocs/op
BenchmarkWriteRecordSize100k-12               20         151194930 ns/op         677.40 MB/s        8092 B/op         20 allocs/op
BenchmarkWriteRecordSize1m-12                  2        1427030100 ns/op         734.81 MB/s      527056 B/op         20 allocs/op
BenchmarkWriteSyncRecordSize1k-12              1        2848388700 ns/op           0.37 MB/s        3864 B/op         23 allocs/op
BenchmarkWriteSyncRecordSize10k-12             1        3073590800 ns/op           3.34 MB/s       13016 B/op         21 allocs/op
BenchmarkWriteSyncRecordSize100k-12            1        5032531600 ns/op          20.35 MB/s      109272 B/op         21 allocs/op
BenchmarkWriteSyncRecordSize1m-12              1        11963311000 ns/op         87.65 MB/s     1051416 B/op         22 allocs/op
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 32.571s
```
