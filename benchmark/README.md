### RecordIO Benchmark

Here's a simple write benchmark on a NVME SSD with 3.5 GB/s sequential read and 2.3 GB/s sequential write throughput.
Basically writing a thousand records of varying sizes, with normal buffered writes and sync writes after each record.

Keep in mind that compression should not save any IO, since we're compressing random data. So the below table actually
measures the algorithmic overhead (plus the inefficiency of encoding random data).

```
$ make bench
go test -v -benchmem -bench=. ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkRecordIOWriteRecordSize1k
BenchmarkRecordIOWriteRecordSize1k-20                     181730              6614 ns/op         154.82 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIOWriteRecordSize10k
BenchmarkRecordIOWriteRecordSize10k-20                    114516             14225 ns/op         719.87 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIORecordIOWriteRecordSize100k
BenchmarkRecordIORecordIOWriteRecordSize100k-20            30376             39735 ns/op        2577.05 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIOWriteRecordSize1m
BenchmarkRecordIOWriteRecordSize1m-20                       3748            315101 ns/op        3327.74 MB/s         216 B/op          2 allocs/op

BenchmarkRecordIOWriteGzipRecordSize1k
BenchmarkRecordIOWriteGzipRecordSize1k-20                   6519            183771 ns/op           5.57 MB/s      815360 B/op         22 allocs/op
BenchmarkRecordIOWriteGzipRecordSize10k
BenchmarkRecordIOWriteGzipRecordSize10k-20                  4239            295723 ns/op          34.63 MB/s      825085 B/op         22 allocs/op
BenchmarkRecordIOWriteGzipRecordSize100k
BenchmarkRecordIOWriteGzipRecordSize100k-20                  631           1799259 ns/op          56.91 MB/s     1012864 B/op         24 allocs/op
BenchmarkRecordIOWriteGzipRecordSize1m
BenchmarkRecordIOWriteGzipRecordSize1m-20                     66          17476348 ns/op          60.00 MB/s     4912270 B/op         28 allocs/op

BenchmarkRecordIOWriteSnappyRecordSize1k
BenchmarkRecordIOWriteSnappyRecordSize1k-20               166664              7489 ns/op         136.74 MB/s        1496 B/op          3 allocs/op
BenchmarkRecordIOWriteSnappyRecordSize10k
BenchmarkRecordIOWriteSnappyRecordSize10k-20               65203             16912 ns/op         605.48 MB/s       12504 B/op          3 allocs/op
BenchmarkRecordIOWriteSnappyRecordSize100k
BenchmarkRecordIOWriteSnappyRecordSize100k-20              20797             62038 ns/op        1650.61 MB/s      123096 B/op          3 allocs/op
BenchmarkRecordIOWriteSnappyRecordSize1m
BenchmarkRecordIOWriteSnappyRecordSize1m-20                 2349            464893 ns/op        2255.52 MB/s     1229022 B/op          3 allocs/op

BenchmarkRecordIOWriteSyncRecordSize1k
BenchmarkRecordIOWriteSyncRecordSize1k-20                    972           1285536 ns/op           0.80 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIOWriteSyncRecordSize10k
BenchmarkRecordIOWriteSyncRecordSize10k-20                   844           1436104 ns/op           7.13 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIOWriteSyncRecordSize100k
BenchmarkRecordIOWriteSyncRecordSize100k-20                  747           1433848 ns/op          71.42 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIOWriteSyncRecordSize1m
BenchmarkRecordIOWriteSyncRecordSize1m-20                    603           1990142 ns/op         526.88 MB/s         217 B/op          2 allocs/op
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 57.838s
```
