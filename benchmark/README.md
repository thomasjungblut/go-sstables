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
BenchmarkRecordIO/RecordSize1k-20                 545428              1926 ns/op         531.62 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIO/RecordSize10k-20                 92306             14636 ns/op         699.64 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIO/RecordSize100k-20                28035             43979 ns/op        2328.41 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIO/RecordSize1M-20                   3870            303124 ns/op        3378.16 MB/s         216 B/op          2 allocs/op

BenchmarkRecordIO/GzipRecordSize1k-20               6294            168901 ns/op           6.06 MB/s      815360 B/op         22 allocs/op
BenchmarkRecordIO/GzipRecordSize10k-20              4285            267298 ns/op          38.31 MB/s      825084 B/op         22 allocs/op
BenchmarkRecordIO/GzipRecordSize100k-20              674           1804549 ns/op          56.75 MB/s     1012862 B/op         24 allocs/op
BenchmarkRecordIO/GzipRecordSize1M-20                 69          16908007 ns/op          60.56 MB/s     2823314 B/op         27 allocs/op

BenchmarkRecordIO/SnappyRecordSize1k-20           444480              2550 ns/op         401.52 MB/s        1496 B/op          3 allocs/op
BenchmarkRecordIO/SnappyRecordSize10k-20           70164             19496 ns/op         525.23 MB/s       12504 B/op          3 allocs/op
BenchmarkRecordIO/SnappyRecordSize100k-20          19162             60501 ns/op        1692.54 MB/s      123096 B/op          3 allocs/op
BenchmarkRecordIO/SnappyRecordSize1M-20             2610            443578 ns/op        2308.50 MB/s     1196255 B/op          3 allocs/op

BenchmarkRecordIO/SyncRecordSize1k-20                764           1381452 ns/op           0.74 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIO/SyncRecordSize10k-20               762           1746557 ns/op           5.86 MB/s         216 B/op          2 allocs/op
BenchmarkRecordIO/SyncRecordSize100k-20              680           2915550 ns/op          35.12 MB/s         217 B/op          2 allocs/op
BenchmarkRecordIO/SyncRecordSize1M-20                602           2034315 ns/op         503.36 MB/s         217 B/op          2 allocs/op
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 57.838s
```
