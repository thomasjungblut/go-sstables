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