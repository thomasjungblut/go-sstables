### RecordIO Write Benchmark

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

BenchmarkRecordIO/RecordSize1k-20                3883150               290 ns/op        3527.15 MB/s           0 B/op         0 allocs/op
BenchmarkRecordIO/RecordSize10k-20                413266              2857 ns/op        3584.41 MB/s           0 B/op          0 allocs/op
BenchmarkRecordIO/RecordSize100k-20                43568             28391 ns/op        3606.75 MB/s           0 B/op          0 allocs/op
BenchmarkRecordIO/RecordSize1M-20                   4221            275834 ns/op        3712.38 MB/s           0 B/op          0 allocs/op

BenchmarkRecordIO/GzipRecordSize1k-20               7500            136226 ns/op           7.52 MB/s      815138 B/op         20 allocs/op
BenchmarkRecordIO/GzipRecordSize10k-20              4999            217475 ns/op          47.09 MB/s      824866 B/op         20 allocs/op
BenchmarkRecordIO/GzipRecordSize100k-20              688           1730058 ns/op          59.19 MB/s     1012643 B/op         22 allocs/op
BenchmarkRecordIO/GzipRecordSize1M-20                 69          16388058 ns/op          62.48 MB/s     2823085 B/op         25 allocs/op

BenchmarkRecordIO/SnappyRecordSize1k-20          1668518               690 ns/op        1483.80 MB/s        1280 B/op          1 allocs/op
BenchmarkRecordIO/SnappyRecordSize10k-20          222228              5038 ns/op        2032.75 MB/s       12288 B/op          1 allocs/op
BenchmarkRecordIO/SnappyRecordSize100k-20          27940             43273 ns/op        2366.38 MB/s      122880 B/op          1 allocs/op
BenchmarkRecordIO/SnappyRecordSize1M-20             2402            475310 ns/op        2154.38 MB/s     1196034 B/op          1 allocs/op

BenchmarkRecordIO/SyncRecordSize1k-20                795           1511583 ns/op           0.68 MB/s           0 B/op          0 allocs/op
BenchmarkRecordIO/SyncRecordSize10k-20               861           1616945 ns/op           6.33 MB/s           0 B/op          0 allocs/op
BenchmarkRecordIO/SyncRecordSize100k-20              684           1672742 ns/op          61.22 MB/s           1 B/op          0 allocs/op
BenchmarkRecordIO/SyncRecordSize1M-20                603           2034682 ns/op         503.27 MB/s           1 B/op          0 allocs/op

PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 57.838s
```

### SSTable Read Benchmark

Below is a quick read (sequential full table scan) benchmark of an SSTable, writing a gigabyte of random data and then
reading it off the disk. That includes reading the index into memory and iterating over all records:

```
$ make bench
go test -v -benchmem -bench=SSTable ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkSSTableRead
BenchmarkSSTableRead-20    	       1	1959477000 ns/op	 564.96 MB/s
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 9.018s
```

### SSTable Write Benchmark

A common requirement is to flush a memstore to a sstable, here is the benchmark for various memstore sizes:

```
$ make bench
go test -v -benchmem -bench=SSTable ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkSSTableMemstoreFlush
BenchmarkSSTableMemstoreFlush/32mb-20         	      22	  50409068 ns/op	 665.67 MB/s
BenchmarkSSTableMemstoreFlush/64mb-20         	      12	  94149400 ns/op	 712.80 MB/s
BenchmarkSSTableMemstoreFlush/128mb-20        	       6	 186079033 ns/op	 721.30 MB/s
BenchmarkSSTableMemstoreFlush/256mb-20        	       3	 367057467 ns/op	 731.32 MB/s
BenchmarkSSTableMemstoreFlush/512mb-20        	       2	 740347450 ns/op	 725.16 MB/s
BenchmarkSSTableMemstoreFlush/1024mb-20       	       1	1506872700 ns/op	 712.56 MB/s
BenchmarkSSTableMemstoreFlush/2048mb-20       	       1	2972302200 ns/op	 722.50 MB/s
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 19.018s
```

