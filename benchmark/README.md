## RecordIO

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

## SSTable

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
BenchmarkSSTableRead-20                        1        1465768700 ns/op         753.11 MB/s    1412781624 B/op  9437704 allocs/op
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
BenchmarkSSTableMemstoreFlush/32mb-20                 25          41657792 ns/op         805.51 MB/s    52187665 B/op     198903 allocs/op
BenchmarkSSTableMemstoreFlush/64mb-20                 14          76066743 ns/op         882.25 MB/s    95117949 B/op     397605 allocs/op
BenchmarkSSTableMemstoreFlush/128mb-20                 7         149681114 ns/op         896.70 MB/s    180954365 B/op    794972 allocs/op
BenchmarkSSTableMemstoreFlush/256mb-20                 4         289055850 ns/op         928.67 MB/s    352624252 B/op   1589710 allocs/op
BenchmarkSSTableMemstoreFlush/512mb-20                 2         574371500 ns/op         934.71 MB/s    695955168 B/op   3179176 allocs/op
BenchmarkSSTableMemstoreFlush/1024mb-20                1        1141287400 ns/op         940.82 MB/s    1382598768 B/op  6358078 allocs/op
BenchmarkSSTableMemstoreFlush/2048mb-20                1        2264936500 ns/op         948.14 MB/s    2755888184 B/op 12715874 allocs/op
PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 20.650s
```

## SimpleDB

### SimpleDB Read Benchmark

```
$ make bench-simpledb
go test -v -benchmem -bench=SimpleDB ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkSimpleDBReadLatency
BenchmarkSimpleDBReadLatency/100-20              3547354               315 ns/op        47746.66 MB/s         86 B/op          4 allocs/op
BenchmarkSimpleDBReadLatency/1000-20             2721176               414 ns/op        36230.03 MB/s         99 B/op          4 allocs/op
BenchmarkSimpleDBReadLatency/10000-20            1842484               632 ns/op        23849.40 MB/s        281 B/op          4 allocs/op
BenchmarkSimpleDBReadLatency/100000-20             78621             13038 ns/op        1149.06 MB/s       33951 B/op         20 allocs/op

PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 83.801s
```

### SimpleDB Write Benchmark

```
$ make bench-simpledb
go test -v -benchmem -bench=SimpleDB ./benchmark
goos: windows
goarch: amd64
pkg: github.com/thomasjungblut/go-sstables/benchmark
BenchmarkSimpleDBWriteLatency/100-20                1058           8343294 ns/op        1864.65 MB/s    27093557 B/op       5182 allocs/op
BenchmarkSimpleDBWriteLatency/1000-20               1034           8458750 ns/op        1810.21 MB/s    26472343 B/op       5061 allocs/op
BenchmarkSimpleDBWriteLatency/10000-20               540           4569896 ns/op        1709.64 MB/s    13695999 B/op       2582 allocs/op
BenchmarkSimpleDBWriteLatency/100000-20             1110           8667185 ns/op        1899.54 MB/s    28439698 B/op       5443 allocs/op
BenchmarkSimpleDBWriteLatency/1000000-20            1110           9002177 ns/op        1818.23 MB/s    28434465 B/op       5442 allocs/op

PASS
ok      github.com/thomasjungblut/go-sstables/benchmark 83.801s
```

### SimpleDB YCSB Benchmark

(benchmark below aws done on different hardware specs than those above, namely a Toshiba m.2 NVME with 2.5GB/s sequential read and 1.5GB/s sequential write)

YCSB is a popular benchmarking system for databases, gladly there is a Go port where one can hook SimpleDB in. 
You can find the whole code in the sdb branch on my fork: https://github.com/tjungblu/go-ycsb/tree/sdb

Clone the fork, then build with the makefile or:

> go build -o bin/go-ycsb cmd/go-ycsb/*

Which then allows you to load/run using:

> bin/go-ycsb load gosstables -P workloads/workloada
> 
> bin/go-ycsb run gosstables -P workloads/workloada

SimpleDB here is used as an embedded database on your local disk, which by default writes to `/tmp/gosstables-simpledb`.  

To test the performance for longer I've created a [workload scenario](https://github.com/tjungblu/go-ycsb/blob/sdb/workloads/simpledb) to 
do 20% insert, 30% read and 50% updates on 1KB records. MemStore size is reduced to only 128mb to exercise the disk paths and compaction more often.

The results are with asynchronous WAL (no fsync):
```
[tjungblu ~/git/go-ycsb]$ bin/go-ycsb load gosstables -P workloads/simpledb 
2022/09/09 15:37:35 done with recovery, starting with fresh WAL directory in /home/tjungblu/simpledb.tmp/wal
***************** properties *****************
"insertproportion"="0.2"
"gosstables.asyncWal"="true"
"command"="load"
"fieldcount"="10"
"requestdistribution"="uniform"
"dotransactions"="false"
"operationcount"="1000000"
"scanproportion"="0"
"gosstables.path"="/home/tjungblu/simpledb.tmp"
"updateproportion"="0.5"
"readproportion"="0.3"
"readallfields"="true"
"recordcount"="1000000"
"gosstables.memstoreSizeBytes"="134217728"
"workload"="core"
**********************************************
INSERT - Takes(s): 10.0, Count: 394419, OPS: 39440.6, Avg(us): 11, Min(us): 6, Max(us): 11431, 99th(us): 31, 99.9th(us): 62, 99.99th(us): 1645
INSERT - Takes(s): 20.0, Count: 779775, OPS: 38986.9, Avg(us): 11, Min(us): 6, Max(us): 11431, 99th(us): 31, 99.9th(us): 57, 99.99th(us): 1687
INSERT - Takes(s): 27.6, Count: 1000000, OPS: 36259.0, Avg(us): 13, Min(us): 6, Max(us): 25727, 99th(us): 39, 99.9th(us): 103, 99.99th(us): 1815

[tjungblu ~/git/go-ycsb]$ bin/go-ycsb run gosstables -P workloads/simpledb 
2022/09/09 15:38:53 found 3 existing sstables, starting recovery...
2022/09/09 15:38:54 done with recovery, starting with fresh WAL directory in /home/tjungblu/simpledb.tmp/wal
***************** properties *****************
"gosstables.path"="/home/tjungblu/simpledb.tmp"
"gosstables.asyncWal"="true"
"workload"="core"
"requestdistribution"="uniform"
"fieldcount"="10"
"updateproportion"="0.5"
"command"="run"
"dotransactions"="true"
"operationcount"="1000000"
"readproportion"="0.3"
"gosstables.memstoreSizeBytes"="134217728"
"insertproportion"="0.2"
"readallfields"="true"
"recordcount"="1000000"
"scanproportion"="0"
**********************************************
INSERT - Takes(s): 10.0, Count: 85005, OPS: 8500.2, Avg(us): 14, Min(us): 7, Max(us): 8719, 99th(us): 42, 99.9th(us): 82, 99.99th(us): 1698
READ   - Takes(s): 10.0, Count: 127529, OPS: 12751.3, Avg(us): 36, Min(us): 8, Max(us): 13695, 99th(us): 87, 99.9th(us): 152, 99.99th(us): 508
UPDATE - Takes(s): 10.0, Count: 212649, OPS: 21261.8, Avg(us): 9, Min(us): 2, Max(us): 4061, 99th(us): 32, 99.9th(us): 52, 99.99th(us): 167
INSERT - Takes(s): 20.0, Count: 168353, OPS: 8417.5, Avg(us): 14, Min(us): 7, Max(us): 13111, 99th(us): 40, 99.9th(us): 73, 99.99th(us): 1698
READ   - Takes(s): 20.0, Count: 252125, OPS: 12605.5, Avg(us): 38, Min(us): 7, Max(us): 17151, 99th(us): 85, 99.9th(us): 132, 99.99th(us): 474
UPDATE - Takes(s): 20.0, Count: 420114, OPS: 21003.9, Avg(us): 9, Min(us): 2, Max(us): 14255, 99th(us): 31, 99.9th(us): 46, 99.99th(us): 127
INSERT - Takes(s): 24.0, Count: 200279, OPS: 8340.8, Avg(us): 14, Min(us): 7, Max(us): 13111, 99th(us): 39, 99.9th(us): 70, 99.99th(us): 1677
READ   - Takes(s): 24.0, Count: 300283, OPS: 12505.3, Avg(us): 39, Min(us): 7, Max(us): 17151, 99th(us): 87, 99.9th(us): 129, 99.99th(us): 474
UPDATE - Takes(s): 24.0, Count: 499438, OPS: 20799.1, Avg(us): 9, Min(us): 2, Max(us): 14255, 99th(us): 31, 99.9th(us): 45, 99.99th(us): 130
```

Without the async WAL (default without supplied argument), you get much worse numbers which is expected as we're calling fsync after every operation:

```
[tjungblu ~/git/go-ycsb]$ bin/go-ycsb load gosstables -P workloads/simpledb 
2022/09/09 15:41:51 done with recovery, starting with fresh WAL directory in /home/tjungblu/simpledb.tmp/wal
***************** properties *****************
"workload"="core"
"readproportion"="0.3"
"updateproportion"="0.5"
"dotransactions"="false"
"command"="load"
"insertproportion"="0.2"
"recordcount"="1000000"
"gosstables.memstoreSizeBytes"="134217728"
"operationcount"="1000000"
"fieldcount"="10"
"readallfields"="true"
"requestdistribution"="uniform"
"scanproportion"="0"
"gosstables.path"="/home/tjungblu/simpledb.tmp"
**********************************************
INSERT - Takes(s): 10.0, Count: 4988, OPS: 498.9, Avg(us): 1982, Min(us): 1252, Max(us): 44511, 99th(us): 5071, 99.9th(us): 6835, 99.99th(us): 44511
INSERT - Takes(s): 20.0, Count: 9955, OPS: 497.8, Avg(us): 1989, Min(us): 1252, Max(us): 44511, 99th(us): 5035, 99.9th(us): 6835, 99.99th(us): 21679
INSERT - Takes(s): 30.0, Count: 14942, OPS: 498.1, Avg(us): 1988, Min(us): 1252, Max(us): 44511, 99th(us): 5027, 99.9th(us): 6443, 99.99th(us): 21679
INSERT - Takes(s): 40.0, Count: 19985, OPS: 499.6, Avg(us): 1982, Min(us): 1252, Max(us): 44511, 99th(us): 4995, 99.9th(us): 6459, 99.99th(us): 21679
...
INSERT - Takes(s): 2020.2, Count: 1000000, OPS: 495.0, Avg(us): 2001, Min(us): 1234, Max(us): 982527, 99th(us): 5031, 99.9th(us): 6867, 99.99th(us): 25311

[tjungblu ~/git/go-ycsb]$ bin/go-ycsb run gosstables -P workloads/simpledb 
2022/09/09 16:16:23 found 3 existing sstables, starting recovery...
2022/09/09 16:16:24 done with recovery, starting with fresh WAL directory in /home/tjungblu/simpledb.tmp/wal
***************** properties *****************
"recordcount"="1000000"
"command"="run"
"insertproportion"="0.2"
"updateproportion"="0.5"
"readproportion"="0.3"
"workload"="core"
"operationcount"="1000000"
"scanproportion"="0"
"fieldcount"="10"
"gosstables.path"="/home/tjungblu/simpledb.tmp"
"requestdistribution"="uniform"
"dotransactions"="true"
"readallfields"="true"
"gosstables.memstoreSizeBytes"="134217728"
**********************************************
INSERT - Takes(s): 10.0, Count: 1456, OPS: 145.6, Avg(us): 1939, Min(us): 1279, Max(us): 9567, 99th(us): 4903, 99.9th(us): 7199, 99.99th(us): 9567
READ   - Takes(s): 10.0, Count: 2267, OPS: 226.8, Avg(us): 49, Min(us): 21, Max(us): 363, 99th(us): 114, 99.9th(us): 321, 99.99th(us): 363
UPDATE - Takes(s): 10.0, Count: 3634, OPS: 363.5, Avg(us): 1928, Min(us): 1264, Max(us): 26799, 99th(us): 4923, 99.9th(us): 9135, 99.99th(us): 26799
INSERT - Takes(s): 20.0, Count: 2890, OPS: 144.5, Avg(us): 1972, Min(us): 1279, Max(us): 9567, 99th(us): 5007, 99.9th(us): 6231, 99.99th(us): 9567
READ   - Takes(s): 20.0, Count: 4376, OPS: 218.9, Avg(us): 50, Min(us): 12, Max(us): 384, 99th(us): 142, 99.9th(us): 306, 99.99th(us): 384
UPDATE - Takes(s): 20.0, Count: 7195, OPS: 359.8, Avg(us): 1941, Min(us): 1264, Max(us): 26799, 99th(us): 4927, 99.9th(us): 7167, 99.99th(us): 26527
...
INSERT - Takes(s): 1418.6, Count: 199794, OPS: 140.8, Avg(us): 2001, Min(us): 1233, Max(us): 52703, 99th(us): 5063, 99.9th(us): 6751, 99.99th(us): 19407
READ   - Takes(s): 1418.6, Count: 300617, OPS: 211.9, Avg(us): 65, Min(us): 8, Max(us): 2163, 99th(us): 251, 99.9th(us): 493, 99.99th(us): 728
UPDATE - Takes(s): 1418.6, Count: 499589, OPS: 352.2, Avg(us): 1982, Min(us): 1213, Max(us): 104127, 99th(us): 5023, 99.9th(us): 6663, 99.99th(us): 19615

```

With fsync we're about 100x slower than without, which becomes especially noticeable in the latencies and the similarly reduced throughput. 
In this mixed benchmark, we also see the lock contention of the writes to cause the read performance to degrade significantly, compare the above with a workload of 100% reads:

```
[tjungblu ~/git/go-ycsb]$ bin/go-ycsb run gosstables -P workloads/simpledb 
2022/09/09 16:42:32 found 7 existing sstables, starting recovery...
2022/09/09 16:42:34 done with recovery, starting with fresh WAL directory in /home/tjungblu/simpledb.tmp/wal
***************** properties *****************
"readproportion"="1"
"gosstables.memstoreSizeBytes"="134217728"
"scanproportion"="0"
"readallfields"="true"
"workload"="core"
"updateproportion"="0"
"requestdistribution"="uniform"
"dotransactions"="true"
"recordcount"="1000000"
"insertproportion"="0"
"gosstables.path"="/home/tjungblu/simpledb.tmp"
"operationcount"="1000000"
"command"="run"
"fieldcount"="10"
**********************************************
READ   - Takes(s): 10.0, Count: 329358, OPS: 32929.0, Avg(us): 28, Min(us): 3, Max(us): 16911, 99th(us): 69, 99.9th(us): 94, 99.99th(us): 166
READ   - Takes(s): 20.0, Count: 654551, OPS: 32724.9, Avg(us): 29, Min(us): 3, Max(us): 16911, 99th(us): 71, 99.9th(us): 103, 99.99th(us): 212
READ   - Takes(s): 30.0, Count: 965836, OPS: 32192.1, Avg(us): 29, Min(us): 3, Max(us): 20143, 99th(us): 75, 99.9th(us): 119, 99.99th(us): 236
Run finished, takes 31.057865283s
READ   - Takes(s): 31.1, Count: 1000000, OPS: 32197.8, Avg(us): 29, Min(us): 3, Max(us): 20143, 99th(us): 75, 99.9th(us): 118, 99.99th(us): 234
```

