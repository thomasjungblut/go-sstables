# Benchmarks

## Setup

The numbers below were measured on:

* Intel Core i9-10900K (10 cores, 20 threads), 64 GB RAM
* Samsung SSD 980 PRO 2TB (NVMe), btrfs on top of LUKS disk encryption
* Linux 7.2.7 (Fedora 44), Go 1.26.1

The 980 PRO is a PCIe 4.0 drive, but this platform only offers PCIe 3.0: the link runs at 8 GT/s x4, which caps the
throughput at about 3.5 GB/s instead of the 7 GB/s read and 5.1 GB/s write from the drive's spec sheet. LUKS and btrfs
add their own overhead on top, most visibly in the fsync latency.

The benchmarks are meant to measure the disk and not the page cache:

* All writes are measured with fsync: every writer fsyncs its files (and their directories) on `Close`, and that
  fsync is part of the measured time. None of the write numbers stop at the page cache, the data is durably on the
  drive when the benchmark stops the clock.
* Files are created in a `bench-*` directory next to the benchmark package, never in the OS temp directory, as `/tmp`
  is a tmpfs (memory) on many Linux distributions. Set `GO_SSTABLES_BENCH_DIR` to benchmark a different disk.
* Before every timed read iteration, the files are evicted from the page cache using
  `posix_fadvise(POSIX_FADV_DONTNEED)`, the reads thus come from the disk. This is only implemented on Linux (amd64 and
  arm64), other platforms will read from the page cache.

Run the benchmarks with:

```
$ make bench           # RecordIO and SSTable, this includes files and memstores up to 8 GB
$ make bench-simpledb
```

The below tables contain the median of three runs for RecordIO and a single run for SSTables, restricted to files of
up to 1 GB (2 GB for the SSTable flush and index benchmarks), e.g. with
`go test -run xxx -benchmem -count 3 -bench 'BenchmarkRecordIO(Proto)?Read/(32|256|1024)mb' ./benchmark`.
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) is useful to compare runs.

## RecordIO

All records contain random data. Compression can't save any IO on random data, so the compressed numbers show the
algorithmic overhead of the compression.

### Write

Writes records of the given size into a single file, the throughput includes the final fsync when closing the file.
The sync column calls `WriteSync` for every record, which is dominated by the fsync latency of about 7 ms of btrfs on
LUKS.

| Record size | Uncompressed | Snappy    | Gzip     | LZW      | Sync (fsync per record) |
|-------------|--------------|-----------|----------|----------|-------------------------|
| 1 KB        | 2338 MB/s    | 1515 MB/s | 21 MB/s  | 91 MB/s  | 6.78 ms/record          |
| 10 KB       | 2475 MB/s    | 1953 MB/s | 83 MB/s  | 91 MB/s  | 6.95 ms/record          |
| 100 KB      | 2468 MB/s    | 2179 MB/s | 76 MB/s  | 92 MB/s  | 6.86 ms/record          |
| 1 MB        | 2425 MB/s    | 1742 MB/s | 74 MB/s  | 88 MB/s  | 7.23 ms/record          |

Writes of up to 100 KB records don't allocate, bigger records exceed the largest bucket of the internal buffer pool.

### Sequential read

Reading a whole file of 1 KB records with `ReadNext`, with and without the protobuf reader on top:

| File size | `FileReader`          | Proto `Reader`        |
|-----------|-----------------------|-----------------------|
| 32 MB     | 2127 MB/s             | 1537 MB/s             |
| 256 MB    | 2296 MB/s             | 1786 MB/s             |
| 1 GB      | 2473 MB/s             | 1655 MB/s             |

The record size matters much more than the file size, as there is a fixed cost for every record. Reading 16 MB
files with `ReadNext` (`FileReader`) and `ReadNextAt` (`MMapReader`), from front to back:

| Record size | `FileReader` | +Snappy   | +Gzip     | +LZW     | `MMapReader` | +Snappy   | +Gzip     | +LZW     |
|-------------|--------------|-----------|-----------|----------|--------------|-----------|-----------|----------|
| 16 B        | 110 MB/s     | 76 MB/s   | 70 MB/s   | 42 MB/s  | 521 MB/s     | 530 MB/s  | 115 MB/s  | 75 MB/s  |
| 128 B       | 478 MB/s     | 313 MB/s  | 229 MB/s  | 103 MB/s | 1466 MB/s    | 1200 MB/s | 422 MB/s  | 132 MB/s |
| 1 KB        | 1400 MB/s    | 1062 MB/s | 861 MB/s  | 158 MB/s | 2120 MB/s    | 2114 MB/s | 1335 MB/s | 172 MB/s |
| 64 KB       | 1791 MB/s    | 1673 MB/s | 1664 MB/s | 178 MB/s | 2438 MB/s    | 2411 MB/s | 2238 MB/s | 188 MB/s |

Every record read allocates its returned slice, compression doesn't add further allocations.

### Skipping and seeking

`SkipNext` skips records without reading them. `SeekNext` on the `MMapReader` searches the next record from an
arbitrary offset, the benchmark seeks from one byte after every record start. Both on 16 MB uncompressed files:

| Record size | `SkipNext` | `SeekNext` (mmap) |
|-------------|------------|-------------------|
| 16 B        | 135 MB/s   | 402 MB/s          |
| 128 B       | 567 MB/s   | 1001 MB/s         |
| 1 KB        | 1549 MB/s  | 1385 MB/s         |
| 64 KB       | 2097 MB/s  | 1530 MB/s         |

## SSTable

The SSTables contain 1 KB values with 20 byte keys, written from a memstore.

### Write

Flushing a memstore into an SSTable, including the fsync of all files:

| Memstore size | Throughput | Time    |
|---------------|------------|---------|
| 32 MB         | 370 MB/s   | 91 ms   |
| 256 MB        | 623 MB/s   | 431 ms  |
| 1 GB          | 721 MB/s   | 1.49 s  |
| 2 GB          | 733 MB/s   | 2.93 s  |

### Full table scan

Opening the SSTable (which loads the index with the default loader) and scanning all records:

| SSTable size | Records | Index load | Scan    | Throughput |
|--------------|---------|------------|---------|------------|
| 32 MB        | 27,949  | 7 ms       | 24 ms   | 948 MB/s   |
| 256 MB       | 223,585 | 54 ms      | 164 ms  | 1109 MB/s  |
| 1 GB         | 894,338 | 213 ms     | 636 ms  | 1129 MB/s  |

### Index types

A 2 GB SSTable (1.79 mio. records, 89 MB index) with the different index loaders. The random read column shows the
latency of `Get` on random keys.

| Index loader | Index load | Scan throughput | Random read |
|--------------|------------|-----------------|-------------|
| skiplist     | 998 ms     | 808 MB/s        | 127 µs      |
| slice        | 444 ms     | 1176 MB/s       | 107 µs      |
| map          | 741 ms     | 990 MB/s        | 89 µs       |
| disk         | 3 ms       | 1165 MB/s       | 115 µs      |

The disk index doesn't load anything upfront, but needs to read the index from disk on every `Get`.

### Random reads

`Get` on random keys with the default index loader. The page cache is only dropped once before the run, it warms up
during the run like it would in any long-running process. Only the 2 GB tables above are large enough to be read
mostly from disk (about 10k reads), the smaller ones are cached after a fraction of the run (185k to 926k reads) and
their latency is dominated by the in-memory index lookup:

| SSTable size | Random read |
|--------------|-------------|
| 32 MB        | 1.3 µs      |
| 256 MB       | 1.8 µs      |
| 1 GB         | 5.4 µs      |

## SimpleDB

All records have 14 byte keys and 1 KB values.

### Write

20 goroutines writing concurrently. By default every `Put` fsyncs the write-ahead log before it returns, the async WAL
skips that fsync and trades durability of the most recent writes for speed. The time per `Put` is the total time
divided by the number of `Put`s of all goroutines, thus the inverse of the throughput:

| WAL              | Time per `Put` | Throughput        |
|------------------|----------------|-------------------|
| sync (default)   | 6.57 ms        | 152 `Put`/s       |
| async            | 2.4 µs         | ~420,000 `Put`/s  |

The WAL fsyncs are not batched across goroutines (no group commit), every synced `Put` waits for its own fsync. The
synced writes are thus capped by the fsync latency of the disk, independent of the number of writers, and a single
writer waits about 20 times the fsync latency when 20 goroutines write at once.

### Read

`Get` latency, cycling through all keys of a database with the given number of records. For the memstore variant all
records are still in the memstore, which is where recently written records are read from. For the sstable variant,
the database was reopened, so all records are read from the SSTables. The page cache is only dropped once before the
run and the whole data set is read many times over, the numbers thus mostly show the lookup cost with a warm cache:

| Records | Memstore | SSTable |
|---------|----------|---------|
| 1,000   | 332 ns   | 561 ns  |
| 10,000  | 428 ns   | 586 ns  |
| 100,000 | 513 ns   | 698 ns  |

### YCSB

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

