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

[YCSB](https://github.com/brianfrankcooper/YCSB) is a popular benchmarking system for databases, there is a Go port
where SimpleDB can be hooked in. The binding lives in the `sdb` branch of this fork:
https://github.com/tjungblu/go-ycsb/tree/sdb

Clone the fork, build it with `make` and then load and run a workload:

```
$ bin/go-ycsb load gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb
$ bin/go-ycsb run gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb
```

SimpleDB runs as an embedded database in the YCSB process. It writes to `/tmp/gosstables-simpledb` by default, which
might be a tmpfs, thus set `gosstables.path` to a directory on the disk you want to benchmark. Add
`-p gosstables.asyncWal=true` to disable the fsync of the write-ahead log on every write.

The [simpledb workload](https://github.com/tjungblu/go-ycsb/blob/sdb/workloads/simpledb) loads one million 1 KB
records, then runs one million operations: 20% inserts, 30% reads and 50% updates with uniformly distributed keys. The
memstore is reduced to 128 MB to exercise flushes and compactions more often. The read-only run uses the same workload
with `-p readproportion=1 -p insertproportion=0 -p updateproportion=0` on the database from the async runs. The
database was evicted from the page cache before every run.

go-ycsb uses a single client thread by default, the throughput is thus the inverse of the average latency:

| Run                       | Throughput     | Write latency (avg / p99) | Read latency (avg / p99) |
|---------------------------|----------------|---------------------------|--------------------------|
| Load, async WAL           | 65,265 ops/s   | 6 µs / 12 µs              |                          |
| Mixed, async WAL          | 91,468 ops/s   | 3-6 µs / 8-16 µs          | 15 µs / 32 µs            |
| Read-only                 | 92,917 ops/s   |                           | 9 µs / 20 µs             |
| Load, synced WAL          | 101 ops/s      | 9.9 ms / 18.4 ms          |                          |
| Mixed, synced WAL         | 136 ops/s      | 10.5 ms / 19.3 ms         | 34 µs / 155 µs           |

With the synced WAL, every insert and update waits for an fsync, which takes about 10 ms here. That's slower than in
the SimpleDB write benchmark above, likely because btrfs has more to commit with the flushes and compactions running
alongside. The loading thus takes 2h45m instead of 15 seconds. The reads have a slower tail in the synced mix too
(p99 155 µs instead of 32 µs). That's not lock contention, a single client thread never reads and writes at the same
time. It's more likely caused by the background flushes and compactions, which ran for two hours instead of eleven
seconds, but this wasn't investigated further.

The detailed results, async WAL:

```
$ bin/go-ycsb load gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb -p gosstables.asyncWal=true
Run finished, takes 15.322108927s
INSERT - Takes(s): 15.3, Count: 1000000, OPS: 65264.8, Avg(us): 6, Min(us): 3, Max(us): 61855, 50th(us): 5, 90th(us): 6, 95th(us): 7, 99th(us): 12, 99.9th(us): 35, 99.99th(us): 471

$ bin/go-ycsb run gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb -p gosstables.asyncWal=true
Run finished, takes 10.932801012s
INSERT - Takes(s): 10.9, Count: 199936, OPS: 18287.6, Avg(us): 6, Min(us): 4, Max(us): 58591, 50th(us): 6, 90th(us): 7, 95th(us): 8, 99th(us): 16, 99.9th(us): 37, 99.99th(us): 453
READ   - Takes(s): 10.9, Count: 299967, OPS: 27437.0, Avg(us): 15, Min(us): 3, Max(us): 668, 50th(us): 16, 90th(us): 21, 95th(us): 22, 99th(us): 32, 99.9th(us): 52, 99.99th(us): 117
UPDATE - Takes(s): 10.9, Count: 500097, OPS: 45742.5, Avg(us): 3, Min(us): 1, Max(us): 70591, 50th(us): 3, 90th(us): 5, 95th(us): 6, 99th(us): 8, 99.9th(us): 19, 99.99th(us): 79
```

Read-only:

```
$ bin/go-ycsb run gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb \
    -p readproportion=1 -p insertproportion=0 -p updateproportion=0
Run finished, takes 10.762387944s
READ   - Takes(s): 10.8, Count: 1000000, OPS: 92916.4, Avg(us): 9, Min(us): 1, Max(us): 730, 50th(us): 13, 90th(us): 14, 95th(us): 15, 99th(us): 20, 99.9th(us): 41, 99.99th(us): 89
```

Synced WAL:

```
$ bin/go-ycsb load gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb
Run finished, takes 2h45m7.100491439s
INSERT - Takes(s): 9907.1, Count: 1000000, OPS: 100.9, Avg(us): 9888, Min(us): 5252, Max(us): 1104895, 50th(us): 9703, 90th(us): 10959, 95th(us): 13143, 99th(us): 18351, 99.9th(us): 34655, 99.99th(us): 50655

$ bin/go-ycsb run gosstables -P workloads/simpledb -p gosstables.path=/var/tmp/ycsb-simpledb
Run finished, takes 2h2m19.019612045s
INSERT - Takes(s): 7339.0, Count: 199233, OPS: 27.1, Avg(us): 10459, Min(us): 6132, Max(us): 103039, 50th(us): 9863, 90th(us): 11855, 95th(us): 13863, 99th(us): 19327, 99.9th(us): 40191, 99.99th(us): 70719
READ   - Takes(s): 7339.0, Count: 300302, OPS: 40.9, Avg(us): 34, Min(us): 4, Max(us): 773, 50th(us): 29, 90th(us): 44, 95th(us): 97, 99th(us): 155, 99.9th(us): 191, 99.99th(us): 230
UPDATE - Takes(s): 7339.0, Count: 500465, OPS: 68.2, Avg(us): 10463, Min(us): 5476, Max(us): 259455, 50th(us): 9879, 90th(us): 11855, 95th(us): 13879, 99th(us): 19247, 99.9th(us): 38655, 99.99th(us): 77567
```
