# SimpleDB

SimpleDB is a simple embedded database built using the primitives in this repository. It's an example project to show
how the different pieces can fit together and is not meant to be a production-ready database
like [RocksDB](https://github.com/facebook/rocksdb) or [LevelDB](https://github.com/google/leveldb).

## Interface

There are three methods, namely `Get`, `Put` and `Delete` to query and add data to the database. For simplicity's sake
the whole interface is based on strings.

`Get` is retrieving the value for the given key. If there is no value for the given key it will return NotFound as the
error, and an empty string value. Otherwise, the error will contain any other usual io error that can be expected.

`Put` adds the given value for the given key. If this key already exists, it will overwrite the already existing value
with the given one (basically an upsert).

`Delete` will remove the value for the given key. It will ignore when a key does not exist in the database. Underneath
it will be tombstoned, which still store it and make it not retrievable through this interface.

As with any embedded database, they are based on an empty directory you supply. All the state is contained in that
database and opening an existing database in a folder will allow you to continue where you've left off.

Thus, there are two more methods in the interface: `Open` and `Close`. `Open` will set up some state, kick off the
compaction and memstore flushing goroutines and make sure to recover anything that wasn't closed properly beforehand -
more is described in the recovery section below. `Close` will make sure everything is flushed, cleaned and all started
goroutines are stopped.

## Examples

### Opening and closing

```go
db, err := simpledb.NewSimpleDB("path")
if err != nil { log.Fatalf("error: %v", err) }

err = db.Open()
if err != nil { log.Fatalf("error: %v", err) }

err = db.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

### Put data

```go
err = db.Put("hello", "world")
if err != nil { log.Fatalf("error: %v", err) }
```

### Get data

```go
value, err = db.Get("hello")
if err != simpledb.NotFound {
log.Printf("value %s", value)
}
```

### Delete data

```go
err = db.Delete("hello")
if err != nil { log.Fatalf("error: %v", err) }
```

The full end to end example can be found in [examples/simpledb.go](/_examples/simpledb.go).

## Configuration

The database can be configured using options, here are a few that can be used to tune the database:

```go
db, err := NewSimpleDB(
        "some_path", // the non-empty and existing base path of the database - only mandatory argument 
        MemstoreSizeBytes(1024*1024*1024), // the maximum size a memstore should have in bytes
        CompactionRunInterval(30*time.Second), // how often the compaction process should run  
        CompactionMaxSizeBytes(1024 * 1024 * 1024 * 5) // up to which size in bytes to continue to compact sstables
        CompactionFileThreshold(20), // how many files must be at least compacted together
        DisableCompactions() // turn off the compaction completely 
)
```

## Concurrency

The database itself is thread-safe and can be used from multiple goroutines. It's not advised to open or close it from
multiple threads however - which is mostly a matter of idempotency than concurrency.

## How does it work?

Effectively SimpleDB implements the given diagram:

![rocksdb architecture overview](https://user-images.githubusercontent.com/62277872/119747261-310fb300-be47-11eb-92c3-c11719fa8a0c.png)

There is always a writable-memstore that stores in the writes in memory. When it's becoming too big (default of 1gb) it
will rotate to a new memstore and flush the filled one to a SSTable and rotate the WAL. The WAL will be deleted once the
SSTable is written fully to disk.

In SimpleDB only L0 is implemented, meaning there are multiple SSTables at the same time that have overlapping key
ranges. There is a maximum limit of ten simultaneous SSTables, after that limit an asynchronous compaction and merge
process will kick off and merge into a single SSTable again.

The only difference is that there is no manifest log, the purpose of this tracking is delegated to the filesystem mainly
using path patterns and conventions.

## Recovery Modes

The recovery in SimpleDB is quite simple, the supplied base path when the database is opened will be scanned for
existing sstable paths first. They are read in sequence of their file name, and the internal state is reset to be
starting from the latest number.

Then the remainder of the WAL will be read into the existing memstore, but the memstore will be immediately flushed
synchronously into a new SSTable. That has a very simple reason: the WAL folder should be fully empty to not create edge
cases when WALs are being overwritten in multi-crash scenarios. The trade-off here is that most likely the created
SSTable is too small, but it will be compacted with others later on.

In case the crash happened during a compaction, there are two places where we can attempt to recover. The first one is
while the compaction is ongoing (there is no compaction_successful flag file in the folder yet), in this case we can
discard the compaction result. If there is a compaction that has finished successfully, we can try to recover that by
completing the steps in the sstable_manager. The flag file contains several meta information for that case and can be
used to trigger the remainder of the logic again. 
