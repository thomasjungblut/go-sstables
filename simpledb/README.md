# SimpleDB

SimpleDB is a simple embedded database built using the primitives in this repository. It's an example project to show how the different pieces can fit together and is not meant to be a production-ready database like [RocksDB](https://github.com/facebook/rocksdb) or [LevelDB](https://github.com/google/leveldb).


## Interface

There are three methods, namely `Get`, `Put` and `Delete` to query and add data to the database. For simplicity's sake the whole interface is based on strings.

`Get` is retrieving the value for the given key. If there is no value for the given key it will return NotFound as the error, and an empty string value. Otherwise, the error will contain any other usual io error that can be expected.

`Put` adds the given value for the given key. If this key already exists, it will overwrite the already existing value with the given one (basically an upsert).

`Delete` will remove the value for the given key. It will ignore when a key does not exist in the database. Underneath it will be tombstoned, which still store it and make it not retrievable through this interface.

## Concurrency

The database itself is thread-safe and can be used from multiple goroutines.

## How does it work?

Effectively SimpleDB implements the given diagram:

![rocksdb architecture overview](https://user-images.githubusercontent.com/62277872/119747261-310fb300-be47-11eb-92c3-c11719fa8a0c.png)

There is always a writable-memstore that stores in the writes in memory. When it's becoming too big (default of 1gb) it will rotate to a new memstore and flush the filled one as a SSTable and rotate the WAL. The WAL will be deleted once the sstable is written fully to disk.

In SimpleDB only L0 is implemented, meaning there are multiple SSTables at the same time that have overlapping key ranges. There is a maximum limit of five simultaneous SSTables, after that limit an asynchronous compaction and merge process will kick off and merge into a single SSTable again.
