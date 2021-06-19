# SimpleDB

SimpleDB is a simple embedded database built using the primitives in this repository. It's an example project to show how the different pieces can fit together and is not meant to be a production-ready database like [RocksDB](https://github.com/facebook/rocksdb) or [LevelDB](https://github.com/google/leveldb).


## Interface

There are three methods, namely `Get`, `Put` and `Delete` to query and add data to the database. For simplicity's sake the whole interface is based on strings. 

## Concurrency

The database itself is thread-safe and can be used from multiple goroutines. The writing only allows a single thread for simplicity, could be sharded across several memstores and WALs for more throughput.