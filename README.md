[![Build Status](https://travis-ci.org/thomasjungblut/go-sstables.svg?branch=master)](https://travis-ci.org/thomasjungblut/go-sstables)

## go-sstables

`go-sstables` is a Go library that contains NoSQL database building blocks like a sequential record format (recordio),
a sorted string table (sstable), a write-ahead-log (WAL), and a memory store (memstore) that stores key/value pairs in memory using a skip list.

While plain `[]byte` are at the core of this library, there are wrappers and bindings for protobuf to enable more convenient serialization. 

## Installation

This is a library as it does not contain any installable binary, which means you can just directly add it to your dependency via `go get`:

> go get -d github.com/thomasjungblut/go-sstables

## Documentation

This README became quite large, thus the documentation is now separated by package. There you'll find more information on how to use each individual package.

* [RecordIO](recordio/README.md)
  * [Benchmark](benchmark/README.md)
* [Memstore](memstore/README.md)
* [sstables](sstables/README.md)
* [SkipList](skiplist/README.md)
* [WriteAheadLog](wal/README.md)

## Kaitai Support

As you might want to read the data and files in other languages, I've added support for [Kaitai](https://kaitai.io/).  
Kaitai is a declarative schema file to define a binary format. From that `ksy` file you can generate code for a lot of other languages and read the data.

Currently, there is support for:
* [RecordIO (v2)](kaitai/recordio_v2.ksy)

You can find more information on how to generate Kaitai readers in [kaitai/README.md](kaitai/README.md).

## Development on go-sstables

### Updating dependencies through Go Modules

[General overview of modules](https://github.com/golang/go/wiki/Modules)

One can update dependencies via:

```
go get -u <repo url>
```

### Releasing the Go Module

[General Guidance](https://github.com/golang/go/wiki/Modules#releasing-modules-all-versions)

In short, run these commands:

```
make unit-test
make release
git push --tags 
```
