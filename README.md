[![unit-test](https://github.com/thomasjungblut/go-sstables/actions/workflows/unit-test.yml/badge.svg)](https://github.com/thomasjungblut/go-sstables/actions/workflows/unit-test.yml)
[![CodeQL](https://github.com/thomasjungblut/go-sstables/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/thomasjungblut/go-sstables/actions/workflows/codeql-analysis.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thomasjungblut/go-sstables.svg)](https://pkg.go.dev/github.com/thomasjungblut/go-sstables)

## go-sstables

`go-sstables` is a Go library that contains NoSQL database building blocks like a sequential record format (recordio), a
sorted string table (sstable), a write-ahead-log (WAL), and a memory store (memstore) that stores key/value pairs in
memory using a skip list.

You can frequently find those in embedded key-value stores or databases as well, notable examples
are [RocksDB](https://github.com/facebook/rocksdb) or [LevelDB](https://github.com/google/leveldb).

There is an example embedded key-value database in this library, you can find it in the simpledb folder - please don't
use it for any production workload.

While plain `[]byte` are at the core of this library, there are wrappers and bindings for protobuf to enable more
convenient serialization.

## Installation

This is a library as it does not contain any installable binary, which means you can just directly add it to your
dependency via `go get`:

> go get -d github.com/thomasjungblut/go-sstables

## Documentation

[RocksDB has a great overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview#3-high-level-architecture) of
how the components usually play together to get an idea:

![rocksdb architecture overview](https://user-images.githubusercontent.com/62277872/119747261-310fb300-be47-11eb-92c3-c11719fa8a0c.png)

You will find basically all of those mentioned pieces above and all of them stitched together as SimpleDB. The
documentation is now separated by package for easier browsing, each of those READMEs contain examples - there is
also [/examples](_examples) when you prefer browsing the code directly.

* [RecordIO](recordio/README.md)
    * [Benchmark](benchmark/README.md#recordio)
* [sstables](sstables/README.md)
    * [Benchmark](benchmark/README.md#sstable)
* [Memstore](memstore/README.md)
* [SkipList](skiplist/README.md)
* [WriteAheadLog](wal/README.md)
* [SimpleDB](simpledb/README.md)
    * [Benchmark](benchmark/README.md#simpledb)

You can also find all interface and
method [documentation on `pkg.go.dev`](https://pkg.go.dev/github.com/thomasjungblut/go-sstables/sstables#section-documentation)
.

## Kaitai Support

As you might want to read the data and files in other languages, I've added support for [Kaitai](https://kaitai.io/).  
Kaitai is a declarative schema file to define a binary format. From that `ksy` file you can generate code for a lot of
other languages and read the data.

Currently, there is support for:

* [RecordIO (v2)](kaitai/recordio_v2.ksy)
* [RecordIO (v3)](kaitai/recordio_v3.ksy)
* [RecordIO (v4)](kaitai/recordio_v4.ksy)

You can find more information on how to generate Kaitai readers in [kaitai/README.md](kaitai/README.md).


## Development on go-sstables

### Thank you

I want to deeply thank Sébastien Heitzmann (@sebheitzmann) for his continued support, reviews and usage of this library.


### Generating protobufs

This needs some pre-requisites installed, namely
the [protobuf compiler](https://github.com/protocolbuffers/protobuf/releases) and the go generator plugin. The latter
can be installed as a go package:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go
```

Full installation details can be found in
the [protobuf dev documentation](https://developers.google.com/protocol-buffers/docs/gotutorial#compiling-your-protocol-buffers)
.

Once installed, one can generate the protobuf structs using:

```
make compile-proto
```

### Releasing the Go Module

[General Guidance](https://github.com/golang/go/wiki/Modules#releasing-modules-all-versions)

In short, run these commands:

```
make unit-test
make release
git push --tags 
```
