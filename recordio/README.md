## Using RecordIO

RecordIO allows you to write sequential key/value entities into a flat file and is heavily inspired by [Hadoop's SequenceFile](https://cwiki.apache.org/confluence/display/HADOOP2/SequenceFile). 

Below sections focus on reading and writing using plain byte slices. If you want to read more about the Protobuf bindings go to the section [Using Proto RecordIO](#using-proto-recordio).

### Writing

The general flow is:
* create a new recordio writer (with a string path or file pointer, optionally with compression)
* open the file
* `write` a "record" as often and as much as you want
* close the file

In go that looks like this:

```go
import "github.com/thomasjungblut/go-sstables/recordio"

writer, err := recordio.NewFileWriter(
                     recordio.Path("some/path/records.rio"), 
                     recordio.CompressionType(recordio.CompressionTypeSnappy))
if err != nil { log.Fatalf("error: %v", err) }

err = writer.Open()
if err != nil { log.Fatalf("error: %v", err) }

offset, err := writer.Write([]byte{1,3,3,7})
if err != nil { log.Fatalf("error: %v", err) }

err = writer.Close()
if err != nil { log.Fatalf("error: %v", err) }
``` 

After `Write`, you get the offset in the file returned at which the record was written. This is quite useful for indexing and is used heavily in the `sstables` package.

There is another alternative method called `WriteSync`, which can be used to flush the disk write cache ["fsync"](https://man7.org/linux/man-pages/man2/fdatasync.2.html) to actually persist the data. That's a must-have in a write-ahead-log to guarantee the persistence on the disk. Keep in mind that this is drastically slower, consult the benchmark section for more information.

By default, the `recordio.NewFileWriter` will not use any compression, but if configured there are two compression libs available: Snappy and GZIP. The compression is per record and not for the whole file - so it might not be as efficient as compressing the whole content at once after closing.

### Reading

Reading follows the general lifecycle as well. The reading works by reading the next byte slices until `io.EOF` (or a wrapped alternative) is returned - which is a familiar pattern from other "iterables".

```go
import (
   "github.com/thomasjungblut/go-sstables/recordio"
)

reader, err := recordio.NewFileReaderWithFile(path)
if err != nil { log.Fatalf("error: %v", err) }

err = reader.Open()
if err != nil { log.Fatalf("error: %v", err) }

for {
    _, err := reader.ReadNext()
    // io.EOF signals that no records are left to be read, could be wrapped - so always check using errors.Is()
    if errors.Is(err, io.EOF) {
        break
    }

    if err != nil {
        log.Fatalf("error: %v", err)
    }

    log.Printf("%s", record.GetMessage())
}

err = reader.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

## Using Proto RecordIO

Reading and writing a `recordio` file using Protobuf and snappy compression can be done quite easily with the below sections. Here's the simple proto file we use:

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/thomasjungblut/go-sstables/examples/proto";

message HelloWorld {
    string message = 1;
}

```

You can compile using protoc, here we are saving the message as part of our examples:

```
protoc --go_out=. --go_opt=paths=source_relative examples/proto/hello_world.proto
```

### Writing 

Writing a recordio file in Go then becomes:

```go
import (
   "github.com/thomasjungblut/go-sstables/examples/proto" // generated proto
   rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
)

writer, err := rProto.NewWriter(
                     rProto.Path(path), 
                     rProto.CompressionType(recordio.CompressionTypeSnappy))
if err != nil { log.Fatalf("error: %v", err) }

err = writer.Open()
if err != nil { log.Fatalf("error: %v", err) }

record := &proto.HelloWorld{Message: "Hello World"}
recordOffset, err := writer.Write(record)
if err != nil { log.Fatalf("error: %v", err) }

log.Printf("wrote a record at offset of %d bytes", recordOffset)

err = writer.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

### Reading

Reading the same file we just wrote can be done like this:

```go
import (
   "github.com/thomasjungblut/go-sstables/examples/proto" // generated proto
   rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
)

reader, err := rProto.NewProtoReaderWithPath(path)
if err != nil { log.Fatalf("error: %v", err) }

err = reader.Open()
if err != nil { log.Fatalf("error: %v", err) }

for {
    record := &proto.HelloWorld{}
    _, err := reader.ReadNext(record)
    // io.EOF signals that no records are left to be read
    if err == io.EOF {
        break
    }

    if err != nil {
        log.Fatalf("error: %v", err)
    }

    log.Printf("%s", record.GetMessage())
}

err = reader.Close()
if err != nil { log.Fatalf("error: %v", err) }
```

SSTables support random reads of backing values, thus recordio also supports it using its `mmap` implementation:

```go
import (
   "github.com/thomasjungblut/go-sstables/examples/proto" // generated proto
   rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
)

reader, err := rProto.NewMMapProtoReaderWithPath(path)
if err != nil { log.Fatalf("error: %v", err) }

err = reader.Open()
if err != nil { log.Fatalf("error: %v", err) }

record := &proto.HelloWorld{}
_, err = reader.ReadNextAt(record, 8)
if err != nil { log.Fatalf("error: %v", err) }

log.Printf("Reading message at offset 8: %s", record.GetMessage())

err = reader.Close()
if err != nil { log.Fatalf("error: %v", err) }
``` 

You can get the full example from [examples/recordio.go](/_examples/recordio.go).

## DirectIO (experimental)

DirectIO is useful when you want to bypass the operating system memory caches when writing something to disk directly. This can be useful in database applications like bulk-imports, where you don't want to pollute/churn existing memory for pages that were recently written and won't be read anytime soon.

DirectIO can be enabled when creating a new writer by setting:

````go
import "github.com/thomasjungblut/go-sstables/recordio"

writer, err := recordio.NewFileWriter(
	recordio.Path("some/path/records.rio"), 
	recordio.DirectIO(), 
	recordio.BufferSizeBytes(4096))
if err != nil { log.Fatalf("error: %v", err) }
````

It's highly recommended to check what buffer/block sizes are available on the target system, usually those need to be a power of two. It's very important to test a full open/write/close cycle as well, otherwise you might encounter rather strange error messages like `The parameter is incorrect.`, which sadly isn't very meaningful and difficult to debug. Usually this either means that DirectIO wasn't available to begin with, or the block sizes are not aligned with what the operating system expects to be written.

You can check whether your OS is theoretically capable to enable DirectIO using:

````go
import "github.com/thomasjungblut/go-sstables/recordio"

// true if yes, otherwise not
available, err := recordio.IsDirectIOAvailable()
````

In this package the DirectIO support comes through a library called [ncw/directio](https://github.com/ncw/directio), which has good support across Linux, macOS and Windows under a single interface. The caveats of each platform, for example the buffer/block sizes, need to still be taken into account.  
Another caveat is that the block alignment causes to write a certain amount of waste. Let's imagine you have blocks of 1024 bytes and only want to write 1025 bytes, with DirectIO enabled you will end up with a file of size 2048 (2 blocks) instead of a file with only 1025 bytes with DirectIO disabled. The DirectIO file will be padded with zeroes towards the end and the in-library readers honor this format and not assume a corrupted file format. 


## io_uring (experimental)

Since version 5.x the linux kernel supports a new asynchronous approach to execute syscalls. In a few words, io_uring is a shared ring buffer between the kernel and user space which allows queueing syscalls and later retrieve their results.

You can read more about io_uring at [https://kernel.dk/io_uring.pdf](https://kernel.dk/io_uring.pdf).

In case you have SELinux enabled, you might hit "permission denied" errors when initializing the uring try to test with permissive mode enabled temporarily. 
