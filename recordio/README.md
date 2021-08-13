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

Reading follows the general lifecycle as well. The reading works by reading the next byte slices until `io.EOF` is returned - which is a familiar pattern from other "iterables".

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
