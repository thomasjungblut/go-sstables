## Using RecordIO

RecordIO allows you to write sequential key/value entities into a flat file and is heavily inspired by [Hadoop's SequenceFile](https://wiki.apache.org/hadoop/SequenceFile). 
Writing a `recordio` file using Protobuf and snappy compression can be done as follows. Here's the simple proto file we use:

```protobuf
message HelloWorld {
    string message = 1;
}
```

Writing in Go then becomes this:

```go
writer, err := recordio.NewCompressedProtoWriterWithPath(path, recordio.CompressionTypeSnappy)
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

Reading the same file can be done like this:

```go
reader, err := recordio.NewProtoReaderWithPath(path)
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
reader, err := recordio.NewMMapProtoReaderWithPath(path)
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

You can get the full example from [examples/recordio.go](/examples/recordio.go).
