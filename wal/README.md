## Using WAL (write-ahead-log)

A [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) is a very common abstraction in database systems that allow the DBMS to write a given mutation (INSERT/UPDATE/DELETE) to durable storage before applying to the actual database files.  

This WAL is based on the [recordio package](/recordio) and features a fsync-based append and a replay functionality. It has a maximum size per file and automatically rotates it when reaching the threshold. There is a maximum amount of WAL files that is enforced (1 million files at 128mb by default).

The current implementation is still a bit naive for these reasons:
1. it does not have a notion of sequence numbers as a first class citizen, which means that you can't selectively commit and replay from a given sequence number. When you want to implement it, keep the sequence number in the schema you're writing into the log.
2. the underlying filesystem of recordio (most likely) doesn't replicate internally, so the log is lost in case of a machine/disk failure
3. no log compaction is currently implemented

### Creating a WAL

Creating a WAL is pretty easy, you effectively just need to supply a directory for it to store the individual WALs as an option:

```go
opts, err := NewWriteAheadLogOptions(BasePath("some_directory"))
if err != nil { log.Fatalf("error: %v", err) }
wal, err := NewWriteAheadLog(opts)
```

There are several options that you can make use of:
```go
opts, err := NewWriteAheadLogOptions(
    // mandatory folder path of the WAL
    BasePath("some_directory"), 
    // maximum size of each WAL file in bytes
    MaximumWalFileSizeBytes(1024 * 1024 * 10), 
    // customization to the recordio writer, for example compression for the records:
    WriterFactory(func(path string) (recordio.WriterI, error) {
        return recordio.NewCompressedFileWriterWithPath(path, recordio.CompressionTypeSnappy)
    })),
    // readers can be customized in similar fashion (if necessary)
    ReaderFactory(func(path string) (recordio.ReaderI, error) {
        return recordio.NewFileReaderWithPath(path)
    }),
)
```

### Appending to the WAL

Appending works similar to recordio:

```go
record := make([]byte, 8)
binary.BigEndian.PutUint64(record, 42)
err := wal.AppendSync(record)

... append more

// this closes the WAL
err := wal.Close()
```

The "AppendSync" operation is always followed by a fsync syscall, so the throughput is quite bad as a trade-off with durability. 

### Replaying from the WAL

Replaying can be done by supplying a function that processes one record at a time:

```go
err := wal.Replay(func(record []byte) error {
    n := binary.BigEndian.Uint64(record)						
    return nil
})
```

Errors encountered during the process will always bubble up to the return of the replay immediately.

### Deleting it

That will delete the whole folder containing all WALs:

```go
err := wal.Clean()
```

### Using Protobuf

As with the other packages, there are also some proto bindings around the raw byte slices APIs. Let's say you have a mutation defined as such:

```protobuf
message Mutation {
    uint64 seqNumber  = 1;
    // imagine a oneof CREATE/UPDATE/INSERT/DELETE mutation types below  
}
```

The only semantic difference to the API with byte slices is that `Replay` takes a factory to generate new protobuf types, this can't be guessed entirely from the context and avoids costly reflection calls. 

A full WAL example becomes:

```go
opts, err := NewWriteAheadLogOptions(BasePath("some_directory"))
if err != nil { log.Fatalf("error: %v", err) }
wal, err := NewProtoWriteAheadLog(opts)
if err != nil { log.Fatalf("error: %v", err) }

updateMutation := proto.UpdateMutation{
    ColumnName:  "some_col",
    ColumnValue: "some_val",
}
mutation := proto.Mutation{
    SeqNumber: 1,
    Mutation:  &proto.Mutation_Update{Update: &updateMutation},
}

err = wal.AppendSync(&mutation)
if err != nil {
    log.Fatalf("error: %v", err)
}

deleteMutation := proto.DeleteMutation{
    ColumnName: "some_col",
}
mutation = proto.Mutation{
    SeqNumber: 2,
    Mutation:  &proto.Mutation_Delete{Delete: &deleteMutation},
}

err = wal.AppendSync(&mutation)
if err != nil {
    log.Fatalf("error: %v", err)
}

err = wal.Close()
if err != nil {
    log.Fatalf("error: %v", err)
}

err = wal.Replay(func() pb.Message {
    return &proto.Mutation{}
}, func(record pb.Message) error {
    mutation := record.(*proto.Mutation)
    fmt.Printf("seq no: %d\n", mutation.SeqNumber)
    switch x := mutation.Mutation.(type) {
    case *proto.Mutation_Update:
        fmt.Printf("update with colname %s and val %s\n", x.Update.ColumnName, x.Update.ColumnValue)
    case *proto.Mutation_Delete:
        fmt.Printf("delete with colname %s\n", x.Delete.ColumnName)
    default:
        return fmt.Errorf("proto.Mutation has unexpected oneof type %T", x)
    }
    return nil
})

if err != nil {
    log.Fatalf("error: %v", err)
}

```

which prints:

```
seq no: 1
update with colname some_col and val some_val
seq no: 2
delete with colname some_col
```

You can get the full example from [examples/wal.go](/examples/wal.go).