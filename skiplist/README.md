## Using SkipListMap

Whenever you find yourself in need of a sorted list/map for range scans or ordered iteration, you can resort to a `SkipList`. 
The `SkipListMap` in this project is based on [LevelDBs skiplist](https://github.com/google/leveldb/blob/master/db/skiplist.h) and super easy to use:

```go
skipListMap := skiplist.NewSkipListMap(skiplist.IntComparator)
skipListMap.Insert(13, 91)
skipListMap.Insert(3, 1)
skipListMap.Insert(5, 2)
log.Printf("size: %d", skipListMap.Size())

it, _ := skipListMap.Iterator()
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}

log.Printf("starting at key: %d", 5)
it, _ = skipListMap.IteratorStartingAt(5)
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}

log.Printf("between: %d and %d", 8, 50)
it, _ = skipListMap.IteratorBetween(8, 50)
for {
    k, v, err := it.Next()
    if err == skiplist.Done {
        break
    }
    log.Printf("key: %d, value: %d", k, v)
}
```

You can supply any kind of element and comparator to sort arbitrary structs and primitives. 
You can get the full example from [examples/skiplist.go](/examples/skiplist.go).
 
