# SkipLists

Whenever you find yourself in need of a sorted list/map for range scans or ordered iteration, you can resort to a `SkipList`. The `SkipList` in this project is based on [LevelDBs skiplist](https://github.com/google/leveldb/blob/master/db/skiplist.h)

This package is the only one compatible with versions pre-generics.

## Using skiplist.Map (generics Go >=1.18)

You can get the full example from [examples/skiplist.go](/_examples/skiplist.go).

```go
import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"log"
)

func main() {

	skipListMap := skiplist.NewSkipListMap[int, int](skiplist.OrderedComparator[int]{})
	skipListMap.Insert(13, 91)
	skipListMap.Insert(3, 1)
	skipListMap.Insert(5, 2)
	log.Printf("size: %d", skipListMap.Size())

	it, _ := skipListMap.Iterator()
	for {
		k, v, err := it.Next()
		if errors.is(err, skiplist.Done) {
			break
		}
		log.Printf("key: %d, value: %d", k, v)
	}

	log.Printf("starting at key: %d", 5)
	it, _ = skipListMap.IteratorStartingAt(5)
	for {
		k, v, err := it.Next()
		if errors.is(err, skiplist.Done) {
			break
		}
		log.Printf("key: %d, value: %d", k, v)
	}

	log.Printf("between: %d and %d", 8, 50)
	it, _ = skipListMap.IteratorBetween(8, 50)
	for {
		k, v, err := it.Next()
		if errors.is(err, skiplist.Done) {
			break
		}
		log.Printf("key: %d, value: %d", k, v)
	}
}
```
