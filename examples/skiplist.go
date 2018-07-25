package main

import (
	"github.com/thomasjungblut/go-sstables/skiplist"
	"log"
)

func mainSkipList() {

	skipListMap := skiplist.NewSkipListMap(skiplist.IntComparator)
	skipListMap.Insert(13, 91)
	skipListMap.Insert(3, 1)
	skipListMap.Insert(5, 2)
	log.Printf("size: %d", skipListMap.Size())

	it := skipListMap.Iterator()
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		log.Printf("key: %d, value: %d", k, v)
	}

	log.Printf("starting at key: %d", 5)
	it = skipListMap.IteratorStartingAt(5)
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		log.Printf("key: %d, value: %d", k, v)
	}

}
