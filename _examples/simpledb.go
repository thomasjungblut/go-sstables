package main

import (
	"log"
	"os"

	"github.com/thomasjungblut/go-sstables/simpledb"
)

func main() {
	path := "/tmp/simpledb_example/"
	os.MkdirAll(path, 0777)
	defer os.RemoveAll(path)

	db, err := simpledb.NewSimpleDB(path)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = db.Open()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = db.Put("hello", "world")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	get, err := db.Get("hello")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	log.Printf("get 'hello' = %s", get)

	_, err = db.Get("not found")
	if err == simpledb.ErrNotFound {
		log.Printf("not found!")
	}

	err = db.Delete("hello")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = db.Close()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}
