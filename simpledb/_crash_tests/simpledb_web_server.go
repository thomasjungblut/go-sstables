package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/thomasjungblut/go-sstables/simpledb"
)

const address = "0.0.0.0:29071"
const dataEndpoint = "/data"
const fullDataEndpoint = address + dataEndpoint

type Server struct {
	db simpledb.DatabaseI
}

func (s *Server) handleGet(w http.ResponseWriter, key string) {
	get, err := s.db.Get(key)
	if err != nil {
		if err == simpledb.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		} else {
			log.Printf("error while getting key '%s': %v\n", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(get))
	if err != nil {
		log.Printf("error while writing value for key '%s': %v\n", key, err)
		return
	}
}

func (s *Server) Data(w http.ResponseWriter, r *http.Request) {

	key := r.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodDelete:
		err := s.db.Delete(key)
		if err != nil {
			log.Printf("error while deleting key '%s': %v\n", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		allBytes, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("error while getting body for key '%s': %v\n", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = s.db.Put(key, string(allBytes))
		if err != nil {
			log.Printf("error while getting putting value for key '%s': %v\n", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		log.Printf("not supported method '%s'\n", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	}

}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("missing db folder argument\n")
	}
	baseDir := os.Args[1]

	_, err := os.Stat(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("db folder does not exist\n")
		} else {
			log.Fatalf("stat: %v\n", err)
		}
	}

	// make sure we sync to WAL all the time and have a small memstore size for flushing often
	db, err := simpledb.NewSimpleDB(baseDir,
		simpledb.CompactionFileThreshold(5),
		simpledb.CompactionRunInterval(1*time.Second),
		simpledb.MemstoreSizeBytes(1024*1024*4))
	if err != nil {
		log.Fatalf("newDB: %v\n", err)
	}
	defer func() {
		err := db.Close()
		log.Fatalf("error while closing db: %v\n", err)
	}()

	err = db.Open()
	if err != nil {
		log.Fatalf("openDB: %v\n", err)
	}

	serv := &Server{db: db}
	http.HandleFunc(dataEndpoint, serv.Data)
	log.Printf("running on %s\n", address)
	log.Fatal(http.ListenAndServe(address, nil))
}
