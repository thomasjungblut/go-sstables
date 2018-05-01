package main

import (
	"github.com/thomasjungblut/go-sstables/recordio"
	"log"
	"os"
	"io"
	"github.com/thomasjungblut/go-sstables/examples/proto"
)

func main() {
	path := "/tmp/some_file.snap"
	defer os.Remove(path)

	simpleWrite(path)
	simpleRead(path)
}

func simpleRead(path string) {
	reader, err := recordio.NewProtoReaderWithPath(path)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	if reader.Open() != nil {
		log.Fatalf("error: %v", err)
	}

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

	if reader.Close() != nil {
		log.Fatalf("error: %v", err)
	}

}

func simpleWrite(path string) {
	writer, err := recordio.NewCompressedProtoWriterWithPath(path, recordio.CompressionTypeSnappy)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	if writer.Open() != nil {
		log.Fatalf("error: %v", err)
	}
	record := &proto.HelloWorld{Message: "Hello World"}
	recordOffset, err := writer.Write(record)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	log.Printf("wrote a record at offset of %d bytes", recordOffset)

	if writer.Close() != nil {
		log.Fatalf("error: %v", err)
	}
}
