package main

import (
	"errors"
	"github.com/thomasjungblut/go-sstables/_examples/proto"
	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"io"
	"log"
	"os"
)

func main() {
	path := "/tmp/some_file.snap"
	defer os.Remove(path)

	simpleWrite(path)
	simpleRead(path)

	simpleReadAtOffset(path)
}

func simpleRead(path string) {
	reader, err := rProto.NewProtoReaderWithPath(path)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = reader.Open()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	for {
		record := &proto.HelloWorld{}
		_, err := reader.ReadNext(record)
		// io.EOF signals that no records are left to be read
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Fatalf("error: %v", err)
		}

		log.Printf("%s", record.GetMessage())
	}

	err = reader.Close()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func simpleWrite(path string) {
	writer, err := rProto.NewWriter(rProto.Path(path), rProto.CompressionType(recordio.CompressionTypeSnappy))
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = writer.Open()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	record := &proto.HelloWorld{Message: "Hello World"}
	recordOffset, err := writer.Write(record)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	log.Printf("wrote a record at offset of %d bytes", recordOffset)

	err = writer.Close()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func simpleReadAtOffset(path string) {
	reader, err := rProto.NewMMapProtoReaderWithPath(path)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = reader.Open()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	record := &proto.HelloWorld{}
	_, err = reader.ReadNextAt(record, 8)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	log.Printf("Reading message at offset 8: %s", record.GetMessage())

	err = reader.Close()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}
