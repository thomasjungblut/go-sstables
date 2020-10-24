package main

import (
	"fmt"
	pb "github.com/gogo/protobuf/proto"
	"github.com/thomasjungblut/go-sstables/examples/proto"
	. "github.com/thomasjungblut/go-sstables/wal"
	"log"
	"os"
)

func main() {
	path := "/tmp/wal_example/"
	os.MkdirAll(path, 0777)
	defer os.RemoveAll(path)

	opts, err := NewWriteAheadLogOptions(BasePath(path))
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	wal, err := NewProtoWriteAheadLog(opts)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

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
}
