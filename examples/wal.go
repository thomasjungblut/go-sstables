package main

import (
	"fmt"

	exProto "github.com/thomasjungblut/go-sstables/examples/proto"
	. "github.com/thomasjungblut/go-sstables/wal"
	wProto "github.com/thomasjungblut/go-sstables/wal/proto"
	pb "google.golang.org/protobuf/proto"
	"log"
	"os"
)

func main() {
	path := "/tmp/wal_example/"
	_ = os.MkdirAll(path, 0777)
	defer os.RemoveAll(path)

	opts, err := NewWriteAheadLogOptions(BasePath(path))
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	wal, err := wProto.NewProtoWriteAheadLog(opts)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	updateMutation := exProto.UpdateMutation{
		ColumnName:  "some_col",
		ColumnValue: "some_val",
	}
	mutation := exProto.Mutation{
		SeqNumber: 1,
		Mutation:  &exProto.Mutation_Update{Update: &updateMutation},
	}

	err = wal.AppendSync(&mutation)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	deleteMutation := exProto.DeleteMutation{
		ColumnName: "some_col",
	}
	mutation = exProto.Mutation{
		SeqNumber: 2,
		Mutation:  &exProto.Mutation_Delete{Delete: &deleteMutation},
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
		return &exProto.Mutation{}
	}, func(record pb.Message) error {
		mutation := record.(*exProto.Mutation)
		fmt.Printf("seq no: %d\n", mutation.SeqNumber)
		switch x := mutation.Mutation.(type) {
		case *exProto.Mutation_Update:
			fmt.Printf("update with colname %s and val %s\n", x.Update.ColumnName, x.Update.ColumnValue)
		case *exProto.Mutation_Delete:
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
