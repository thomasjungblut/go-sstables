package porcupine

import (
	"github.com/anishathalye/porcupine"
	"github.com/thomasjungblut/go-sstables/simpledb"
	"time"
)

type DatabaseRecorder struct {
	clientId int
	db       simpledb.DatabaseI

	operations []porcupine.Operation
}

func NewDatabaseRecorder(db simpledb.DatabaseI) *DatabaseRecorder {
	return &DatabaseRecorder{
		clientId:   0,
		db:         db,
		operations: []porcupine.Operation{},
	}
}

func (d *DatabaseRecorder) Close() error {
	return d.db.Close()
}

func (d *DatabaseRecorder) Open() error {
	return d.db.Open()
}

func (d *DatabaseRecorder) Get(key string) (string, error) {
	start := time.Now()
	val, err := d.db.Get(key)
	end := time.Now()
	d.operations = append(d.operations, porcupine.Operation{
		ClientId: d.clientId,
		Input: Input{
			Operation: GetOp,
			Key:       key,
			Val:       val,
		},
		Call: start.UnixNano(),
		Output: Output{
			Key: key,
			Val: val,
			Err: err,
		},
		Return: end.UnixNano(),
	})

	return val, err
}

func (d *DatabaseRecorder) Put(key, value string) error {
	start := time.Now()
	err := d.db.Put(key, value)
	end := time.Now()
	d.operations = append(d.operations, porcupine.Operation{
		ClientId: d.clientId,
		Input: Input{
			Operation: PutOp,
			Key:       key,
			Val:       value,
		},
		Call: start.UnixNano(),
		Output: Output{
			Key: key,
			Val: value,
			Err: err,
		},
		Return: end.UnixNano(),
	})

	return err
}

func (d *DatabaseRecorder) Delete(key string) error {
	start := time.Now()
	err := d.db.Delete(key)
	end := time.Now()
	d.operations = append(d.operations, porcupine.Operation{
		ClientId: d.clientId,
		Input: Input{
			Operation: DelOp,
			Key:       key,
			Val:       "",
		},
		Call: start.UnixNano(),
		Output: Output{
			Err: err,
		},
		Return: end.UnixNano(),
	})

	return err
}
