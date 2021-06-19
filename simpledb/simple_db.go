package simpledb

type SimpleDB interface {
	Get() error

	Put() error

	Delete() error
}
