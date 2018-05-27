//
// Face-meltingly fast, thread-safe, marshalable, unionable, probability- and optimal-size-calculating Bloom filter in go
//
// https://github.com/steakknife/bloomfilter
//
// Copyright © 2014, 2015 Barry Allard
//
// MIT license
//
package bloomfilter

// a read-only type that conforms to hash.Hash64, but only Sum64() works.
// It is set by writing the underlying value.
type hashableUint64 uint64

func (h hashableUint64) Write([]byte) (int, error) {
	panic("Unimplemented")
	return 0, nil
}

func (h hashableUint64) Sum([]byte) []byte {
	panic("Unimplemented")
	return nil
}

func (h hashableUint64) Reset() {
	panic("Unimplemented")
}

func (h hashableUint64) BlockSize() int {
	panic("Unimplemented")
	return 0
}

func (h hashableUint64) Size() int {
	panic("Unimplemented")
	return 0
}

func (h hashableUint64) Sum64() uint64 {
	return uint64(h)
}
