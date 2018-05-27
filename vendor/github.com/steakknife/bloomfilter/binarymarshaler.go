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

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
)

// conforms to encoding.BinaryMarshaler

// marshalled binary layout (Little Endian):
//
//   k      1 uint64
//   n      1 uint64
//   m      1 uint64
//   keys   [k]uint64
//   bits   [(m+63)/64]uint64
//   hash   sha384 (384 bits == 48 bytes)
//
//   size = (3 + k + (m+63)/64) * 8 bytes
//
func (f *Filter) MarshalBinary() (data []byte, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	buf := new(bytes.Buffer)

	if err = binary.Write(buf, binary.LittleEndian, f.K()); err != nil {
		return nil, err
	}

	if err = binary.Write(buf, binary.LittleEndian, f.n); err != nil {
		return nil, err
	}

	if err = binary.Write(buf, binary.LittleEndian, f.m); err != nil {
		return nil, err
	}

	if err = binary.Write(buf, binary.LittleEndian, f.keys); err != nil {
		return nil, err
	}

	if err = binary.Write(buf, binary.LittleEndian, f.bits); err != nil {
		return nil, err
	}

	hash := sha512.Sum384(buf.Bytes())
	if err = binary.Write(buf, binary.LittleEndian, hash); err != nil {
		return nil, err
	}

	bytes := buf.Bytes()
	debug("bloomfilter.MarshalBinary: Successfully wrote %d byte(s), sha384 %v", bytes, hash)

	return bytes, nil
}
