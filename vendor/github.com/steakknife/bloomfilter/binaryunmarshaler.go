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
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"errors"
)

var hashError = errors.New("Hash mismatch, the bloomfilter is probably corrupt.")

// conforms to encoding.BinaryUnmarshaler

func (f *Filter) UnmarshalBinary(data []byte) (err error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	var k uint64

	buf := bytes.NewBuffer(data)
	if err = binary.Read(buf, binary.LittleEndian, &k); err != nil {
		return err
	}

	if err = binary.Read(buf, binary.LittleEndian, &(f.n)); err != nil {
		return err
	}

	if err = binary.Read(buf, binary.LittleEndian, &(f.m)); err != nil {
		return err
	}

	debug("read bf k=%d n=%d m=%d\n", k, f.n, f.m)

	f.keys = make([]uint64, k)
	if err = binary.Read(buf, binary.LittleEndian, f.keys); err != nil {
		return err
	}

	f.bits = make([]uint64, f.n)
	if err = binary.Read(buf, binary.LittleEndian, f.bits); err != nil {
		return err
	}

	expectedHash := make([]byte, sha512.Size384)
	if err = binary.Read(buf, binary.LittleEndian, expectedHash); err != nil {
		return err
	}

	actualHash := sha512.Sum384(data[:len(data)-sha512.Size384])

	if !hmac.Equal(expectedHash, actualHash[:]) {
		debug("bloomfilter.UnmarshalBinary() sha384 hash failed: actual %v  expected %v", actualHash, expectedHash)
		return hashError
	}

	debug("bloomfilter.UnmarshalBinary() successfully read %d byte(s), sha384 %v", len(data), actualHash)

	return nil
}
