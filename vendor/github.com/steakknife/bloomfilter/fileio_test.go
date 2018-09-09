// Package bloomfilter is face-meltingly fast, thread-safe,
// marshalable, unionable, probability- and
// optimal-size-calculating Bloom filter in go
//
// https://github.com/steakknife/bloomfilter
//
// Copyright © 2014, 2015, 2018 Barry Allard
//
// MIT license
//
package bloomfilter

import (
	"bytes"
	"testing"
)

func TestWriteRead(t *testing.T) {
	// minimal filter
	f, _ := New(2, 1)

	v := hashableUint64(0)
	f.Add(v)

	var b bytes.Buffer

	_, err := f.WriteTo(&b)
	if err != nil {
		t.Error(err)
	}

	f2, _, err := ReadFrom(&b)
	if err != nil {
		t.Error(err)
	}

	if !f2.Contains(v) {
		t.Error("Filters not equal")
	}
}
