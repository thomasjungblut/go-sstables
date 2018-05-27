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
	"compress/gzip"
	"io/ioutil"
	"os"
)

// Read a gzip'ed, marshaled Bloom filter
// Suggested file extension: .bf.gz
func ReadFile(filename string) (*Filter, error) {
	fr, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	r, err := gzip.NewReader(fr)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	content, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	bf := new(Filter)
	if err = bf.UnmarshalBinary(content); err != nil {
		return nil, err
	}
	return bf, nil
}

// Write a gzip'ed, marshaled Bloom filter
// Suggested file extension: .bf.gz
func (f *Filter) WriteFile(filename string) error {
	f.lock.RLock()
	defer f.lock.RUnlock()

	fw, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fw.Close()

	w := gzip.NewWriter(fw)
	defer w.Close()

	content, err := f.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = w.Write(content)
	if err != nil {
		return err
	}
	return nil
}
