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
	"errors"
	"hash"
	"sync"
)

// opaque Bloom filter
type Filter struct {
	lock sync.RWMutex
	bits []uint64
	keys []uint64
	m    uint64 // number of bits the "bits" field should recognize
	n    uint64 // number of inserted elements
}

var incompatibleBloomFilters = errors.New("Cannot perform union on two incompatible Bloom filters")

// Hashable -> hashes
func (f Filter) hash(v hash.Hash64) []uint64 {
	rawHash := v.Sum64()
	n := len(f.keys)
	hashes := make([]uint64, n)
	for i := 0; i < n; i++ {
		hashes[i] = rawHash ^ f.keys[i]
	}
	return hashes
}

// return size of Bloom filter, in bits
func (f Filter) M() uint64 {
	return f.m
}

// return count of keys
func (f Filter) K() uint64 {
	return uint64(len(f.keys))
}

// add a hashable item, v, to the filter
func (f *Filter) Add(v hash.Hash64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	for _, i := range f.hash(v) {
		// f.setBit(i)
		i %= f.m
		f.bits[i>>6] |= 1 << uint(i&0x3f)
	}
	f.n++
}

// false: f definitely does not contain value v
// true:  f maybe contains value v
func (f *Filter) Contains(v hash.Hash64) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()

	r := uint64(0)
	for _, i := range f.hash(v) {
		// r |= f.getBit(k)
		i %= f.m
		r |= (f.bits[i>>6] >> uint(i&0x3f)) & 1
	}
	return uint64ToBool(r)
}

// create a copy of Bloom filter f
func (f *Filter) Copy() *Filter {
	f.lock.RLock()
	defer f.lock.RUnlock()

	out := f.NewCompatible()
	copy(out.bits, f.bits)
	out.n = f.n
	return out
}

// merge Bloom filter f2 into f
func (f *Filter) UnionInPlace(f2 *Filter) error {
	if !f.IsCompatible(f2) {
		return incompatibleBloomFilters
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f2.lock.RLock()
	defer f2.lock.RUnlock()

	for i, bitword := range f2.bits {
		f.bits[i] |= bitword
	}
	return nil
}

// out is a copy of f unioned with f2
func (f *Filter) Union(f2 *Filter) (*Filter, error) {
	out := f.Copy()
	if err := out.UnionInPlace(f2); err != nil {
		return nil, err
	}
	return out, nil
}
