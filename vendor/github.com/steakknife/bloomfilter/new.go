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

import "crypto/rand"

const (
	M_MIN        = 2 // minimum Bloom filter bits count
	K_MIN        = 1 // minimum number of keys
	UINT64_BYTES = 8
)

// new with CSPRNG keys
//
// m is the size of the Bloom filter, in bits, >= 2
//
// k is the number of keys, >= 1
func New(m, k uint64) *Filter {
	return newWithKeys(m, newRandKeys(k))
}

func newRandKeys(k uint64) []uint64 {
	keys := make([]uint64, k)
	buf := make([]byte, UINT64_BYTES)
	for i := uint64(0); i < k; i++ {
		if read, err := rand.Read(buf); err != nil || read != UINT64_BYTES {
			panicf("Cannot read %d bytes from CSRPNG crypto/rand.Read (err=%v)", UINT64_BYTES, err)
		}
		keys[i] = uint64(buf[0]) |
			uint64(buf[1])<<8 |
			uint64(buf[2])<<16 |
			uint64(buf[3])<<24 |
			uint64(buf[4])<<32 |
			uint64(buf[5])<<40 |
			uint64(buf[6])<<48 |
			uint64(buf[7])<<56
	}
	return keys
}

// Create a new Filter compatible with f
func (f Filter) NewCompatible() *Filter {
	return newWithKeys(f.m, f.keys)
}

// New optimal Bloom filter with CSPRNG keys
func NewOptimal(maxN uint64, p float64) *Filter {
	m := OptimalM(maxN, p)
	k := OptimalK(m, maxN)
	debug("New optimal bloom filter :: requested max elements (n):%d, probability of collision (p):%1.10f -> recommends -> bits (m): %d (%f GiB), number of keys (k): %d", maxN, p, m, float64(m)/(gigabitsPerGiB), k)
	return New(m, k)
}

func newWithKeys(m uint64, keys []uint64) *Filter {
	if m < M_MIN {
		panicf("m (number of bits in the bloom filter) must be >= %d", M_MIN)
	}
	if len(keys) < K_MIN {
		panicf("keys must have length %d or greater", K_MIN)
	}
	return &Filter{
		m:    m,
		n:    0,
		bits: make([]uint64, (m+63)>>6), // ceiling( m / 64 bits )
		keys: append([]uint64{}, keys...),
	}
}
