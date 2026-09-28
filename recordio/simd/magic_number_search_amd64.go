//go:build goexperiment.simd && amd64

package simd

import (
	"math/bits"
	"simd/archsimd"
)

var (
	avx2Supported   = archsimd.X86.AVX2()
	avx512Supported = archsimd.X86.AVX512()
)

func findMagicNumber(data []byte, off int) int {
	// Use best available implementation: AVX512 > AVX2 > Scalar
	if avx512Supported {
		return findMagicNumberAVX512(data, off)
	} else if avx2Supported {
		return findMagicNumberAVX2(data, off)
	}

	return findMagicNumberScalar(data, off)
}

// findMagicNumberAVX2 compares three 32 byte loads shifted by one byte each against
// the broadcast pattern bytes. Lane k of the combined mask is set iff the pattern
// starts at i+k, so every iteration checks 32 complete positions.
func findMagicNumberAVX2(data []byte, off int) int {
	p0 := archsimd.BroadcastUint8x32(magic0)
	p1 := archsimd.BroadcastUint8x32(magic1)
	p2 := archsimd.BroadcastUint8x32(magic2)

	// Shrinking the remaining slice instead of indexing with i lets the loop condition
	// prove all loads in bounds, so the compiler emits no bounds checks in the loop.
	// The load at offset 2 needs 32 readable bytes, hence the window of 32+2 bytes.
	rest := data[off:]
	for len(rest) >= 32+2 {
		win := (*[32 + 2]byte)(rest)
		d0 := archsimd.LoadUint8x32((*[32]byte)(win[0:]))
		d1 := archsimd.LoadUint8x32((*[32]byte)(win[1:]))
		d2 := archsimd.LoadUint8x32((*[32]byte)(win[2:]))

		mask := d0.Equal(p0).And(d1.Equal(p1)).And(d2.Equal(p2)).ToBits()
		if mask != 0 {
			return len(data) - len(rest) + bits.TrailingZeros32(mask)
		}
		rest = rest[32:]
	}

	return findMagicNumberScalar(data, len(data)-len(rest))
}

// findMagicNumberAVX512 works like findMagicNumberAVX2 with 64 byte vectors, the
// comparisons produce AVX-512 mask registers directly.
func findMagicNumberAVX512(data []byte, off int) int {
	p0 := archsimd.BroadcastUint8x64(magic0)
	p1 := archsimd.BroadcastUint8x64(magic1)
	p2 := archsimd.BroadcastUint8x64(magic2)

	// Shrinking the remaining slice instead of indexing with i lets the loop condition
	// prove all loads in bounds, so the compiler emits no bounds checks in the loop.
	// The load at offset 2 needs 64 readable bytes, hence the window of 64+2 bytes.
	rest := data[off:]
	for len(rest) >= 64+2 {
		win := (*[64 + 2]byte)(rest)
		d0 := archsimd.LoadUint8x64((*[64]byte)(win[0:]))
		d1 := archsimd.LoadUint8x64((*[64]byte)(win[1:]))
		d2 := archsimd.LoadUint8x64((*[64]byte)(win[2:]))

		mask := d0.Equal(p0).And(d1.Equal(p1)).And(d2.Equal(p2)).ToBits()
		if mask != 0 {
			return len(data) - len(rest) + bits.TrailingZeros64(mask)
		}
		rest = rest[64:]
	}

	return findMagicNumberScalar(data, len(data)-len(rest))
}
