//
// Package hamming distance calculations in Go
//
// https://github.com/steakknife/hamming
//
// Copyright © 2014, 2015, 2016, 2018 Barry Allard
//
// MIT license
//
package hamming

import (
	"math"
	"strconv"
	"testing"
)

const (
	maxInt  = (1 << (strconv.IntSize - 1)) - 1
	maxUint = (1 << strconv.IntSize) - 1
)

func TestCountBitsInt8(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsInt8(int8(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", int8(c.x), actualN, c.n)
		}
	}
}

func TestCountBitsInt16(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint16 {
			continue
		}
		if actualN := CountBitsInt16(int16(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsInt32(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint32 {
			continue
		}
		if actualN := CountBitsInt32(int32(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsInt64(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint64 {
			continue
		}
		if actualN := CountBitsInt64(int64(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsInt(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > maxUint {
			continue
		}
		if actualN := CountBitsInt(int(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint8(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsUint8(uint8(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint16(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint16 {
			continue
		}
		if actualN := CountBitsUint16(uint16(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint32(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint32 {
			continue
		}
		if actualN := CountBitsUint32(uint32(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint64(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if actualN := CountBitsUint64(c.x); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint64Alt(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if actualN := CountBitsUint64Alt(c.x); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUint(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > maxUint {
			continue
		}
		if actualN := CountBitsUint(uint(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsUintReference(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > maxUint {
			continue
		}
		if actualN := CountBitsUintReference(uint(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsByte(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsByte(byte(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsByteAlt(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsByteAlt(byte(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

func TestCountBitsRune(t *testing.T) {
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxInt32 {
			continue
		}
		if actualN := CountBitsRune(rune(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
}

// ============== benchmarks ==============
func BenchmarkCountBitsInt8(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt8(int8(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt16(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt16(int16(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt32(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt32(int32(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt64(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt64(int64(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt(int(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint16(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint16(uint16(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint32(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint32(uint32(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint64(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint64(uint64(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint64Alt(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint64Alt(uint64(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint(uint(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUintReference(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUintReference(uint(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsByte(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsByte(byte(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsByteAlt(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsByteAlt(byte(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsRune(b *testing.B) {
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsRune(rune(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}
