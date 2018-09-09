//
// Package hamming distance calculations in Go
//
// https://github.com/steakknife/hamming
//
// Copyright © 2014, 2015, 2016, 2018 Barry Allard
//
// MIT license
//
// +build amd64 amd64p32 !purego

package hamming

import (
	"math"
	"testing"
	"testing/quick"
)

func TestCountBitsInt8PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsInt8PopCnt(int8(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x int8) bool {
		return CountBitsInt8PopCnt(x) == CountBitsInt8(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsInt16PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint16 {
			continue
		}
		if actualN := CountBitsInt16PopCnt(int16(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x int16) bool {
		return CountBitsInt16PopCnt(x) == CountBitsInt16(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsInt32PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint32 {
			continue
		}
		if actualN := CountBitsInt32PopCnt(int32(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x int32) bool {
		return CountBitsInt32PopCnt(x) == CountBitsInt32(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsInt64PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint64 {
			continue
		}
		if actualN := CountBitsInt64PopCnt(int64(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x int64) bool {
		return CountBitsInt64PopCnt(x) == CountBitsInt64(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsIntPopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > maxInt {
			continue
		}
		if actualN := CountBitsIntPopCnt(int(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x int) bool {
		return CountBitsIntPopCnt(x) == CountBitsInt(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsUint8PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsUint8PopCnt(uint8(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x uint8) bool {
		return CountBitsUint8PopCnt(x) == CountBitsUint8(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsUint16PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint16 {
			continue
		}
		if actualN := CountBitsUint16PopCnt(uint16(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x uint16) bool {
		return CountBitsUint16PopCnt(x) == CountBitsUint16(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsUint32PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint32 {
			continue
		}
		if actualN := CountBitsUint32PopCnt(uint32(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x uint32) bool {
		return CountBitsUint32PopCnt(x) == CountBitsUint32(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsUint64PopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint64 {
			continue
		}
		if actualN := CountBitsUint64PopCnt(c.x); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x uint64) bool {
		return CountBitsUint64PopCnt(x) == CountBitsUint64(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsUintPopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > maxUint {
			continue
		}
		if actualN := CountBitsUintPopCnt(uint(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x uint) bool {
		return CountBitsUintPopCnt(x) == CountBitsUint(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsBytePopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint8 {
			continue
		}
		if actualN := CountBitsBytePopCnt(byte(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x byte) bool {
		return CountBitsBytePopCnt(x) == CountBitsByte(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

func TestCountBitsRunePopCnt(t *testing.T) {
	if !HasPopCnt() {
		t.SkipNow()
	}
	for _, c := range testCountBitsCases() {
		if c.x > math.MaxUint32 {
			continue
		}
		if actualN := CountBitsRunePopCnt(rune(c.x)); actualN != c.n {
			t.Errorf("%d -> (actual) %d != %d (expected)", c.x, actualN, c.n)
		}
	}
	f := func(x rune) bool {
		return CountBitsRunePopCnt(x) == CountBitsRune(x)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("%v", err)
	}
}

// ============== benchmarks ==============

func BenchmarkCountBitsInt8PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt8PopCnt(int8(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt16PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt16PopCnt(int16(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt32PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt32PopCnt(int32(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsInt64PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsInt64PopCnt(int64(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsIntPopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsIntPopCnt(i)
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint8PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint8PopCnt(uint8(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint16PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint16PopCnt(uint16(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint32PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint32PopCnt(uint32(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUint64PopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUint64PopCnt(uint64(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsUintPopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsUintPopCnt(uint(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsBytePopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsBytePopCnt(byte(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}

func BenchmarkCountBitsRunePopCnt(b *testing.B) {
	if !HasPopCnt() {
		b.SkipNow()
	}
	stopDeadCodeElimination := 0
	for i := 0; i < b.N; i++ {
		stopDeadCodeElimination |= CountBitsRunePopCnt(rune(i))
	}
	nullLog().Printf("stopDeadCodeElimination: %d", stopDeadCodeElimination)
}
