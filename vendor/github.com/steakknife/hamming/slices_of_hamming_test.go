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
	"reflect"
	"strconv"
	"testing"
	"unsafe"
)

const uintSize = 32 << (^uint(0) >> 32 & 1)

func TestInt8s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 8
		b0Hdr.Cap *= 64 / 8
		b0 := *(*[]int8)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 8
		b1Hdr.Cap *= 64 / 8
		b1 := *(*[]int8)(unsafe.Pointer(&b1Hdr))

		if actualN := Int8s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestInt16s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 16
		b0Hdr.Cap *= 64 / 16
		b0 := *(*[]int16)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 16
		b1Hdr.Cap *= 64 / 16
		b1 := *(*[]int16)(unsafe.Pointer(&b1Hdr))

		if actualN := Int16s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestInt32s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 32
		b0Hdr.Cap *= 64 / 32
		b0 := *(*[]int32)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 32
		b1Hdr.Cap *= 64 / 32
		b1 := *(*[]int32)(unsafe.Pointer(&b1Hdr))

		if actualN := Int32s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestInt64s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0 := *(*[]int64)(unsafe.Pointer(&c.b0))

		b1 := *(*[]int64)(unsafe.Pointer(&c.b1))

		if actualN := Int64s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestInts(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / strconv.IntSize
		b0Hdr.Cap *= 64 / strconv.IntSize
		b0 := *(*[]int)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / strconv.IntSize
		b1Hdr.Cap *= 64 / strconv.IntSize
		b1 := *(*[]int)(unsafe.Pointer(&b1Hdr))

		if actualN := Ints(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestUint8s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 8
		b0Hdr.Cap *= 64 / 8
		b0 := *(*[]uint8)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 8
		b1Hdr.Cap *= 64 / 8
		b1 := *(*[]uint8)(unsafe.Pointer(&b1Hdr))

		if actualN := Uint8s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestUint16s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 16
		b0Hdr.Cap *= 64 / 16
		b0 := *(*[]uint16)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 16
		b1Hdr.Cap *= 64 / 16
		b1 := *(*[]uint16)(unsafe.Pointer(&b1Hdr))

		if actualN := Uint16s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestUint32s(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 32
		b0Hdr.Cap *= 64 / 32
		b0 := *(*[]uint32)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 32
		b1Hdr.Cap *= 64 / 32
		b1 := *(*[]uint32)(unsafe.Pointer(&b1Hdr))

		if actualN := Uint32s(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestUint64s(t *testing.T) {
	for _, c := range testArrayCases() {
		if actualN := Uint64s(c.b0, c.b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", c.b0, c.b1, actualN, c.n)
		}
	}
}

func TestUints(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / uintSize
		b0Hdr.Cap *= 64 / uintSize
		b0 := *(*[]uint)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / uintSize
		b1Hdr.Cap *= 64 / uintSize
		b1 := *(*[]uint)(unsafe.Pointer(&b1Hdr))

		if actualN := Uints(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestBytes(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 8
		b0Hdr.Cap *= 64 / 8
		b0 := *(*[]byte)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 8
		b1Hdr.Cap *= 64 / 8
		b1 := *(*[]byte)(unsafe.Pointer(&b1Hdr))

		if actualN := Bytes(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestRunes(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0Hdr.Len *= 64 / 32
		b0Hdr.Cap *= 64 / 32
		b0 := *(*[]rune)(unsafe.Pointer(&b0Hdr))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1Hdr.Len *= 64 / 32
		b1Hdr.Cap *= 64 / 32
		b1 := *(*[]rune)(unsafe.Pointer(&b1Hdr))

		if actualN := Runes(b0, b1); actualN != c.n {
			t.Errorf("(%d,%d) -> %d != %d", b0, b1, actualN, c.n)
		}
	}
}

func TestStrings(t *testing.T) {
	for _, c := range testArrayCases() {

		b0Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b0))
		b0 := *(*string)(unsafe.Pointer(&reflect.StringHeader{
			Data: b0Hdr.Data,
			Len:  b0Hdr.Len * 64 / 8}))

		b1Hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b1))
		b1 := *(*string)(unsafe.Pointer(&reflect.StringHeader{
			Data: b1Hdr.Data,
			Len:  b1Hdr.Len * 64 / 8}))

		if actualN := Strings(b0, b1); actualN != c.n {
			t.Errorf("(%v,%v) -> %d != %d", []byte(b0), []byte(b1), actualN, c.n)
		}
	}
}
