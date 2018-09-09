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

func TestCountBitsInt8s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 8
		bHdr.Cap *= 64 / 8
		b := *(*[]int8)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsInt8s(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}

	t0, t0Cnt := []int8{1, 2, 3, 4, 5}, 1+1+2+1+2
	if actualN := CountBitsInt8s(t0); actualN != t0Cnt {
		t.Errorf("(%d) -> %d != %d", t0, actualN, t0Cnt)
	}

	t1, t1Cnt := []int8{1, 2, 3, 4, 5, 7}, 1+1+2+1+2+3
	if actualN := CountBitsInt8s(t1); actualN != t1Cnt {
		t.Errorf("(%d) -> %d != %d", t1, actualN, t1Cnt)
	}

	t2, t2Cnt := []int8{1, 2, 3, 4, 5, 7, -1}, 1+1+2+1+2+3+8
	if actualN := CountBitsInt8s(t2); actualN != t2Cnt {
		t.Errorf("(%d) -> %d != %d", t2, actualN, t2Cnt)
	}

	t3, t3Cnt := []int8{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 5, 7, -1},
		1+1+2+1+2+3+8+1+1
	if actualN := CountBitsInt8s(t3); actualN != t3Cnt {
		t.Errorf("(%d) -> %d != %d", t3, actualN, t3Cnt)
	}
}

func TestCountBitsInt16s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 16
		bHdr.Cap *= 64 / 16
		b := *(*[]int16)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsInt16s(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsInt32s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 32
		bHdr.Cap *= 64 / 32
		b := *(*[]int32)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsInt32s(b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsInt64s(t *testing.T) {
	for _, c := range testSliceCases() {

		b := *(*[]int64)(unsafe.Pointer(&c.b))

		if actualN := CountBitsInt64s(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsInts(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / strconv.IntSize
		bHdr.Cap *= 64 / strconv.IntSize
		b := *(*[]int)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsInts(b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsUint8s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 8
		bHdr.Cap *= 64 / 8
		b := *(*[]uint8)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsUint8s(b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsUint16s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 16
		bHdr.Cap *= 64 / 16
		b := *(*[]uint16)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsUint16s(b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsUint32s(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 32
		bHdr.Cap *= 64 / 32
		b := *(*[]uint32)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsUint32s(b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsUint64s(t *testing.T) {
	for _, c := range testSliceCases() {
		if actualN := CountBitsUint64s(c.b); actualN != c.n {
			t.Errorf("(%d) -> actual %d != expected %d", c.b, actualN, c.n)
		} else {
			t.Logf("(%d) -> actual %d == expected %d", c.b, actualN, c.n)
		}
	}
}

func TestCountBitsUints(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / strconv.IntSize
		bHdr.Cap *= 64 / strconv.IntSize
		b := *(*[]uint)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsUints(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsBytes(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 8
		bHdr.Cap *= 64 / 8
		b := *(*[]byte)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsBytes(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsRunes(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		bHdr.Len *= 64 / 32
		bHdr.Cap *= 64 / 32
		b := *(*[]rune)(unsafe.Pointer(&bHdr))

		if actualN := CountBitsRunes(b); actualN != c.n {
			t.Errorf("(%d) -> %d != %d", b, actualN, c.n)
		}
	}
}

func TestCountBitsString(t *testing.T) {
	for _, c := range testSliceCases() {

		bHdr := *(*reflect.SliceHeader)(unsafe.Pointer(&c.b))
		b := *(*string)(unsafe.Pointer(&reflect.StringHeader{
			Data: bHdr.Data,
			Len:  bHdr.Len * 64 / 8}))

		if actualN := CountBitsString(b); actualN != c.n {
			t.Errorf("(%v) -> %d != %d", []byte(b), actualN, c.n)
		}
	}
}
