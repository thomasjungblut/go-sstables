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
	"testing"
	"testing/quick"
)

func refInt8(x, y int8) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refInt16(x, y int16) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refInt32(x, y int32) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refInt64(x, y int64) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refUint8(x, y uint8) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refUint16(x, y uint16) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refUint32(x, y uint32) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refUint64(x, y uint64) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refByte(x, y byte) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}
func refRune(x, y rune) (r int) {
	x ^= y
	for x != 0 {
		r++
		x &= x - 1
	}
	return r
}

func TestInt8(t *testing.T) {
	f := func(x, y int8) bool {
		return Int8(x, y) == refInt8(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestInt16(t *testing.T) {
	f := func(x, y int16) bool {
		return Int16(x, y) == refInt16(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestInt32(t *testing.T) {
	f := func(x, y int32) bool {
		return Int32(x, y) == refInt32(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestInt64(t *testing.T) {
	f := func(x, y int64) bool {
		return Int64(x, y) == refInt64(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestUint8(t *testing.T) {
	f := func(x, y uint8) bool {
		return Uint8(x, y) == refUint8(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestUint16(t *testing.T) {
	f := func(x, y uint16) bool {
		return Uint16(x, y) == refUint16(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestUint32(t *testing.T) {
	f := func(x, y uint32) bool {
		return Uint32(x, y) == refUint32(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestUint64(t *testing.T) {
	f := func(x, y uint64) bool {
		return Uint64(x, y) == refUint64(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestByte(t *testing.T) {
	f := func(x, y byte) bool {
		return Byte(x, y) == refByte(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestRune(t *testing.T) {
	f := func(x, y rune) bool {
		return Rune(x, y) == refRune(x, y)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
