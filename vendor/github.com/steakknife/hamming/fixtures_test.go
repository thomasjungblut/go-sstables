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

type testCountCase struct {
	x uint64
	n int
}

func testCountBitsCases() []testCountCase {
	return []testCountCase{
		{0x00, 0},
		{0x01, 1},
		{0x02, 1},
		{0x03, 2},
		{0x04, 1},
		{0x05, 2},
		{0x06, 2},
		{0x07, 3},
		{0x08, 1},
		{0x09, 2},
		{0x0a, 2},
		{0x0e, 3},
		{0x0f, 4},
		{0x10, 1},
		{0xf0, 4},
		{0xf1, 5},
		{0x77, 6},
		{0xaa, 4},
		{0x55, 4},
		{0x7f, 7},
		{0xfe, 7},
		{0xff, 8},
		{0x100, 1},
		{0x101, 2},
		{0xdad, 8},
		{0x1111, 4},
		{0x7fff, 15},
		{0xbeef, 13},
		{0xfffe, 15},
		{0xffff, 16},
		{0x10000, 1},
		{0x10001, 2},
		{0xffffffff, 32},
		{0x1ffffffff, 33},
		{0x3ffffffff, 34},
		{0x7fffffffe, 34},
		{0x7ffffffff, 35},
		{0xfffffffff, 36},
		{0xfffffffff0, 36},
		{0xfffffffff1, 37},
		{0xfffffffff00, 36},
		{0xfffffffff000, 36},
		{0xfffffffff0000, 36},
		{0xfffffffff00000, 36},
		{0xfffffffff000000, 36},
		{0xfffffffff0000000, 36},
		{0xfffffffff0000001, 37},
		{0x3fffffffffffffff, 62},
		{0x4000000000000000, 1},
		{0x7ffffffffffffffe, 62},
		{0x7fffffffffffffff, 63},
		{0x8000000000000000, 1},
		{0x8000000000000001, 2},
		{0x8000000000000002, 2},
		{0xdeadbeefdeadbeef, 48},
		{0xfffffffffffffffe, 63},
		{0xffffffffffffffff, 64},
	}
}

type testArrayCase struct {
	b0, b1 []uint64
	n      int
}

func testArrayCases() []testArrayCase {
	return []testArrayCase{
		{[]uint64{}, []uint64{}, 0},
		{[]uint64{1}, []uint64{0}, 1},
		{[]uint64{1}, []uint64{2}, 2},
		{[]uint64{0, 0}, []uint64{0, 0}, 0},
		{[]uint64{0, 0}, []uint64{1, 0}, 1},
		{[]uint64{0, 1}, []uint64{1, 0}, 2},
		{[]uint64{1, 0}, []uint64{0, 1}, 2},
	}
}

type testSliceCase struct {
	b []uint64
	n int
}

func testSliceCases() []testSliceCase {
	return []testSliceCase{
		{[]uint64{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 64},
		{[]uint64{}, 0},
		{[]uint64{0}, 0},
		{[]uint64{2}, 1},
		{[]uint64{0, 0}, 0},
		{[]uint64{1, 0}, 1},
		{[]uint64{1, 1}, 2},
		{[]uint64{0, 1}, 1},
		{[]uint64{0x0f, 0x10, 1, 2, 4, 8, 16}, 10},
		{[]uint64{0x0f, 0x10, 1, 2, 4, 8, 16, 32}, 11},
		{[]uint64{0x0f, 0x10}, 5},
		{[]uint64{0xff, 0xfe}, 15},
		{[]uint64{0xff, 0xff}, 16},
	}
}
