//
// hamming distance calculations in Go
//
// https://github.com/steakknife/hamming
//
// Copyright © 2014, 2015, 2016 Barry Allard
//
// MIT license
//

package hamming

func CountBitsInt8s(b []int8) int {
	c := 0
	for _, x := range b {
		c += CountBitsInt8(x)
	}
	return c
}

func CountBitsInt16s(b []int16) int {
	c := 0
	for _, x := range b {
		c += CountBitsInt16(x)
	}
	return c
}

func CountBitsInt32s(b []int32) int {
	c := 0
	for _, x := range b {
		c += CountBitsInt32(x)
	}
	return c
}

func CountBitsInt64s(b []int64) int {
	c := 0
	for _, x := range b {
		c += CountBitsInt64(x)
	}
	return c
}

func CountBitsInts(b []int) int {
	c := 0
	for _, x := range b {
		c += CountBitsInt(x)
	}
	return c
}

func CountBitsUint8s(b []uint8) int {
	c := 0
	for _, x := range b {
		c += CountBitsUint8(x)
	}
	return c
}

func CountBitsUint16s(b []uint16) int {
	c := 0
	for _, x := range b {
		c += CountBitsUint16(x)
	}
	return c
}

func CountBitsUint32s(b []uint32) int {
	c := 0
	for _, x := range b {
		c += CountBitsUint32(x)
	}
	return c
}

func CountBitsUint64s(b []uint64) int {
	c := 0
	for _, x := range b {
		c += CountBitsUint64(x)
	}
	return c
}

func CountBitsUints(b []uint) int {
	c := 0
	for _, x := range b {
		c += CountBitsUint(x)
	}
	return c
}

func CountBitsBytes(b []byte) int {
	c := 0
	for _, x := range b {
		c += CountBitsByte(x)
	}
	return c
}

func CountBitsRunes(b []rune) int {
	c := 0
	for _, x := range b {
		c += CountBitsRune(x)
	}
	return c
}

func CountBitsString(s string) int {
	return CountBitsBytes([]byte(s))
}
