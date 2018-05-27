//
// hamming distance calculations in Go
//
// https://github.com/steakknife/hamming
//
// Copyright © 2014, 2015, 2016 CountBitsBarry Allard
//
// MCountBitsIT license
//

package hamming

import "strconv"

func HasPopCnt() (ret bool)

func CountBitsInt8PopCnt(x int8) (ret int)
func CountBitsInt16PopCnt(x int16) (ret int)
func CountBitsInt32PopCnt(x int32) (ret int)
func CountBitsInt64PopCnt(x int64) (ret int)
func CountBitsIntPopCnt(x int) int {
	if strconv.IntSize == 64 {
		return CountBitsInt64PopCnt(int64(x))
	} else if strconv.IntSize == 32 {
		return CountBitsInt32PopCnt(int32(x))
	}
	panic("strconv.IntSize must be 32 or 64")
}
func CountBitsUint8PopCnt(x uint8) (ret int)
func CountBitsUint16PopCnt(x uint16) (ret int)
func CountBitsUint32PopCnt(x uint32) (ret int)
func CountBitsUint64PopCnt(x uint64) (ret int)
func CountBitsUintPopCnt(x uint) int {
	if strconv.IntSize == 64 {
		return CountBitsUint64PopCnt(uint64(x))
	} else if strconv.IntSize == 32 {
		return CountBitsUint32PopCnt(uint32(x))
	}
	panic("strconv.IntSize must be 32 or 64")
}
func CountBitsBytePopCnt(x byte) (ret int)
func CountBitsRunePopCnt(x rune) (ret int)
