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

import (
	"strconv"
	"unsafe"
)

func CountBitsInt8sPopCnt(x []int8) (ret int)
func CountBitsInt16sPopCnt(x []int16) (ret int)
func CountBitsInt32sPopCnt(x []int32) (ret int)
func CountBitsInt64sPopCnt(x []int64) (ret int)
func CountBitsIntsPopCnt(x []int) int {
	if strconv.IntSize == 64 {
		y := (*[]int64)(unsafe.Pointer(&x))
		return CountBitsInt64sPopCnt(*y)
	} else if strconv.IntSize == 32 {
		y := (*[]int32)(unsafe.Pointer(&x))
		return CountBitsInt32sPopCnt(*y)
	}
	panic("strconv.IntSize must be 32 or 64 bits")
}
func CountBitsUint8sPopCnt(x []uint8) (ret int)
func CountBitsUint16sPopCnt(x []uint16) (ret int)
func CountBitsUint32sPopCnt(x []uint32) (ret int)
func CountBitsUint64sPopCnt(x []uint64) (ret int)
func CountBitsUintsPopCnt(x []uint) int {
	if strconv.IntSize == 64 {
		y := (*[]uint64)(unsafe.Pointer(&x))
		return CountBitsUint64sPopCnt(*y)
	} else if strconv.IntSize == 32 {
		y := (*[]uint32)(unsafe.Pointer(&x))
		return CountBitsUint32sPopCnt(*y)
	}
	panic("strconv.IntSize must be 32 or 64 bits")
}
func CountBitsBytesPopCnt(x []byte) (ret int)
func CountBitsRunesPopCnt(x []rune) (ret int)
func CountBitsStringPopCnt(s string) (ret int)
