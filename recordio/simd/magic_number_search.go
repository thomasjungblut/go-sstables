package simd

/*
#cgo CFLAGS: -mavx2
#include "search.h"
*/
import "C"
import "unsafe"

func AVXSupported() bool {
	result := C.cpu_supports_avx2()
	return int(result) == 1
}

func FindFirstMagicNumber(data []byte) int {
	if len(data) < 3 {
		return -1
	}
	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(0)
	length := C.size_t(len(data))
	result := C.find_magic_numbers(ptr, offset, length)
	return int(result)
}

func FindMagicNumber(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}
	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(off)
	length := C.size_t(len(data))
	result := C.find_magic_numbers(ptr, offset, length)
	return int(result)
}
