//go:build cgo

package simd

/*
#cgo CFLAGS: -msse4.2 -mavx2 -mavx512f -mavx512bw
#include "search.h"
*/
import "C"
import (
	"unsafe"
)

var (
	sse42Supported  bool
	avx2Supported   bool
	avx512Supported bool
)

func init() {
	sse42Supported = int(C.cpu_supports_sse42()) == 1
	avx2Supported = int(C.cpu_supports_avx2()) == 1
	avx512Supported = int(C.cpu_supports_avx512()) == 1
}

func cgo_find_magic_numbers_avx512(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(off)
	length := C.size_t(len(data))
	return int(C.find_magic_numbers_avx512(ptr, offset, length))
}

func cgo_find_magic_numbers_sse4(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(off)
	length := C.size_t(len(data))
	return int(C.find_magic_numbers_sse4(ptr, offset, length))
}

func cgo_find_magic_numbers_avx2(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(off)
	length := C.size_t(len(data))
	return int(C.find_magic_numbers_avx2(ptr, offset, length))
}

func cgo_find_magic_numbers_scalar(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	ptr := (*C.uchar)(unsafe.Pointer(&data[0]))
	offset := C.size_t(off)
	length := C.size_t(len(data))
	return int(C.find_magic_numbers_scalar(ptr, offset, length))
}

func FindMagicNumber(data []byte, off int) int {
	// Use best available implementation: AVX512 > AVX2 > SSE4 > Scalar
	if avx512Supported {
		return cgo_find_magic_numbers_avx512(data, off)
	} else if avx2Supported {
		return cgo_find_magic_numbers_avx2(data, off)
	} else if sse42Supported {
		return cgo_find_magic_numbers_sse4(data, off)
	}

	return cgo_find_magic_numbers_scalar(data, off)
}

// FindAllMagicNumbers finds all occurrences of the magic number pattern in the data,
// starting from the given offset. Returns a slice of all offsets where the pattern was found.
func FindAllMagicNumbers(data []byte, off int) []int {
	if len(data) < 3 {
		return nil
	}
	if off >= len(data) || off < 0 {
		return nil
	}

	var results []int
	pos := off

	for {
		next := FindMagicNumber(data, pos)
		if next < 0 {
			break
		}
		results = append(results, next)
		// Start searching from the next position after this match
		pos = next + 1
		if pos >= len(data)-2 {
			break
		}
	}

	return results
}
