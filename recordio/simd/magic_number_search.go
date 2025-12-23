//go:build !cgo

package simd

func FindMagicNumber(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	for i := off; i < len(data)-2; i++ {
		if data[i] == 145 &&
			data[i+1] == 141 &&
			data[i+2] == 76 {
			return i
		}
	}
	return -1
}
