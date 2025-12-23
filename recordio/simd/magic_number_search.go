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
