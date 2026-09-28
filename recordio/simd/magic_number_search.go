package simd

const (
	magic0 = 145
	magic1 = 141
	magic2 = 76
)

// FindMagicNumber returns the first offset at or after off where the magic number
// pattern starts, or -1 if there is none. When built with GOEXPERIMENT=simd on amd64,
// this dispatches to the widest vectorized implementation the CPU supports.
func FindMagicNumber(data []byte, off int) int {
	if len(data) < 3 {
		return -1
	}
	if off >= len(data) || off < 0 {
		return -1
	}

	return findMagicNumber(data, off)
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
		next := findMagicNumber(data, pos)
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

// findMagicNumberScalar expects len(data) >= 3 and 0 <= off < len(data).
func findMagicNumberScalar(data []byte, off int) int {
	for i := off; i < len(data)-2; i++ {
		if data[i] == magic0 &&
			data[i+1] == magic1 &&
			data[i+2] == magic2 {
			return i
		}
	}
	return -1
}
