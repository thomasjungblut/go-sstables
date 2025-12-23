//go:build cgo

package simd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var allImplementationScenarios = []struct {
	name      string
	fx        func([]byte, int) int
	available func() bool
}{
	{"Scalar", cgo_find_magic_numbers_scalar, func() bool { return true }},
	{"SSE4", cgo_find_magic_numbers_sse4, func() bool { return sse42Supported }},
	{"AVX2", cgo_find_magic_numbers_avx2, func() bool { return avx2Supported }},
	{"AVX512", cgo_find_magic_numbers_avx512, func() bool { return avx512Supported }},
}

func TestMagicNumberSearchByImplementationHappyPath(t *testing.T) {
	for _, scenario := range allImplementationScenarios {
		if !scenario.available() {
			t.Skip("cpu instruction not available")
		}
		t.Run(scenario.name, func(t *testing.T) {
			data := make([]byte, 10000)

			firstMarker := 10000 - 300
			data[firstMarker] = 145
			data[firstMarker+1] = 141
			data[firstMarker+2] = 76

			secondMarker := 10000 - 3
			data[secondMarker] = 145
			data[secondMarker+1] = 141
			data[secondMarker+2] = 76

			// check the entire range
			for i := 0; i < len(data); i++ {
				actualResult := scenario.fx(data, i)
				expectedResult := firstMarker
				if i >= firstMarker+1 {
					expectedResult = secondMarker
				}

				if i > secondMarker {
					expectedResult = -1
				}

				require.Equalf(t, expectedResult, actualResult, "unexpected result at offset %d", i)
			}
		})
	}
}

func TestMagicNumberSearchByImplementation(t *testing.T) {
	for _, scenario := range allImplementationScenarios {
		if !scenario.available() {
			t.Skip("cpu instruction not available")
		}
		t.Run(scenario.name, func(t *testing.T) {
			t.Run("BoundarySizes", func(t *testing.T) {
				// Test sizes around critical boundaries for SIMD implementations
				for size := 9; size < 100; size++ {
					t.Run(fmt.Sprintf("Size%d", size), func(t *testing.T) {
						data := make([]byte, size)

						// Place pattern at various positions
						positions := []int{0, size - 3, size / 2}

						for _, pos := range positions {
							if pos >= 0 && pos+2 < size {
								// Clear and set pattern
								for i := range data {
									data[i] = 0
								}
								data[pos] = 145
								data[pos+1] = 141
								data[pos+2] = 76

								result := scenario.fx(data, 0)
								require.Equalf(t, pos, result, "pattern at position %d in buffer of size %d", pos, size)
							}
						}
					})
				}
			})

			t.Run("LoopBoundaries", func(t *testing.T) {
				// Test offsets that might cause issues with loop boundaries
				// Test with pattern just before the loop would exit
				for offset := 0; offset < 100; offset++ {
					data := make([]byte, 100+offset)
					data[offset+50] = 145
					data[offset+51] = 141
					data[offset+52] = 76

					result := scenario.fx(data, offset)
					require.Equalf(t, offset+50, result, "offset %d", offset)
				}
			})

			t.Run("NearEndOfBuffer", func(t *testing.T) {
				// Test cases where pattern is near the end, requiring fallback loop
				for size := 30; size < 70; size++ {
					data := make([]byte, size)
					// Pattern at the very end
					data[size-3] = 145
					data[size-2] = 141
					data[size-1] = 76

					result := scenario.fx(data, 0)
					require.Equalf(t, size-3, result, "size %d", size)

					// Pattern just before end (should use fallback)
					if size > 35 {
						data[size-4] = 145
						data[size-3] = 141
						data[size-2] = 76
						data[size-1] = 0

						result = scenario.fx(data, 0)
						require.Equalf(t, size-4, result, "size %d, pattern at size-4", size)
					}
				}
			})

			t.Run("MultipleIterations", func(t *testing.T) {
				// Test with buffer large enough for multiple SIMD iterations
				for _, size := range []int{60, 90, 120, 150} {
					data := make([]byte, size)

					// Place pattern at various positions across iterations
					positions := []int{0, 30, 60, size - 3}

					for _, pos := range positions {
						if pos >= 0 && pos+2 < size {
							// Clear and set pattern
							for i := range data {
								data[i] = 0
							}
							data[pos] = 145
							data[pos+1] = 141
							data[pos+2] = 76

							result := scenario.fx(data, 0)
							require.Equalf(t, pos, result, "size %d, pattern at %d", size, pos)
						}
					}
				}
			})

			t.Run("OffsetWithinLoop", func(t *testing.T) {
				// Test with non-zero starting offsets that fall within the SIMD loop
				data := make([]byte, 100)
				data[50] = 145
				data[51] = 141
				data[52] = 76

				// Test various starting offsets
				for offset := 0; offset < 50; offset++ {
					result := scenario.fx(data, offset)
					require.Equalf(t, 50, result, "offset %d", offset)
				}
			})

			// AVX2 specific: loop condition is i + 32 <= end where end = len - 2
			// So loop runs when i + 32 <= len - 2, i.e., i + 34 <= len
			// Test exact boundary: len = 34 means loop runs once (i=0, 0+32=32 <= 34-2=32)
			// Test just below: len = 33 means loop doesn't run (0+32=32 > 33-2=31)
			t.Run("ExactLoopBoundary", func(t *testing.T) {
				// Test size 33: loop should NOT run, fallback should handle it
				data33 := make([]byte, 33)
				data33[30] = 145
				data33[31] = 141
				data33[32] = 76
				result := scenario.fx(data33, 0)
				require.Equal(t, 30, result, "size 33, pattern at end")

				// Test size 34: loop SHOULD run once
				data34 := make([]byte, 34)
				data34[31] = 145
				data34[32] = 141
				data34[33] = 76
				result = scenario.fx(data34, 0)
				require.Equal(t, 31, result, "size 34, pattern at end")

				// Test size 35: loop should run once
				data35 := make([]byte, 35)
				data35[32] = 145
				data35[33] = 141
				data35[34] = 76
				result = scenario.fx(data35, 0)
				require.Equal(t, 32, result, "size 35, pattern at end")
			})

			// Test the transition point where loop stops and fallback takes over
			t.Run("LoopToFallbackTransition", func(t *testing.T) {
				// AVX2 loop advances by 30, so test around positions 30-34
				for size := 32; size <= 70; size++ {
					t.Run(fmt.Sprintf("Size%d", size), func(t *testing.T) {
						// Test pattern at various positions near the boundary
						testPositions := []int{}
						if size >= 3 {
							testPositions = append(testPositions, size-3) // at end
						}
						if size >= 35 {
							testPositions = append(testPositions, 30) // in loop range
							testPositions = append(testPositions, 31) // in loop range
							testPositions = append(testPositions, 32) // boundary
							testPositions = append(testPositions, 33) // boundary
						}

						for _, pos := range testPositions {
							if pos >= 0 && pos+2 < size {
								data := make([]byte, size)
								for i := range data {
									data[i] = 0
								}
								data[pos] = 145
								data[pos+1] = 141
								data[pos+2] = 76

								result := scenario.fx(data, 0)
								require.Equalf(t, pos, result, "size %d, pattern at position %d", size, pos)
							}
						}
					})
				}
			})

			// Test that we don't read past buffer boundaries
			t.Run("BufferBoundarySafety", func(t *testing.T) {
				// AVX2 loads 32 bytes at offsets i, i+1, i+2
				// Maximum load is at i+2, loading 32 bytes = needs i+2+32 = i+34 <= len
				// Test sizes that are exactly at the boundary
				for size := 30; size <= 40; size++ {
					data := make([]byte, size)
					// Place pattern at the last valid position
					if size >= 3 {
						data[size-3] = 145
						data[size-2] = 141
						data[size-1] = 76

						result := scenario.fx(data, 0)
						require.Equalf(t, size-3, result, "size %d, pattern at last position", size)
					}
				}
			})
		})
	}
}
