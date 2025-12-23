//go:build cgo

package simd

import (
	"testing"
)

func BenchmarkFindMagicNumber(b *testing.B) {
	// Create a large buffer with magic numbers scattered throughout
	data := make([]byte, 1024*1024) // 1MB
	pattern := []byte{145, 141, 76}

	// Place magic numbers every 1000 bytes, but NOT at position 0
	// Start at position 50000 to force processing significant data
	for i := 50000; i < len(data)-3; i += 1000 {
		data[i] = pattern[0]
		data[i+1] = pattern[1]
		data[i+2] = pattern[2]
	}

	// Place one at the end to ensure we find something
	data[len(data)-3] = pattern[0]
	data[len(data)-2] = pattern[1]
	data[len(data)-1] = pattern[2]

	for _, scenario := range allImplementationScenarios {
		if !scenario.available() {
			b.Skip("cpu instruction not available")
		}

		b.Run(scenario.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := scenario.fx(data, 0)
				if result < 0 {
					b.Fatal("should find magic number")
				}
			}
		})
	}
}

func BenchmarkFindAllMagicNumbers(b *testing.B) {
	// Create 1GB buffer with magic numbers every 50KB
	const dataSize = 1024 * 1024 * 1024 // 1GB
	const markerInterval = 50 * 1024    // 50KB

	data := make([]byte, dataSize)
	pattern := []byte{145, 141, 76}

	// Place magic numbers every 50KB
	for i := 0; i < len(data)-3; i += markerInterval {
		data[i] = pattern[0]
		data[i+1] = pattern[1]
		data[i+2] = pattern[2]
	}

	// Place one at the end to ensure we find something
	if len(data) >= 3 {
		data[len(data)-3] = pattern[0]
		data[len(data)-2] = pattern[1]
		data[len(data)-1] = pattern[2]
	}

	// Calculate expected number of matches
	// Markers placed at: 0, 50KB, 100KB, ..., n*50KB where n*50KB < dataSize-3
	// Plus one at the end (dataSize-3)
	expectedMatches := (dataSize-3)/markerInterval + 2 // floor division + loop markers + end marker

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(dataSize) // Report bandwidth in GB/s

	for i := 0; i < b.N; i++ {
		results := FindAllMagicNumbers(data, 0)
		if len(results) != expectedMatches {
			b.Fatalf("expected %d matches, got %d", expectedMatches, len(results))
		}
	}
}
