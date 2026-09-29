package testutil

import (
	"math/rand"
	"slices"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func seeded() *rand.Rand {
	return rand.New(rand.NewSource(1))
}

func TestBytes(t *testing.T) {
	assert.Len(t, Bytes(0), 0)
	assert.Len(t, Bytes(42), 42)
	assert.Len(t, BytesRng(seeded(), 42), 42)
	assert.Equal(t, BytesRng(seeded(), 64), BytesRng(seeded(), 64))
}

func TestString(t *testing.T) {
	for _, s := range []string{String(1000), StringRng(seeded(), 1000)} {
		assert.Equal(t, 1000, utf8.RuneCountInString(s))
		for _, r := range s {
			assert.Less(t, r, rune(255))
		}
	}
	assert.Equal(t, StringRng(seeded(), 64), StringRng(seeded(), 64))
}

func TestLetters(t *testing.T) {
	for _, s := range []string{Letters(1000), LettersRng(seeded(), 1000)} {
		assert.Len(t, s, 1000)
		for _, r := range s {
			assert.True(t, r >= 'a' && r <= 'z', "unexpected rune %q", r)
		}
	}
	assert.Equal(t, LettersRng(seeded(), 64), LettersRng(seeded(), 64))
}

func TestIntegers(t *testing.T) {
	for _, ints := range [][]int{Integers[int](1000), IntegersRng[int](seeded(), 1000)} {
		assert.Len(t, ints, 1000)
		for _, i := range ints {
			assert.True(t, i >= 0 && i < 1<<31, "unexpected integer %d", i)
		}
	}
	assert.Equal(t, IntegersRng[uint64](seeded(), 64), IntegersRng[uint64](seeded(), 64))
}

func TestSortedIntegers(t *testing.T) {
	assert.True(t, slices.IsSorted(SortedIntegers[uint32](1000)))
	assert.True(t, slices.IsSorted(SortedIntegersRng[uint32](seeded(), 1000)))
	assert.Equal(t, SortedIntegersRng[int](seeded(), 64), SortedIntegersRng[int](seeded(), 64))
}
