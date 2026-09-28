package testutil

import (
	"math/rand"
	"slices"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	assert.Len(t, Bytes(nil, 0), 0)
	assert.Len(t, Bytes(nil, 42), 42)
	assert.Equal(t, Bytes(rand.New(rand.NewSource(1)), 64), Bytes(rand.New(rand.NewSource(1)), 64))
}

func TestString(t *testing.T) {
	s := String(nil, 1000)
	assert.Equal(t, 1000, utf8.RuneCountInString(s))
	for _, r := range s {
		assert.Less(t, r, rune(255))
	}
	assert.Equal(t, String(rand.New(rand.NewSource(1)), 64), String(rand.New(rand.NewSource(1)), 64))
}

func TestLetters(t *testing.T) {
	s := Letters(nil, 1000)
	assert.Len(t, s, 1000)
	for _, r := range s {
		assert.True(t, r >= 'a' && r <= 'z', "unexpected rune %q", r)
	}
	assert.Equal(t, Letters(rand.New(rand.NewSource(1)), 64), Letters(rand.New(rand.NewSource(1)), 64))
}

func TestIntegers(t *testing.T) {
	ints := Integers[int](nil, 1000)
	assert.Len(t, ints, 1000)
	for _, i := range ints {
		assert.True(t, i >= 0 && i < 1<<31, "unexpected integer %d", i)
	}
	assert.Equal(t, Integers[uint64](rand.New(rand.NewSource(1)), 64), Integers[uint64](rand.New(rand.NewSource(1)), 64))
}

func TestSortedIntegers(t *testing.T) {
	ints := SortedIntegers[uint32](nil, 1000)
	assert.Len(t, ints, 1000)
	assert.True(t, slices.IsSorted(ints))
}
