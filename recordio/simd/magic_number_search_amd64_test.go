//go:build goexperiment.simd && amd64

package simd

var allImplementationScenarios = []struct {
	name      string
	fx        func([]byte, int) int
	available func() bool
}{
	{"Scalar", findMagicNumberScalar, func() bool { return true }},
	{"AVX2", findMagicNumberAVX2, func() bool { return avx2Supported }},
	{"AVX512", findMagicNumberAVX512, func() bool { return avx512Supported }},
}
