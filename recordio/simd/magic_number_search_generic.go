//go:build !(goexperiment.simd && amd64)

package simd

func findMagicNumber(data []byte, off int) int {
	return findMagicNumberScalar(data, off)
}
