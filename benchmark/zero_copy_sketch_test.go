package benchmark

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
)

var zeroCopyModes = []string{"copy", "into", "view"}

func BenchmarkZeroCopyFileReader(b *testing.B) {
	for _, c := range benchCompressionTypes[:2] {
		for _, size := range benchRecordSizes {
			for _, mode := range zeroCopyModes {
				b.Run(fmt.Sprintf("%s/%d/%s", c.name, size, mode), func(b *testing.B) {
					path, _, fileSize := writeBenchFile(b, size, c.comp)
					b.SetBytes(fileSize)
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						b.StopTimer()
						dropPageCache(b, path)
						b.StartTimer()
						reader, err := recordio.NewFileReaderWithPath(path)
						require.NoError(b, err)
						require.NoError(b, reader.Open())
						fr := reader.(*recordio.FileReader)
						var buf []byte
						for {
							switch mode {
							case "copy":
								_, err = fr.ReadNext()
							case "into":
								buf, err = fr.ReadNextInto(buf)
							case "view":
								_, err = fr.ReadNextView()
							}
							if errors.Is(err, io.EOF) {
								break
							}
							require.NoError(b, err)
						}
						require.NoError(b, reader.Close())
					}
				})
			}
		}
	}
}

func BenchmarkZeroCopyMMap(b *testing.B) {
	for _, c := range benchCompressionTypes[:2] {
		for _, size := range benchRecordSizes {
			for _, mode := range zeroCopyModes {
				b.Run(fmt.Sprintf("%s/%d/%s", c.name, size, mode), func(b *testing.B) {
					path, offsets, fileSize := writeBenchFile(b, size, c.comp)
					b.SetBytes(fileSize)
					b.ReportAllocs()
					b.ResetTimer()
					var buf []byte
					var err error
					for n := 0; n < b.N; n++ {
						reader := openColdMMapReader(b, path)
						mr := reader.(*recordio.MMapReader)
						for _, off := range offsets {
							switch mode {
							case "copy":
								_, err = mr.ReadNextAt(off)
							case "into":
								buf, err = mr.ReadNextAtInto(off, buf)
							case "view":
								_, err = mr.ReadNextAtView(off)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						require.NoError(b, reader.Close())
					}
				})
			}
		}
	}
}

// correctness check of the sketch against ReadNext
func TestZeroCopySketchMatchesReadNext(t *testing.T) {
	b := &testing.B{}
	_ = b
	for _, c := range benchCompressionTypes {
		for _, size := range []int{16, 5000} {
			path, offsets, _ := writeBenchFileT(t, size, c.comp)
			a, _ := recordio.NewFileReaderWithPath(path)
			v, _ := recordio.NewFileReaderWithPath(path)
			in, _ := recordio.NewFileReaderWithPath(path)
			require.NoError(t, a.Open())
			require.NoError(t, v.Open())
			require.NoError(t, in.Open())
			m, _ := recordio.NewMemoryMappedReaderWithPath(path)
			require.NoError(t, m.Open())
			var buf, mbuf []byte
			for _, off := range offsets {
				exp, err := a.ReadNext()
				require.NoError(t, err)
				view, err := v.(*recordio.FileReader).ReadNextView()
				require.NoError(t, err)
				require.Equal(t, exp, view)
				buf, err = in.(*recordio.FileReader).ReadNextInto(buf)
				require.NoError(t, err)
				require.Equal(t, exp, buf)
				mv, err := m.(*recordio.MMapReader).ReadNextAtView(off)
				require.NoError(t, err)
				require.Equal(t, exp, mv)
				mbuf, err = m.(*recordio.MMapReader).ReadNextAtInto(off, mbuf)
				require.NoError(t, err)
				require.Equal(t, exp, mbuf)
			}
		}
	}
}
