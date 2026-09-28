package compressor

import "io"

// sliceWriter appends everything written to buf, it allows writing into a caller-supplied buffer without the
// allocation of a bytes.Buffer.
type sliceWriter struct {
	buf []byte
}

func (s *sliceWriter) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}

// readAllInto reads r until EOF into the capacity of buf, it only grows buf when the output doesn't fit.
// Unlike bytes.Buffer.ReadFrom, a buffer that fits the output exactly is never reallocated to probe for EOF, the given
// single byte probe slice is used for that instead.
func readAllInto(r io.Reader, buf []byte, probe []byte) ([]byte, error) {
	buf = buf[:0]
	if cap(buf) == 0 {
		buf = make([]byte, 0, 512)
	}
	for {
		if len(buf) == cap(buf) {
			n, err := io.ReadFull(r, probe[:1])
			if n == 0 {
				if err == io.EOF {
					return buf, nil
				}
				return buf, err
			}
			buf = append(buf, probe[0])
			continue
		}

		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
	}
}
