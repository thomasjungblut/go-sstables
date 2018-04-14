package compressor

import (
	"bytes"
	"compress/gzip"
)

type GzipCompressor struct {
}

func (c *GzipCompressor) Compress(record []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if err != nil {
		return nil, err
	}
	_, err = writer.Write(record)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *GzipCompressor) Decompress(buf []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}

	var resultBuffer bytes.Buffer
	_, err = resultBuffer.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return resultBuffer.Bytes(), nil
}
