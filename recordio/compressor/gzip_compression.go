package compressor

import (
	"bytes"
	"compress/gzip"
)

type GzipCompressor struct {
}

func (c *GzipCompressor) Compress(record []byte) ([]byte, error) {
	var buf bytes.Buffer
	return compressWithBytesBuffer(record, &buf)
}

func (c *GzipCompressor) CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error) {
	// we have to set the length of the buffer (keeping capacity) to make sure gzip doesn't append
	destinationBuffer = destinationBuffer[:0]
	buf := bytes.NewBuffer(destinationBuffer)
	return compressWithBytesBuffer(record, buf)
}

func compressWithBytesBuffer(record []byte, buf *bytes.Buffer) ([]byte, error) {
	writer, err := gzip.NewWriterLevel(buf, gzip.DefaultCompression)
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

func (c *GzipCompressor) DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error) {
	// we have to set the length of the buffer (keeping capacity) to make sure gzip doesn't append
	destinationBuffer = destinationBuffer[:0]
	reader, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}

	resultBuffer := bytes.NewBuffer(destinationBuffer)
	_, err = resultBuffer.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return resultBuffer.Bytes(), nil
}
