package compressor

import (
	"bytes"
	"compress/lzw"
)

type LzwCompressor struct {
}

func (l LzwCompressor) Compress(record []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lzw.NewWriter(&buf, lzw.LSB, 8)
	_, err := writer.Write(record)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l LzwCompressor) Decompress(buf []byte) ([]byte, error) {
	reader := lzw.NewReader(bytes.NewBuffer(buf), lzw.LSB, 8)
	var resultBuffer bytes.Buffer
	_, err := resultBuffer.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return resultBuffer.Bytes(), nil
}

func (l LzwCompressor) CompressWithBuf(record []byte, destinationBuffer []byte) ([]byte, error) {
	// we have to set the length of the buffer (keeping capacity) to make sure gzip doesn't append
	destinationBuffer = destinationBuffer[:0]
	buf := bytes.NewBuffer(destinationBuffer)
	writer := lzw.NewWriter(buf, lzw.LSB, 8)
	_, err := writer.Write(record)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l LzwCompressor) DecompressWithBuf(buf []byte, destinationBuffer []byte) ([]byte, error) {
	// we have to set the length of the buffer (keeping capacity) to make sure gzip doesn't append
	destinationBuffer = destinationBuffer[:0]
	reader := lzw.NewReader(bytes.NewBuffer(buf), lzw.LSB, 8)
	resultBuffer := bytes.NewBuffer(destinationBuffer)
	_, err := resultBuffer.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return resultBuffer.Bytes(), nil
}
