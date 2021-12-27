package recordio

import (
	"os"
)

type BufferedIOFactory struct {
}

func (d BufferedIOFactory) CreateNewReader(filePath string, bufSize int) (*os.File, ByteReaderResetCount, error) {
	readFile, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return readFile, NewCountingByteReader(NewReaderBuf(readFile, block)), nil
}

func (d BufferedIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriteCloserFlusher, error) {
	writeFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return writeFile, NewWriterBuf(writeFile, block), nil
}
