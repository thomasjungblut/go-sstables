package recordio

import (
	"os"
)

type PlainIOFactory struct {
}

func (d PlainIOFactory) CreateNewReader(filePath string, bufSize int) (*os.File, CountingReaderResetComposite, error) {
	readFile, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return readFile, NewCountingByteReader(NewReaderBuf(readFile, block)), nil
}

func (d PlainIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriterCloserFlusher, error) {
	writeFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return writeFile, NewWriterBuf(writeFile, block), nil
}
