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

func (d BufferedIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriteSeekerCloserFlusher, error) {
	writeFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return writeFile, NewWriterBuf(writeFile, block), nil
}

// dontCacheIOFactory is like BufferedIOFactory, but the written data doesn't stay in the page cache, see DontCache.
type dontCacheIOFactory struct {
	BufferedIOFactory
}

func (d dontCacheIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriteSeekerCloserFlusher, error) {
	writeFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := make([]byte, bufSize)
	return writeFile, NewWriterBuf(newDontCacheFile(writeFile), block), nil
}
