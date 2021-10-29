package recordio

import (
	"os"

	"github.com/ncw/directio"
)

type DirectIOFactory struct {
}

func (d DirectIOFactory) CreateNewReader(filePath string, bufSize int) (*os.File, CountingReaderResetComposite, error) {
	readFile, err := directio.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := directio.AlignedBlock(bufSize)
	return readFile, NewCountingByteReader(NewReaderBuf(readFile, block)), nil
}

func (d DirectIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriterCloserFlusher, error) {
	writeFile, err := directio.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := directio.AlignedBlock(bufSize)
	return writeFile, NewWriterBuf(writeFile, block), nil
}
