package recordio

import (
	"errors"
	"io/ioutil"
	"os"
	"syscall"

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
	return writeFile, NewAlignedWriterBuf(writeFile, block), nil
}

// IsDirectIOAvailable tests whether DirectIO is available (on the OS / filesystem).
// It will return (true, nil) if that's the case, if it's not available it will be (false, nil).
// Any other return error indicates any other io problem.
func IsDirectIOAvailable() (bool, error) {
	// the only way to check is to create a tmp file and check whether the error is EINVAL, which indicates it's not available.
	tmpFile, err := ioutil.TempFile("", "directio-test")
	if err != nil {
		return false, err
	}

	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())

	_, err = directio.OpenFile(tmpFile.Name(), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		if errors.Is(err, syscall.EINVAL) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
