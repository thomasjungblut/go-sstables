package recordio

import (
	"github.com/ncw/directio"
	"io/ioutil"
	"os"
)

type DirectIOFactory struct {
}

func (d DirectIOFactory) CreateNewReader(filePath string, bufSize int) (*os.File, ByteReaderResetCount, error) {
	readFile, err := directio.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := directio.AlignedBlock(bufSize)
	return readFile, NewCountingByteReader(NewReaderBuf(readFile, block)), nil
}

func (d DirectIOFactory) CreateNewWriter(filePath string, bufSize int) (*os.File, WriteCloserFlusher, error) {
	writeFile, err := directio.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	block := directio.AlignedBlock(bufSize)
	return writeFile, NewAlignedWriterBuf(writeFile, block), nil
}

// IsDirectIOAvailable tests whether DirectIO is available (on the OS / filesystem).
// It will return (true, nil) if that's the case, if it's not available it will be (false, nil).
// Any other error will be indicated by the error (either true/false).
func IsDirectIOAvailable() (available bool, err error) {
	// the only way to check is to create a tmp file and check whether the error is EINVAL, which indicates it's not available.
	tmpFile, err := ioutil.TempFile("", "directio-test")
	if err != nil {
		return
	}

	err = tmpFile.Close()
	if err != nil {
		return
	}

	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())

	tmpFile, err = directio.OpenFile(tmpFile.Name(), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		// this syscall specifically signals that DirectIO is not supported
		// if errors.Is(err, syscall.EINVAL)
		return
	}

	// at this point we can be sure a file can be opened with DirectIO flags correctly
	available = true

	err = tmpFile.Close()
	if err != nil {
		return
	}

	return
}
