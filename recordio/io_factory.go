package recordio

import (
	"os"
)

type IOFactory interface {
	CreateNewReader(filePath string, bufSize int) (*os.File, ByteReaderResetCount, error)
	CreateNewWriter(filePath string, bufSize int) (*os.File, WriteCloserFlusher, error)
}
