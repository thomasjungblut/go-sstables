package hamming

import (
	"io"
	"log"
	"os"
)

var (
	devNull = func() io.Writer {
		f, err := os.OpenFile("/dev/null", os.O_WRONLY, 0400)
		if err != nil {
			panic(err)
		}
		return f
	}()
	nullLog = log.New(devNull, log.Prefix(), log.Flags())
)
