package hamming

import (
	"io"
	"log"
	"os"
)

func devNull() io.Writer {
	f, err := os.OpenFile("/dev/null", os.O_WRONLY, 0400)
	if err != nil {
		panic(err)
	}
	return f
}

func nullLog() *log.Logger {
	return log.New(devNull(), log.Prefix(), log.Flags())
}
