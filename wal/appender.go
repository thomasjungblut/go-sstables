package wal

import (
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio"
	"path"
)

// this is an implicitly hardcoded limit of one mio. WAL files, I hope that nobody needs more than that.
const defaultWalSuffix = ".wal"
const defaultWalFilePattern = "%06d" + defaultWalSuffix

type Appender struct {
	nextWriterNumber   uint
	walFileNamePattern string
	currentWriter      recordio.WriterI
	walOptions         *Options
}

func (a *Appender) Append(record []byte) error {
	err := checkSizeAndRotate(a, len(record))
	if err != nil {
		return err
	}
	_, err = a.currentWriter.Write(record)

	return err
}

func (a *Appender) AppendSync(record []byte) error {
	err := checkSizeAndRotate(a, len(record))
	if err != nil {
		return err
	}
	_, err = a.currentWriter.WriteSync(record)

	return err
}

func (a *Appender) Close() error {
	return a.currentWriter.Close()
}

func checkSizeAndRotate(a *Appender, nextRecordSize int) error {
	if (a.currentWriter.Size() + uint64(nextRecordSize)) > a.walOptions.maxWalFileSize {
		err := a.currentWriter.Close()
		if err != nil {
			return err
		}

		err = setupNextWriter(a)
		if err != nil {
			return err
		}
	}

	return nil
}

func setupNextWriter(a *Appender) error {
	if a.nextWriterNumber >= 1000000 {
		return fmt.Errorf("not supporting more than one million wal files at the minute. "+
			"Current limit exceeded: %d", a.nextWriterNumber)
	}

	writerPath := path.Join(a.walOptions.basePath, fmt.Sprintf(defaultWalFilePattern, a.nextWriterNumber))
	currentWriter, err := a.walOptions.writerFactory(writerPath)
	if err != nil {
		return err
	}

	err = currentWriter.Open()
	if err != nil {
		return err
	}

	a.nextWriterNumber++
	a.currentWriter = currentWriter

	return nil
}

func NewAppender(walOpts *Options) (*Appender, error) {
	appender := &Appender{
		walOptions:         walOpts,
		nextWriterNumber:   0,
		walFileNamePattern: defaultWalFilePattern,
		currentWriter:      nil,
	}

	err := setupNextWriter(appender)
	if err != nil {
		return nil, err
	}
	return appender, nil
}
