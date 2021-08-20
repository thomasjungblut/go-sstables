package wal

import (
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio"
	"path/filepath"
)

// this is an implicitly hardcoded limit of one mio. WAL files, I hope that nobody needs more than that.
const defaultWalSuffix = ".wal"
const defaultWalFilePattern = "%06d" + defaultWalSuffix

type Appender struct {
	nextWriterNumber   uint
	walFileNamePattern string
	currentWriter      recordio.WriterI
	currentWriterPath  string
	walOptions         *Options
}

func (a *Appender) Append(record []byte) error {
	err := checkSizeAndRotate(a, len(record))
	if err != nil {
		return fmt.Errorf("error while rotating wal writer '%s': %w", a.currentWriterPath, err)
	}
	_, err = a.currentWriter.Write(record)
	if err != nil {
		return fmt.Errorf("error while appending to wal writer '%s': %w", a.currentWriterPath, err)
	}

	return nil
}

func (a *Appender) AppendSync(record []byte) error {
	err := checkSizeAndRotate(a, len(record))
	if err != nil {
		return fmt.Errorf("error while rotating sync wal writer '%s': %w", a.currentWriterPath, err)
	}
	_, err = a.currentWriter.WriteSync(record)
	if err != nil {
		return fmt.Errorf("error while appending to sync wal writer '%s': %w", a.currentWriterPath, err)
	}

	return nil
}

func (a *Appender) Rotate() (string, error) {
	currentPath := a.currentWriterPath
	err := a.currentWriter.Close()
	if err != nil {
		return "", fmt.Errorf("error while closing current rotation writer '%s': %w", a.currentWriterPath, err)
	}

	err = setupNextWriter(a)
	if err != nil {
		return "", fmt.Errorf("error while setting up new rotation writer '%s': %w", a.currentWriterPath, err)
	}

	return currentPath, nil
}

func (a *Appender) Close() error {
	err := a.currentWriter.Close()
	if err != nil {
		return fmt.Errorf("error while closing appender and current rotation writer '%s': %w", a.currentWriterPath, err)
	}

	return nil
}

func checkSizeAndRotate(a *Appender, nextRecordSize int) error {
	if (a.currentWriter.Size() + uint64(nextRecordSize)) > a.walOptions.maxWalFileSize {
		_, err := a.Rotate()
		if err != nil {
			return fmt.Errorf("error rotating appender at '%s': %w", a.currentWriterPath, err)
		}
	}

	return nil
}

func setupNextWriter(a *Appender) error {
	if a.nextWriterNumber >= 1000000 {
		return fmt.Errorf("not supporting more than one million wal files at the minute. "+
			"Current limit exceeded: %d", a.nextWriterNumber)
	}

	writerPath := filepath.Join(a.walOptions.basePath, fmt.Sprintf(defaultWalFilePattern, a.nextWriterNumber))
	currentWriter, err := a.walOptions.writerFactory(writerPath)
	if err != nil {
		return fmt.Errorf("error while creating new wal appender writer under '%s': %w", writerPath, err)
	}

	err = currentWriter.Open()
	if err != nil {
		return fmt.Errorf("error while opening new wal appender writer under '%s': %w", writerPath, err)
	}

	a.nextWriterNumber++
	a.currentWriter = currentWriter
	a.currentWriterPath = writerPath

	return nil
}

func NewAppender(walOpts *Options) (WriteAheadLogAppendI, error) {
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
