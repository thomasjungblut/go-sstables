package wal

import (
	"errors"
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Replayer struct {
	walOptions *Options
}

func (r *Replayer) Replay(process func(record []byte) error) error {
	var walFiles []string
	err := filepath.Walk(r.walOptions.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), defaultWalSuffix) {
			walFiles = append(walFiles, path)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// do not rely on the order of the FS, we do an additional sort to make sure we start reading from 0000 to 9999
	sort.Strings(walFiles)

	var toClose []recordio.ReaderI
	defer func() {
		for _, reader := range toClose {
			_ = reader.Close()
		}
	}()

	for _, path := range walFiles {
		reader, err := r.walOptions.readerFactory(path)
		if err != nil {
			return err
		}
		toClose = append(toClose, reader)

		err = reader.Open()
		if err != nil {
			return err
		}

		for {
			bytes, err := reader.ReadNext()
			// io.EOF signals that no records are left to be read
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return err
			}

			err = process(bytes)
			if err != nil {
				return err
			}
		}

		err = reader.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func NewReplayer(walOpts *Options) (WriteAheadLogReplayI, error) {
	stat, err := os.Stat(walOpts.basePath)
	if err != nil {
		return nil, err
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf("given base path %s is not a directory", walOpts.basePath)
	}

	return &Replayer{
		walOptions: walOpts,
	}, nil
}
