package wal

import (
	"fmt"
	"os"
)

type Cleaner struct {
	walOptions *Options
}

func (c *Cleaner) Clean() error {
	err := os.RemoveAll(c.walOptions.basePath)
	if err != nil {
		return fmt.Errorf("error while cleaning wal folders  under '%s': %w", c.walOptions.basePath, err)
	}
	return nil
}

func NewCleaner(opts *Options) WriteAheadLogCleanI {
	return &Cleaner{walOptions: opts}
}
