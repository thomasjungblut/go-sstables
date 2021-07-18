package wal

import "os"

type Cleaner struct {
	walOptions *Options
}

func (c *Cleaner) Clean() error {
	return os.RemoveAll(c.walOptions.basePath)
}

func NewCleaner(opts *Options) WriteAheadLogCleanI {
	return &Cleaner{walOptions: opts}
}
