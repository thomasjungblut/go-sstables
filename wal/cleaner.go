package wal

import "os"

type Cleaner struct {
	walOptions *Options
}

func (c *Cleaner) Clean() error {
	return os.RemoveAll(c.walOptions.basePath)
}

func NewCleaner(opts *Options) *Cleaner {
	return &Cleaner{walOptions: opts}
}
