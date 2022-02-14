//go:build linux

package iouring

import "github.com/godzie44/go-uring/uring"

// IsIOUringAvailable tests whether io_uring is supported by the kernel.
// It will return (true, nil) if that's the case, if it's not available it will be (false, nil).
// Any other error will be indicated by the error (either true/false).
func IsIOUringAvailable() (available bool, err error) {
	ring, err := uring.New(1)
	defer func() {
		err = ring.Close()
	}()

	return err == nil, err
}
