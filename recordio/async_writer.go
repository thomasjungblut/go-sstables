//go:build linux

package recordio

import (
	"github.com/godzie44/go-uring/uring"
	"os"
)

// AsyncWriter takes an uring and executes all writes asynchronously. There are only two barriers: flush and close.
// Those barriers will ensure all previous writes have succeeded.
type AsyncWriter struct {
	ringSize      int32
	submittedSQEs int32
	ring          *uring.Ring

	file   *os.File
	offset uint64
}

// TODO(thomas): not thread-safe (yet)
func (w *AsyncWriter) Write(p []byte) (int, error) {
	for w.submittedSQEs >= w.ringSize {
		err := w.submitAwaitOne()
		if err != nil {
			return 0, err
		}
	}

	// TODO(thomas): we would need to make a defensive copy for p, which actually is not optimal
	// the reason is the buffer pooling (or the header reuse). It so happens that the original backing array was written
	// a couple times before the ring was submitted. That caused some funny offsets to be written and eventually fail reading.
	pc := make([]byte, len(p))
	copy(pc, p)

	err := w.ring.QueueSQE(uring.Write(w.file.Fd(), pc, w.offset), 0, 0)
	if err != nil {
		return 0, err
	}

	w.submittedSQEs++
	w.offset += uint64(len(p))

	return len(p), nil
}

func (w *AsyncWriter) Flush() error {
	for w.submittedSQEs > 0 {
		// wait for at least one event to free from the queue
		err := w.submitAwaitOne()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *AsyncWriter) submitAwaitOne() error {
	// TODO(thomas): most likely there are more CQ events waiting, we should try to drain them optimistically to avoid overflowing memory buffers
	cqe, err := w.ring.SubmitAndWaitCQEvents(1)
	if err != nil {
		return err
	}

	w.submittedSQEs--
	w.ring.SeenCQE(cqe)

	err = cqe.Error()
	if err != nil {
		return err
	}

	return nil
}

func (w *AsyncWriter) Size() int {
	return 0
}

func (w *AsyncWriter) Close() error {
	err := w.Flush()
	if err != nil {
		return err
	}

	err = w.ring.UnRegisterFiles()
	if err != nil {
		return err
	}

	err = w.ring.Close()
	if err != nil {
		return err
	}

	return w.file.Close()
}

func NewAsyncWriter(filePath string, numRingEntries uint32, opts ...uring.SetupOption) (WriteCloserFlusher, *os.File, error) {
	ring, err := uring.New(numRingEntries, opts...)
	if err != nil {
		return nil, nil, err
	}

	writeFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}

	err = ring.RegisterFiles([]int{int(writeFile.Fd())})
	if err != nil {
		return nil, nil, err
	}

	writer := &AsyncWriter{
		ringSize: int32(numRingEntries),
		file:     writeFile,
		ring:     ring,
	}

	return writer, writeFile, nil
}
