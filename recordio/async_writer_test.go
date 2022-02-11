package recordio

import (
	"fmt"
	"github.com/godzie44/go-uring/uring"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestAsyncWriter_HappyPath(t *testing.T) {
	temp, err := ioutil.TempFile("", "TestAsyncWriter_HappyPath")
	require.NoError(t, err)
	require.NoError(t, temp.Close())
	defer closeCleanFile(t, temp)

	writer, err := NewAsyncWriter(temp.Name(), 4)
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		_, err = writer.Write(randomRecordOfSize(1024))
		require.NoError(t, err)
	}

	require.NoError(t, writer.Close())
}

func TestExample(t *testing.T) {
	ring, err := uring.New(8)
	require.NoError(t, err)
	defer ring.Close()

	// open file and init read buffers
	file, err := os.Open("./go.mod")
	require.NoError(t, err)
	stat, _ := file.Stat()
	buff := make([]byte, stat.Size())

	// add Read operation to SQ queue
	err = ring.QueueSQE(uring.Read(file.Fd(), buff, 0), 0, 0)
	require.NoError(t, err)

	// submit all SQ new entries
	_, err = ring.Submit()
	require.NoError(t, err)

	// wait until data is reading into buffer
	cqe, err := ring.WaitCQEvents(1)
	require.NoError(t, err)

	require.NoError(t, cqe.Error()) //check read error

	fmt.Printf("read %d bytes, read result: \n%s", cqe.Res, string(buff))

	// dequeue CQ
	ring.SeenCQE(cqe)

}
