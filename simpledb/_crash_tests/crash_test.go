//go:build simpleDBcrash
// +build simpleDBcrash

package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/simpledb/porcupine"

	"github.com/thomasjungblut/go-sstables/simpledb"
)

func TestOutOfProcessCrashesSimpleAscendingWorkload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpledb-OutOfProcessCrashes")
	require.NoError(t, err)

	defer func(t *testing.T, p string) { require.NoError(t, os.RemoveAll(p)) }(t, tmpDir)
	cmd := spawnNewDatabaseServer(t, tmpDir)

	c := newRequestClient(0)
	var expectedKeys []string
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("%06d", i)
		err := c.Put(key, key)
		require.NoError(t, err)
		expectedKeys = append(expectedKeys, key)

		// every 100 inserts we're crashing + recovering, ensuring all inserts up until and including this key have been inserted
		if i%100 == 0 {
			cmd = killAndRespawnDatabaseServer(t, cmd, tmpDir)
			assertContains(t, c, expectedKeys)
		}
	}

	assertContains(t, c, expectedKeys)

	// clean up the last remaining database
	killAndWait(t, cmd)
	porcupine.VerifyOperations(t, c.Operations())
}

func TestOutOfProcessCrashesDuringCompactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpledb-OutOfProcessCrashesCompactions")
	require.NoError(t, err)

	defer func(t *testing.T, p string) { require.NoError(t, os.RemoveAll(p)) }(t, tmpDir)
	cmd := spawnNewDatabaseServer(t, tmpDir)

	c := newRequestClient(0)
	var expectedKeys []string
	for i := 0; i < 15000; i++ {
		key := fmt.Sprintf("%06d", i)
		err := c.Put(key, key)
		require.NoError(t, err)
		expectedKeys = append(expectedKeys, key)

		// we create 10 small files in quick succession which should trigger compactions
		// after 1000 inserts we give it increasingly more time to test compaction in different stages
		if i < 1000 && i%100 == 0 {
			cmd = killAndRespawnDatabaseServer(t, cmd, tmpDir)
			assertContains(t, c, expectedKeys)
		} else if i > 1000 && i%1000 == 0 {
			sleepTime := time.Duration(i%1000) * time.Second
			log.Printf("sleeping for %v...\n", sleepTime)
			time.Sleep(sleepTime)
			cmd = killAndRespawnDatabaseServer(t, cmd, tmpDir)
			assertContains(t, c, expectedKeys)
		}
	}

	assertContains(t, c, expectedKeys)

	// clean up the last remaining database
	killAndWait(t, cmd)
	porcupine.VerifyOperations(t, c.Operations())
}

func TestOutOfProcessCrashesSimpleAscendingWorkloadWithDeletions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpledb-OutOfProcessCrashesDeletions")
	require.NoError(t, err)

	defer func(t *testing.T, p string) { require.NoError(t, os.RemoveAll(p)) }(t, tmpDir)
	cmd := spawnNewDatabaseServer(t, tmpDir)

	c := newRequestClient(0)
	var expectedKeys []string
	var unexpectedKeys []string
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("%06d", i)
		err := c.Put(key, key)
		require.NoError(t, err)

		if i%2 == 0 {
			unexpectedKeys = append(unexpectedKeys, key)
			err := c.Delete(key)
			require.NoError(t, err)
		} else {
			expectedKeys = append(expectedKeys, key)
		}

		if i%100 == 0 {
			cmd = killAndRespawnDatabaseServer(t, cmd, tmpDir)
			assertContains(t, c, expectedKeys)
			assertNotContains(t, c, unexpectedKeys)
		}
	}

	assertContains(t, c, expectedKeys)
	assertNotContains(t, c, unexpectedKeys)

	// clean up the last remaining database
	killAndWait(t, cmd)
	porcupine.VerifyOperations(t, c.Operations())
}

func TestOutOfProcessCrashesRandomKeysWithDeletion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpledb-OutOfProcessCrashesDeletionsRandomData")
	require.NoError(t, err)

	defer func(t *testing.T, p string) { require.NoError(t, os.RemoveAll(p)) }(t, tmpDir)
	cmd := spawnNewDatabaseServer(t, tmpDir)

	c := newRequestClient(0)
	rnd := rand.New(rand.NewSource(1337))
	var expectedKeys []string
	var unexpectedKeys []string
	for i := 0; i < 2500; i++ {
		key := randomAsciiString(rnd, 32)
		err := c.Put(key, key)
		require.NoError(t, err)

		expectedKeys = append(expectedKeys, key)
		if rnd.Float32() < 0.3 {
			toDelete := rnd.Intn(len(expectedKeys))
			err := c.Delete(expectedKeys[toDelete])
			require.NoError(t, err)
			unexpectedKeys = append(unexpectedKeys, expectedKeys[toDelete])
			expectedKeys = append(expectedKeys[:toDelete], expectedKeys[toDelete+1:]...)
		}

		// crash in 5% of the inserts
		if rnd.Float32() < 0.05 {
			cmd = killAndRespawnDatabaseServer(t, cmd, tmpDir)
			assertContains(t, c, expectedKeys)
			assertNotContains(t, c, unexpectedKeys)
		}
	}

	assertContains(t, c, expectedKeys)
	assertNotContains(t, c, unexpectedKeys)

	// clean up the last remaining database
	killAndWait(t, cmd)
	porcupine.VerifyOperations(t, c.Operations())
}

func assertContains(t *testing.T, c *porcupine.DatabaseClientRecorder, keys []string) {
	for _, k := range keys {
		s, err := c.Get(k)
		require.NoError(t, err)
		assert.Equal(t, k, s)
	}
	log.Printf("successfully asserted %d keys exist\n", len(keys))
}

func assertNotContains(t *testing.T, c *porcupine.DatabaseClientRecorder, keys []string) {
	for _, k := range keys {
		v, err := c.Get(k)
		assert.Equal(t, simpledb.ErrNotFound, err, "expected '%s' to be deleted, but was '%s'", k, v)
	}
	log.Printf("successfully asserted %d keys don't exist\n", len(keys))
}

func killAndRespawnDatabaseServer(t *testing.T, c *exec.Cmd, dir string) *exec.Cmd {
	log.Printf("killing database...\n")
	killAndWait(t, c)
	return spawnNewDatabaseServer(t, dir)
}

func killAndWait(t *testing.T, c *exec.Cmd) {
	pid := c.Process.Pid
	// this will kill the go build process (parent of the web server)
	err := c.Process.Kill()
	require.NoError(t, err)
	_, err = c.Process.Wait()
	require.NoError(t, err)

	// this should kill the web server (note we pass the negative pid as an indicator for the gpid)
	err = syscall.Kill(-pid, syscall.SIGKILL)
	require.NoError(t, err)

	// wait until the DB is not responding anymore
	for {
		_, err := newRequestClient(0).Get("SOME_KEY")
		if err != nil && err != simpledb.ErrNotFound {
			break
		}

		log.Printf("waiting for db to wind down properly... last err: %v\n", err)
		time.Sleep(1 * time.Second)
	}
}

func spawnNewDatabaseServer(t *testing.T, path string) *exec.Cmd {
	err := os.MkdirAll(path, 700)
	require.NoError(t, err)

	command := exec.Command("go", "run", "simpledb_web_server.go", path)
	command.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
		// setting the pgid is important as `go run` will actually build + run, so killing will only kill the build process
		// this allows us to kill all children via a syscall and the process group id
		Setpgid: true,
	}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr

	err = command.Start()
	require.NoError(t, err)

	// wait until the DB is up
	for {
		_, err := newRequestClient(0).Get("SOME_KEY")
		if err != nil && err == simpledb.ErrNotFound {
			break
		}

		log.Printf("waiting for db to become available... last err: %v\n", err)
		time.Sleep(1 * time.Second)
	}

	return command
}

type requestClient struct {
}

func (c *requestClient) Close() error {
	return nil
}

func (c *requestClient) Open() error {
	return nil
}

func (c *requestClient) Get(key string) (string, error) {
	response, err := http.Get(formatUrl(key))
	if err != nil {
		return "", err
	}

	if response.StatusCode == http.StatusNotFound {
		return "", simpledb.ErrNotFound
	}

	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected response code: %d for GET key '%s'", response.StatusCode, key)
	}

	all, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(all), nil
}

func (c *requestClient) Put(key, val string) error {
	request, err := http.NewRequest(http.MethodPut, formatUrl(key), strings.NewReader(val))
	if err != nil {
		return err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code: %d for PUT key '%s'", response.StatusCode, key)
	}
	return nil
}

func (c *requestClient) Delete(key string) error {
	request, err := http.NewRequest(http.MethodDelete, formatUrl(key), strings.NewReader(""))
	if err != nil {
		return err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code: %d for DELETE key '%s'", response.StatusCode, key)
	}
	return nil
}

func newRequestClient(id int) *porcupine.DatabaseClientRecorder {
	db := &requestClient{}
	return porcupine.NewDatabaseRecorder(db, id)
}

func formatUrl(key string) string {
	return fmt.Sprintf("http://%s?key=%s", fullDataEndpoint, key)
}

func randomAsciiString(rand *rand.Rand, size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		builder.WriteRune(65 + rand.Int31n(25))
	}

	return builder.String()
}
