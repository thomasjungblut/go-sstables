package benchmark

import (
	"github.com/stretchr/testify/assert"
	"github.com/thomasjungblut/go-sstables/simpledb"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

// write

func BenchmarkSimpleDBWriteLatency100(b *testing.B) {
	benchmarkWriteLatency(100, b)
}

func BenchmarkSimpleDBWriteLatency1k(b *testing.B) {
	benchmarkWriteLatency(1000, b)
}

func BenchmarkSimpleDBWriteLatency10k(b *testing.B) {
	benchmarkWriteLatency(10000, b)
}

func BenchmarkSimpleDBWriteLatency100k(b *testing.B) {
	benchmarkWriteLatency(100000, b)
}

// read

func BenchmarkSimpleDBReadLatency100(b *testing.B) {
	benchmarkReadLatency(100, b)
}

func BenchmarkSimpleDBReadLatency1k(b *testing.B) {
	benchmarkReadLatency(1000, b)
}

func BenchmarkSimpleDBReadLatency10k(b *testing.B) {
	benchmarkReadLatency(10000, b)
}

func benchmarkReadLatency(dbSize int, b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "simpledb_Bench")
	assert.Nil(b, err)
	defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()
	db, err := simpledb.NewSimpleDB(tmpDir)
	assert.Nil(b, err)
	defer func() { assert.Nil(b, db.Close()) }()
	assert.Nil(b, db.Open())

	// we save the keys as string to not measure the itoa overhead
	keys := make([]string, 0)
	for i := 0; i < dbSize; i++ {
		keys = append(keys, strconv.Itoa(i))
		assert.Nil(b, db.Put(keys[i], randomRecordWithPrefix(i)))
	}

	b.ResetTimer()
	i := 0
	for n := 0; n < b.N; n++ {
		key := keys[i]
		val, err := db.Get(key)
		assert.Nil(b, err)
		assert.Truef(b, strings.HasPrefix(val, key),
			"expected key %s as prefix, but was %s", key, val[:len(key)])
		i++
		if i >= len(keys) {
			i = 0
		}
	}
}

func benchmarkWriteLatency(dbSize int, b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "simpledb_Bench")
	assert.Nil(b, err)
	defer func() { assert.Nil(b, os.RemoveAll(tmpDir)) }()
	db, err := simpledb.NewSimpleDB(tmpDir)
	assert.Nil(b, err)
	defer func() { assert.Nil(b, db.Close()) }()
	assert.Nil(b, db.Open())

	// we save the keys as string to not measure the itoa overhead
	keys := make([]string, 0)
	values := make([]string, 0)
	for i := 0; i < dbSize; i++ {
		keys = append(keys, strconv.Itoa(i))
		values = append(values, randomRecordWithPrefix(i))
	}

	b.ResetTimer()
	i := 0
	for n := 0; n < b.N; n++ {
		err := db.Put(keys[i], values[i])
		assert.Nil(b, err)
		i++
		if i >= len(keys) {
			i = 0
		}
	}
}

func randomRecordWithPrefix(prefix int) string {
	builder := strings.Builder{}
	builder.WriteString(strconv.Itoa(prefix))
	builder.WriteString("_")
	for i := 0; i < 10000; i++ {
		builder.WriteRune(rand.Int31n(255))
	}

	return builder.String()
}
