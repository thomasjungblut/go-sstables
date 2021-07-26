package simpledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestFlushPathsSortCorrectly(t *testing.T) {
	var tables []string
	for i := 0; i < 10001; i++ {
		tables = append(tables, fmt.Sprintf(SSTablePattern, i))
	}

	sort.Strings(tables)
	for i := 0; i < 10001; i++ {
		is := strings.Split(tables[i], "_")
		atoi, err := strconv.Atoi(is[1])
		assert.Nil(t, err)
		assert.Equal(t, i, atoi)
	}
}
