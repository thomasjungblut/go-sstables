package porcupine

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	pp "github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/simpledb"
)

const (
	GetOp = iota
	PutOp = iota
	DelOp = iota
)

type MapState struct {
	m map[string]string
}

type Input struct {
	Operation uint8
	Key       string
	Val       string
}

type Output struct {
	Key string
	Val string
	Err error
}

func (s MapState) Clone() MapState {
	sx := make(map[string]string, len(s.m))
	for k, v := range s.m {
		sx[k] = v
	}
	return MapState{m: sx}
}

func (s MapState) Equals(otherState MapState) bool {
	return reflect.DeepEqual(s.m, otherState.m)
}

func shorten(s string, size int) string {
	if len(s) > size {
		return s[:size]
	}
	return s
}

func (s MapState) String() string {
	shortValueState := map[string]string{}
	for k, v := range s.m {
		shortValueState[k] = shorten(v, 5)
	}

	return fmt.Sprintf("%v", shortValueState)
}

func NewMapState() MapState {
	return MapState{m: map[string]string{}}
}

var Model = pp.Model[MapState, Input, Output]{
	Init: NewMapState,
	Partition: func(history []pp.Operation[Input, Output]) [][]pp.Operation[Input, Output] {
		indexMap := map[string]int{}
		var partitions [][]pp.Operation[Input, Output]
		for _, op := range history {
			i := op.Input
			ix, found := indexMap[i.Key]
			if !found {
				partitions = append(partitions, []pp.Operation[Input, Output]{op})
				indexMap[i.Key] = len(partitions) - 1
			} else {
				partitions[ix] = append(partitions[ix], op)
			}
		}
		return partitions
	},
	Step: func(s MapState, i Input, o Output) (bool, MapState) {
		stateVal, found := s.m[i.Key]

		switch i.Operation {
		case GetOp:
			if o.Err == simpledb.ErrNotFound {
				return !found, s
			} else if stateVal == o.Val {
				return true, s
			}
			break
		case PutOp:
			if o.Err == nil {
				s.m[i.Key] = i.Val
				return true, s
			}
			break
		case DelOp:
			if o.Err == nil {
				delete(s.m, i.Key)
				return true, s
			}
			break
		}

		if o.Err != nil {
			log.Printf("unexpected error state found for key: [%s] %v\n", i.Key, o.Err)
			panic(o.Err)
		}

		return false, s
	},
	DescribeOperation: func(i Input, o Output) string {
		opName := ""
		switch i.Operation {
		case GetOp:
			opName = "Get"
			break
		case PutOp:
			opName = "Put"
			break
		case DelOp:
			opName = "Del"
			break
		}

		return fmt.Sprintf("%s(%s) -> %s", opName, i.Key, shorten(o.Val, 5))
	},
}

func VerifyOperations(t *testing.T, operations []pp.Operation[Input, Output]) {
	result, info := pp.CheckOperationsVerbose(Model, operations, 0)
	require.NoError(t, pp.VisualizePath(Model, info, t.Name()+"_porcupine.html"))
	require.Equal(t, pp.Ok, result, "output was not linearizable")
}
