package porcupine

import (
	"fmt"
	pp "github.com/anishathalye/porcupine"
	"github.com/thomasjungblut/go-sstables/simpledb"
	"log"
	"reflect"
)

const (
	GetOp = iota
	PutOp = iota
	DelOp = iota
)

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

type State struct {
	state map[string]string
}

func shorten(s string, size int) string {
	if len(s) > size {
		return s[:size]
	}
	return s
}

var Model = pp.Model{
	Init: func() interface{} {
		return State{
			state: map[string]string{},
		}
	},
	Partition: func(history []pp.Operation) [][]pp.Operation {
		indexMap := map[string]int{}
		var partitions [][]pp.Operation
		for _, op := range history {
			i := op.Input.(Input)
			ix, found := indexMap[i.Key]
			if !found {
				partitions = append(partitions, []pp.Operation{op})
				indexMap[i.Key] = len(partitions) - 1
			} else {
				partitions[ix] = append(partitions[ix], op)
			}
		}
		return partitions
	},
	Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
		s := state.(State)
		i := input.(Input)
		o := output.(Output)

		stateVal, found := s.state[i.Key]

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
				s.state[i.Key] = i.Val
				return true, s
			}
			break
		case DelOp:
			if o.Err == nil {
				delete(s.state, i.Key)
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
	Equal: func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	},
	DescribeOperation: func(input interface{}, output interface{}) string {
		i := input.(Input)
		o := output.(Output)

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
	DescribeState: func(state interface{}) string {
		s := state.(State)
		shortValueState := map[string]string{}
		for k, v := range s.state {
			shortValueState[k] = shorten(v, 5)
		}

		return fmt.Sprintf("%v", shortValueState)
	},
}
