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

var Model = pp.Model{
	Init: func() interface{} {
		return State{
			state: map[string]string{},
		}
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

		return fmt.Sprintf("%s(%s) -> %s", opName, i.Val, o.Val)
	},
	DescribeState: func(state interface{}) string {
		s := state.(State)
		return fmt.Sprintf("%v", s.state)
	},
}
