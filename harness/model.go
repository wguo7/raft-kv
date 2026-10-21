// Package harness is a Jepsen-style test harness: it runs a Raft KV cluster
// in one process over a simulated network, injects partitions, crashes and
// message loss from a random seed, records every client operation, and checks
// the history for linearizability and the cluster for Raft's safety rules.
package harness

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

type opInput struct {
	Op    string // "get", "put", "append"
	Key   string
	Value string
}

type opOutput struct {
	Value   string
	Unknown bool // the client gave up without learning whether it applied
}

// kvModel is the sequential spec: a map from key to string. Histories are
// split by key, since operations on different keys commute.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(opInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in, out, st := input.(opInput), output.(opOutput), state.(string)
		switch in.Op {
		case "get":
			return out.Unknown || out.Value == st, st
		case "put":
			return true, in.Value
		case "append":
			next := st + in.Value
			return out.Unknown || out.Value == next, next
		}
		return false, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in, out := input.(opInput), output.(opOutput)
		res := fmt.Sprintf("%q", out.Value)
		if out.Unknown {
			res = "?"
		}
		switch in.Op {
		case "get":
			return fmt.Sprintf("get(%s) -> %s", in.Key, res)
		default:
			return fmt.Sprintf("%s(%s, %q) -> %s", in.Op, in.Key, in.Value, res)
		}
	},
}
