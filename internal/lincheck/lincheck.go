// Package lincheck records a concurrent history of client operations and checks it for
// linearizability with Porcupine, one key at a time.
package lincheck

import (
	"errors"
	"fmt"
	"math"

	"github.com/anishathalye/porcupine"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// Input is the call.
type Input struct {
	Op    kv.Op
	Key   string
	Value string
}

// Output is the reply; Unknown marks a write whose outcome was never learned.
type Output struct {
	Value   string
	Found   bool
	Unknown bool
}

type state struct {
	value string
	found bool
}

// Model is a single-key register with put, get and delete, partitioned by key.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return state{} },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s, i, o := st.(state), in.(Input), out.(Output)
		switch i.Op {
		case kv.OpPut:
			return true, state{value: i.Value, found: true}
		case kv.OpDelete:
			return true, state{}
		default:
			return o.Found == s.found && (!o.Found || o.Value == s.value), s
		}
	},
	DescribeOperation: func(in, out interface{}) string {
		i, o := in.(Input), out.(Output)
		if i.Op == kv.OpGet {
			return fmt.Sprintf("get(%s) -> %q/%v", i.Key, o.Value, o.Found)
		}
		return fmt.Sprintf("%s(%s, %q)", i.Op, i.Key, i.Value)
	},
}

// History collects operations with a logical clock that orders every call and return.
type History struct {
	clock   int64
	ops     []porcupine.Operation
	open    map[int]int // operation index -> client, for unknown outcomes
	Unknown int
	Failed  int
}

// NewHistory starts an empty history.
func NewHistory() *History { return &History{open: map[int]int{}} }

// Call records an invocation and returns a token for Return.
func (h *History) Call(client int, in Input) int {
	h.clock++
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: h.clock})
	h.open[len(h.ops)-1] = client
	return len(h.ops) - 1
}

// Return records the reply to call token. Requests refused before reaching a log had no effect
// and are dropped; writes with an unknown outcome stay open until the end of the history; reads
// with an unknown outcome are dropped (they changed nothing).
func (h *History) Return(token int, r kv.Result) {
	h.clock++
	delete(h.open, token)
	op := &h.ops[token]
	switch {
	case r.Err == nil:
		op.Output = Output{Value: r.Value, Found: r.Found}
		op.Return = h.clock
	case errors.Is(r.Err, kv.ErrUnknown) && op.Input.(Input).Op != kv.OpGet:
		h.Unknown++
		op.Output = Output{Unknown: true}
		op.Return = math.MaxInt64
	default:
		h.Failed++
		op.Input = nil // removed in Operations
	}
}

// Operations returns the history for Porcupine; calls never answered count as unknown writes.
func (h *History) Operations() []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(h.ops))
	for i, op := range h.ops {
		if op.Input == nil {
			continue
		}
		if _, open := h.open[i]; open {
			if op.Input.(Input).Op == kv.OpGet {
				continue
			}
			op.Output = Output{Unknown: true}
			op.Return = math.MaxInt64
		}
		out = append(out, op)
	}
	return out
}

// Completed counts operations with a definite reply.
func (h *History) Completed() int {
	n := 0
	for _, op := range h.ops {
		if op.Input != nil && op.Return != 0 && op.Return != math.MaxInt64 {
			n++
		}
	}
	return n
}

// Check reports whether the history is linearizable.
func (h *History) Check() bool {
	return porcupine.CheckOperations(Model, h.Operations())
}
