// Package kv holds the types shared by the cluster and its clients: regions, commands, results
// and the errors a replica can return.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// Size limits for one key and one value; larger requests are refused before they reach Raft.
const (
	MaxKeyBytes   = 4 << 10
	MaxValueBytes = 64 << 10
)

// Region is the key range [Start, End) replicated by one Raft group; End "" means no upper bound.
type Region struct {
	ID    uint64   `json:"id"`
	Start string   `json:"start"`
	End   string   `json:"end"`
	Epoch uint64   `json:"epoch"` // bumped by every split, so requests routed with an older view are refused
	Peers []uint64 `json:"peers"` // node IDs holding a replica
}

// Contains reports whether key falls in the region's range.
func (r Region) Contains(key string) bool {
	return key >= r.Start && (r.End == "" || key < r.End)
}

// Clone returns a copy that shares no slices with r.
func (r Region) Clone() Region {
	r.Peers = slices.Clone(r.Peers)
	return r
}

// String is a short description for logs and errors.
func (r Region) String() string {
	end := fmt.Sprintf("%q", r.End)
	if r.End == "" {
		end = "+inf"
	}
	return fmt.Sprintf("region %d [%q, %s) epoch %d", r.ID, r.Start, end, r.Epoch)
}

// Op is what a command does.
type Op string

// The commands a region's Raft log can hold.
const (
	OpPut    Op = "put"
	OpGet    Op = "get"
	OpDelete Op = "delete"
	OpSplit  Op = "split"
)

// Command is one entry of a region's Raft log. Every replica applies it in log order, so its
// effect, including a split, is the same everywhere.
type Command struct {
	ReqID       uint64 `json:"req"`
	Op          Op     `json:"op"`
	RegionID    uint64 `json:"region"`
	Epoch       uint64 `json:"epoch"` // the region epoch the sender routed with
	Key         string `json:"key,omitempty"`
	Value       string `json:"value,omitempty"`
	NewRegionID uint64 `json:"new_region,omitempty"` // for a split: the ID of the right-hand half
}

// Encode serializes the command for the Raft log.
func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // a struct of strings and integers always marshals
	}
	return b
}

// DecodeCommand parses a log entry; anything malformed is an error, never a panic.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	switch c.Op {
	case OpPut, OpGet, OpDelete, OpSplit:
		return c, nil
	}
	return Command{}, fmt.Errorf("decode command: unknown op %q", c.Op)
}

// Validate checks a client request before it is proposed.
func Validate(op Op, key, value string) error {
	if op != OpPut && op != OpGet && op != OpDelete {
		return fmt.Errorf("%w: clients may only put, get or delete", ErrInvalid)
	}
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return fmt.Errorf("%w: key must be 1 to %d bytes", ErrInvalid, MaxKeyBytes)
	}
	if len(value) > MaxValueBytes {
		return fmt.Errorf("%w: value larger than %d bytes", ErrInvalid, MaxValueBytes)
	}
	return nil
}

// Result is what applying a command returned.
type Result struct {
	Value string
	Found bool
	Err   error
}

// ErrInvalid marks a request the cluster will never accept (bad key, value or operation).
var ErrInvalid = errors.New("invalid request")

// ErrUnknown means the request was proposed but no reply came: it may or may not have been
// applied, so it must not be blindly retried.
var ErrUnknown = errors.New("no reply in time: the request may or may not have been applied")

// ErrUnavailable means every attempt was refused before reaching a Raft log, so nothing was applied.
var ErrUnavailable = errors.New("region unavailable: every attempt was refused, nothing was applied")

// NotLeaderError is returned by a replica that is not its region's leader.
type NotLeaderError struct {
	RegionID uint64
	Leader   uint64 // the leader this replica knows of, or 0
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("region %d: not leader (leader hint: node %d)", e.RegionID, e.Leader)
}

// StaleEpochError is returned when the request was routed with an outdated view of the region.
type StaleEpochError struct {
	RegionID uint64
	Current  []Region // the regions that now cover the request's key
}

func (e *StaleEpochError) Error() string {
	return fmt.Sprintf("region %d: stale epoch, current: %v", e.RegionID, e.Current)
}

// KeyNotInRegionError is returned when the key is outside the region it was sent to.
type KeyNotInRegionError struct {
	Key    string
	Region Region
}

func (e *KeyNotInRegionError) Error() string {
	return fmt.Sprintf("key %q is not in %v", e.Key, e.Region)
}

// RegionNotFoundError is returned by a node that has no replica of the region.
type RegionNotFoundError struct {
	RegionID uint64
	Node     uint64
}

func (e *RegionNotFoundError) Error() string {
	return fmt.Sprintf("node %d has no replica of region %d", e.Node, e.RegionID)
}

// NodeDownError is returned when the target node is not running.
type NodeDownError struct{ Node uint64 }

func (e *NodeDownError) Error() string { return fmt.Sprintf("node %d is down", e.Node) }

// Retryable reports whether err proves the request was not applied, so sending it again is safe.
func Retryable(err error) bool {
	var nl *NotLeaderError
	var se *StaleEpochError
	var kn *KeyNotInRegionError
	var rn *RegionNotFoundError
	var nd *NodeDownError
	return errors.As(err, &nl) || errors.As(err, &se) || errors.As(err, &kn) || errors.As(err, &rn) || errors.As(err, &nd)
}
