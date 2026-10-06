# ADR 0002: Use etcd's Raft library, driven step by step by a simulator

- **Status:** Accepted

## Context

Each region needs a Raft group. Implementing Raft is a classic exercise, but the subtle parts
(commit only entries of the current term, log matching after leader changes, pre-vote, check
quorum) are where homemade implementations go wrong, and this project is about what a database
builds on top of consensus: ranges, splits, routing and correctness under faults. Testing
distributed behaviour with real goroutines and timers makes failures rare and unrepeatable.

## Decision

Use `go.etcd.io/raft/v3` through its `RawNode` interface, which leaves storage, transport and
time to the caller. A simulator owns all three: one goroutine advances a logical clock; on every
step each replica ticks, persists its new entries and hard state, then hands its messages to a
simulated network (delay, loss, partitions) and applies committed entries. Crashes keep the
replica's storage and discard everything else.

## Consequences

- A test can crash a leader, cut a node off or drop 2% of messages at an exact step, and the
  linearizability test runs 25 such histories in a few seconds.
- The same `RawNode` code would run over a real transport; only the network and the disk are
  simulated.
- The library's election timeouts use `crypto/rand`, so runs with the same seed can elect
  different leaders. Tests assert properties that must hold for every schedule (no lost
  acknowledged write, linearizable histories, replicas converge) rather than exact traces.
