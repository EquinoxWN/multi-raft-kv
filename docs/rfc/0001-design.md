# RFC 0001: multi-raft-kv design

- **Status:** Accepted (M1 implemented)
- **Author:** EquinoxWN
- **Created:** 2026

## Problem

A single Raft group can keep a key-value store correct through crashes and network partitions, but
it cannot grow: every write goes through one leader and one log, and the whole data set lives on
every replica. Databases such as TiKV and CockroachDB scale by cutting the key space into ranges
(regions) and giving each range its own Raft group, so thousands of small groups share a cluster.
That raises the questions this project answers in code: how a region splits without losing or
duplicating a write, how a client that routed with an old map finds out, and how to show that the
whole system stays linearizable while nodes crash, the network partitions and regions split.

## Goals

- **M1 (this RFC):**
  - Regions are key ranges `[start, end)`, each replicated by its own Raft group (etcd's `raft`
    library) on three nodes. Every read and write goes through the region's Raft log, so reads are
    linearizable.
  - A region's leader proposes a split at the median key once the region holds more than a
    configured number of keys. The split is a log entry: every replica applies it at the same log
    index, keeps the left half, and creates the right half as a new region with its own Raft group.
  - Every region has an epoch, bumped by each split. Requests carry the epoch they were routed
    with; a replica refuses an old epoch both when the request arrives and when its log entry is
    applied, so a write can never land in a region that no longer owns its key.
  - A client caches the region map, follows "not leader" hints, refreshes after "stale epoch" or
    "key not in region", and retries only when the reply proves nothing was applied.
  - A seeded simulator crashes and restarts nodes, partitions them, delays and drops messages, and
    Porcupine checks the recorded histories for linearizability.
- **M2:** a placement driver that moves replicas and leaders to balance load, more nodes than the
  replication factor, and follower reads.
- **M3:** heartbeats batched per node across all groups, a fully deterministic simulator, thousands
  of seeds, and throughput as nodes are added.

## Non-goals

- Persistence to real disks and a network transport: M1 runs every node in one process so faults
  are controlled and repeatable. The Raft library and the state machine code would be the same
  over gRPC.
- Region merges, membership changes and log compaction (snapshots) in M1.

## Proposed design

```
client --(region id, epoch, key)--> leader replica of the region on some node
                                     |  checks: leader? epoch current? key in range?
                                     v
                                 Raft log of that region (etcd raft RawNode)
                                     |  committed on a majority, applied in order on every replica
                                     v
                     put / get / delete on the region's keys, or split
```

- **Cluster** (`internal/cluster`): every step, each live node ticks all its replicas, persists
  what Raft produced (entries and hard state go to the node's "disk" before any message is sent),
  sends messages through the simulated network, and applies committed entries. The network delays
  each message by 1 to N steps and can drop it, cut a node off (`Isolate`), or heal.
- **Crash and restart:** a crash discards everything in memory and fails pending requests with
  "unknown outcome". The disk keeps each region's Raft storage, whose snapshot at index 1 holds
  the region and data as they were when the region was created. A restart rebuilds every replica
  from that snapshot and replays the committed log, including any splits, which do not create the
  child region a second time.
- **Raft settings:** election timeout 10 to 20 ticks, heartbeat every tick, `CheckQuorum` (a
  leader cut off from the majority steps down) and `PreVote` (a node coming back from a partition
  does not force a needless election).
- **Split:** `{op: split, key: median, new_region: id}` goes through the log. On apply, the
  replica bumps its epoch, keeps `[start, median)`, moves the keys at or above the median into the
  child `[median, end)` with the same epoch, and opens the child's Raft group from a fresh
  snapshot. The node that led the parent campaigns at once for the child.
- **Directory** (M1's routing table): leaders report their region after elections and splits;
  clients ask it when their cache has no region for a key. Older epochs never replace newer ones.
- **Linearizability check** (`internal/lincheck`): every call and return is stamped with a logical
  clock. A request refused before reaching a log is dropped from the history (it had no effect); a
  write whose reply never came stays open until the end (it may or may not have happened); then
  Porcupine searches for a valid order, one key at a time.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Write Raft from scratch | Raft's corner cases (log matching, commit rules, pre-vote) are where homemade versions fail; etcd's library is the one TiKV's design descends from, and this project is about what sits on top of it. ADR 0002. |
| Hash partitioning | Range partitioning keeps ordered scans possible and makes splits a matter of choosing one key; it is what TiKV and CockroachDB use. |
| Split by moving data through a separate snapshot transfer | Applying the split as a log entry gives every replica the same cut at the same index with no extra protocol. ADR 0003. |
| Lease-based local reads on the leader | Faster, but correct only with bounded clock drift; reads through the log are correct with no assumptions. A mutation test shows local reads break linearizability under partitions. |

## Measurement plan

- Linearizability of seeded fault-injected histories (25 seeds in the test suite and the demo).
- Mutation checks: deliberately broken versions (local reads, acknowledging before commit, no
  epoch check at apply) must make the tests fail.
- Time to re-elect leaders after a crash, in simulation steps.

## Milestones

- **M1 (done):** regions with one Raft group each, splits with epochs, routing client, crash and
  restart, simulated network, Porcupine checks.
- **M2:** placement driver (balance replicas and leaders), more nodes, follower reads.
- **M3:** batched heartbeats, deterministic simulator, thousands of seeds, throughput scaling.

## Risks and open questions

- etcd raft draws election timeouts from `crypto/rand`, so a seed fixes the faults and the
  workload but not the exact election outcomes; failures are reported with the seed, and M3's
  simulator will make runs fully reproducible.
- All regions live on all three nodes in M1, so a split adds Raft groups but not capacity; that is
  what the M2 placement driver changes.
- Logs are never compacted, so memory grows with history; snapshots arrive with M3.
