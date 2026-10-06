# ADR 0003: Split regions through the Raft log, and fence requests with region epochs

- **Status:** Accepted

## Context

When a region splits, writes for keys in the upper half must go to the new region from then on. If
replicas cut the region at different moments, or a write routed before the split is applied after
it, the write lands in a region that no longer owns its key and silently disappears.

## Decision

- The leader proposes the split as an ordinary log entry carrying the split key and the new region
  ID. Every replica applies it at the same log index, so every replica cuts the same data at the
  same point; the right half becomes a new region with its own Raft group on the same nodes.
- Each split increments the region's epoch, and the new region starts at that epoch. Every request
  carries the epoch the client routed with. The replica checks it twice: when the request arrives
  (refused at once, nothing proposed) and again when its log entry is applied (refused on every
  replica alike, so the write never changes state). Both refusals tell the client which regions
  now cover the key, and the client may safely retry.
- After a restart the log is replayed, including splits; a child that already exists on disk is
  not created again.

## Consequences

- Splits need no extra protocol and cannot diverge between replicas; tests check that every
  replica ends with the same ranges, epochs and data, and that regions tile the key space with no
  gap or overlap.
- Removing the apply-time epoch check makes the split test fail, which shows the second check is
  needed, not just the first.
- A client with a stale map pays one extra round trip per region it learns about; the demo counts
  these retries.
