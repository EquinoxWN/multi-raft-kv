package cluster

import (
	"slices"
	"sort"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// splitRetrySteps is how long a leader waits for a proposed split to apply before proposing again.
const splitRetrySteps = 50

// Peer is one replica of one region: a Raft state machine plus the region's keys.
type Peer struct {
	node       *Node
	region     kv.Region
	raft       *raft.RawNode
	storage    *raft.MemoryStorage
	data       map[string]string
	pending    map[uint64]func(kv.Result)
	splitAt    int64 // step at which this leader last proposed a split (0: none outstanding)
	wasLeader  bool
	AppliedOps int
}

// Region is the replica's current view of its region.
func (p *Peer) Region() kv.Region { return p.region.Clone() }

// Data returns a copy of the replica's key-value pairs.
func (p *Peer) Data() map[string]string {
	out := make(map[string]string, len(p.data))
	for k, v := range p.data {
		out[k] = v
	}
	return out
}

// LastIndex is the index of the replica's last Raft log entry.
func (p *Peer) LastIndex() uint64 {
	i, _ := p.storage.LastIndex()
	return i
}

// Applied is the index of the last entry applied to the replica's data.
func (p *Peer) Applied() uint64 { return p.raft.BasicStatus().Applied }

func (p *Peer) isLeader() bool { return p.raft.BasicStatus().RaftState == raft.StateLeader }

func (p *Peer) handleReady() {
	if !p.raft.HasReady() {
		return
	}
	rd := p.raft.Ready()
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := p.storage.SetHardState(rd.HardState); err != nil {
			panic(err)
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		// Logs are never compacted in M1, so leaders do not send snapshots; kept for completeness.
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			panic(err)
		}
	}
	if err := p.storage.Append(rd.Entries); err != nil {
		panic(err)
	}
	// Entries are persisted before messages leave, as Raft requires.
	for _, m := range rd.Messages {
		p.node.c.send(p.node.ID, p.region.ID, m)
	}
	for _, e := range rd.CommittedEntries {
		p.apply(e)
	}
	if rd.SoftState != nil {
		leader := rd.SoftState.RaftState == raft.StateLeader
		if leader && !p.wasLeader {
			p.node.c.Stats.Elections++
			p.node.c.Dir.Report(p.region, p.node.ID)
		}
		if !leader && p.wasLeader {
			// Proposals this leader accepted may still commit under the next leader, or may not:
			// their outcome is unknown, and the client must not assume either.
			p.failPending(kv.ErrUnknown)
			p.splitAt = 0
		}
		p.wasLeader = leader
	}
	p.raft.Advance(rd)
	p.maybeSplit()
}

func (p *Peer) apply(e raftpb.Entry) {
	switch e.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(e.Data); err == nil {
			p.raft.ApplyConfChange(cc)
		}
		return
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(e.Data); err == nil {
			p.raft.ApplyConfChange(cc)
		}
		return
	}
	if len(e.Data) == 0 {
		return // the empty entry a new leader appends
	}
	cmd, err := kv.DecodeCommand(e.Data)
	if err != nil {
		return
	}
	res := p.execute(cmd)
	p.AppliedOps++
	if done, ok := p.pending[cmd.ReqID]; ok {
		delete(p.pending, cmd.ReqID)
		done(res)
	}
}

// execute applies one command; every replica gets the same result because it only depends on
// the command and the replica's state, which is the same at the same log index.
func (p *Peer) execute(cmd kv.Command) kv.Result {
	if cmd.RegionID != p.region.ID || cmd.Epoch != p.region.Epoch {
		// A split was applied between proposing and applying: the key may now belong elsewhere.
		return kv.Result{Err: p.staleEpoch(cmd.Key)}
	}
	if cmd.Op != kv.OpSplit && !p.region.Contains(cmd.Key) {
		return kv.Result{Err: &kv.KeyNotInRegionError{Key: cmd.Key, Region: p.region.Clone()}}
	}
	switch cmd.Op {
	case kv.OpPut:
		p.data[cmd.Key] = cmd.Value
	case kv.OpDelete:
		delete(p.data, cmd.Key)
	case kv.OpGet:
		v, ok := p.data[cmd.Key]
		return kv.Result{Value: v, Found: ok}
	case kv.OpSplit:
		p.applySplit(cmd)
	}
	return kv.Result{}
}

// applySplit cuts the region at cmd.Key: this replica keeps [Start, Key), and a new region with
// cmd.NewRegionID takes [Key, End) with the keys that fall there. Both get the next epoch.
func (p *Peer) applySplit(cmd kv.Command) {
	if cmd.Key <= p.region.Start || !p.region.Contains(cmd.Key) {
		return
	}
	p.region.Epoch++
	child := kv.Region{ID: cmd.NewRegionID, Start: cmd.Key, End: p.region.End, Epoch: p.region.Epoch, Peers: slices.Clone(p.region.Peers)}
	p.region.End = cmd.Key
	moved := map[string]string{}
	for k, v := range p.data {
		if child.Contains(k) {
			moved[k] = v
			delete(p.data, k)
		}
	}
	p.splitAt = 0
	n := p.node
	if _, onDisk := n.disk[child.ID]; !onDisk {
		// First time this replica applies the split. After a restart the log is replayed and the
		// child already exists on disk with exactly this state, so it is not created twice.
		n.create(child, moved, p.isLeader())
		n.c.Stats.Splits++
	}
	if p.isLeader() {
		n.c.Dir.Report(p.region, n.ID)
		n.c.Dir.Report(child, 0)
	}
}

// maybeSplit lets a leader whose region holds too many keys propose a split at the median key.
func (p *Peer) maybeSplit() {
	limit := p.node.c.cfg.SplitKeys
	if limit <= 0 || len(p.data) <= limit || !p.isLeader() {
		return
	}
	now := p.node.c.now
	if p.splitAt != 0 && now-p.splitAt < splitRetrySteps {
		return
	}
	keys := make([]string, 0, len(p.data))
	for k := range p.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mid := keys[len(keys)/2]
	if mid <= p.region.Start {
		return
	}
	cmd := kv.Command{ReqID: p.node.c.NextReqID(), Op: kv.OpSplit, RegionID: p.region.ID, Epoch: p.region.Epoch, Key: mid, NewRegionID: p.node.c.Dir.AllocID()}
	if err := p.raft.Propose(cmd.Encode()); err == nil {
		p.splitAt = now
	}
}

func (p *Peer) staleEpoch(key string) error {
	current := []kv.Region{p.region.Clone()}
	for _, other := range p.node.peers {
		if other != p && other.region.Contains(key) {
			current = append(current, other.region.Clone())
		}
	}
	return &kv.StaleEpochError{RegionID: p.region.ID, Current: current}
}

func (p *Peer) failPending(err error) {
	ids := make([]uint64, 0, len(p.pending))
	for id := range p.pending {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		done := p.pending[id]
		delete(p.pending, id)
		done(kv.Result{Err: err})
	}
}
