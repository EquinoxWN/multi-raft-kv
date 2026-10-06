package cluster

import (
	"encoding/json"
	"io"
	"log"
	"slices"
	"sort"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// disk is what survives a crash for one region on one node: its Raft storage, whose snapshot at
// index 1 holds the region as it was created and its data at that moment.
type disk struct {
	storage *raft.MemoryStorage
}

type initialState struct {
	Region kv.Region         `json:"region"`
	Data   map[string]string `json:"data"`
}

// Node is one server: it hosts a replica of each region placed on it.
type Node struct {
	ID    uint64
	c     *Cluster
	alive bool
	peers map[uint64]*Peer
	disk  map[uint64]*disk
}

// Alive reports whether the node is running.
func (n *Node) Alive() bool { return n.alive }

// Regions returns the regions this node holds a replica of, ordered by start key.
func (n *Node) Regions() []kv.Region {
	var out []kv.Region
	for _, p := range n.peers {
		out = append(out, p.region.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func (n *Node) sortedPeers() []*Peer {
	ids := make([]uint64, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]*Peer, len(ids))
	for i, id := range ids {
		out[i] = n.peers[id]
	}
	return out
}

func (n *Node) tick() {
	for _, p := range n.sortedPeers() {
		p.raft.Tick()
	}
}

func (n *Node) handleReady() {
	for _, p := range n.sortedPeers() {
		p.handleReady()
	}
}

// create writes a new region's initial state to disk and opens its replica.
func (n *Node) create(region kv.Region, data map[string]string, campaign bool) *Peer {
	b, err := json.Marshal(initialState{Region: region, Data: data})
	if err != nil {
		panic(err)
	}
	s := raft.NewMemoryStorage()
	snap := raftpb.Snapshot{Data: b, Metadata: raftpb.SnapshotMetadata{
		Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: slices.Clone(region.Peers)},
	}}
	if err := s.ApplySnapshot(snap); err != nil {
		panic(err)
	}
	if err := s.SetHardState(raftpb.HardState{Term: 1, Commit: 1}); err != nil {
		panic(err)
	}
	n.disk[region.ID] = &disk{storage: s}
	p := n.open(s)
	if campaign {
		_ = p.raft.Campaign()
	}
	return p
}

var discard = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

// open builds a replica from storage: the initial state comes from the snapshot, and Raft
// replays every committed entry after it.
func (n *Node) open(s *raft.MemoryStorage) *Peer {
	snap, err := s.Snapshot()
	if err != nil {
		panic(err)
	}
	var st initialState
	if err := json.Unmarshal(snap.Data, &st); err != nil {
		panic(err) // written by create; never user input
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:              n.ID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		CheckQuorum:     true, // a leader cut off from the majority steps down
		PreVote:         true, // a node rejoining after a partition does not disrupt the leader
		Logger:          discard,
	})
	if err != nil {
		panic(err)
	}
	if st.Data == nil {
		st.Data = map[string]string{}
	}
	p := &Peer{node: n, region: st.Region, raft: rn, storage: s, data: st.Data, pending: map[uint64]func(kv.Result){}}
	n.peers[st.Region.ID] = p
	return p
}

func (n *Node) crash() {
	if !n.alive {
		return
	}
	n.alive = false
	for _, p := range n.peers {
		p.failPending(kv.ErrUnknown)
	}
	n.peers = map[uint64]*Peer{}
}

func (n *Node) restart() {
	if n.alive {
		return
	}
	n.alive = true
	ids := make([]uint64, 0, len(n.disk))
	for id := range n.disk {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		n.open(n.disk[id].storage)
	}
}

// Propose submits a client command to this node's replica of cmd.RegionID. done is called once:
// at once with an error that proves nothing was applied, or later with the result of applying
// it, or with kv.ErrUnknown if the outcome cannot be known (leadership lost, node crashed).
func (n *Node) Propose(cmd kv.Command, done func(kv.Result)) {
	if !n.alive {
		done(kv.Result{Err: &kv.NodeDownError{Node: n.ID}})
		return
	}
	if err := kv.Validate(cmd.Op, cmd.Key, cmd.Value); err != nil {
		done(kv.Result{Err: err})
		return
	}
	p := n.peers[cmd.RegionID]
	if p == nil {
		done(kv.Result{Err: &kv.RegionNotFoundError{RegionID: cmd.RegionID, Node: n.ID}})
		return
	}
	if !p.isLeader() {
		done(kv.Result{Err: &kv.NotLeaderError{RegionID: cmd.RegionID, Leader: p.raft.BasicStatus().Lead}})
		return
	}
	if cmd.Epoch != p.region.Epoch {
		done(kv.Result{Err: p.staleEpoch(cmd.Key)})
		return
	}
	if !p.region.Contains(cmd.Key) {
		done(kv.Result{Err: &kv.KeyNotInRegionError{Key: cmd.Key, Region: p.region.Clone()}})
		return
	}
	p.pending[cmd.ReqID] = done
	if err := p.raft.Propose(cmd.Encode()); err != nil {
		delete(p.pending, cmd.ReqID)
		done(kv.Result{Err: &kv.NotLeaderError{RegionID: cmd.RegionID}})
	}
}
