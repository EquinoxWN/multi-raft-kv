// Package cluster runs nodes that host one Raft replica per region (etcd's raft library), over a
// simulated network that delays, drops and partitions messages, and can crash and restart nodes.
//
// Everything runs in one goroutine, one Step at a time: faults, message order and the workload
// come from a seeded random source. Raft's own randomized election timeouts come from
// crypto/rand inside the library, so two runs with one seed agree on faults but not always on
// who wins an election; tests therefore assert properties that hold for any schedule.
package cluster

import (
	"math/rand"
	"slices"
	"sort"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// Config sets the cluster's size and the network's behaviour.
type Config struct {
	Nodes     int     // node IDs are 1..Nodes; in M1 every region has a replica on every node
	SplitKeys int     // a region's leader proposes a split once it holds more keys than this (0: never)
	MaxDelay  int     // a message arrives 1..MaxDelay steps after it is sent
	DropRate  float64 // fraction of messages lost in transit
	Seed      int64
}

type envelope struct {
	at     int64
	from   uint64
	region uint64
	msg    raftpb.Message
}

type timer struct {
	at int64
	fn func()
}

// Cluster is the whole simulated deployment: nodes, network, directory and clock.
type Cluster struct {
	cfg      Config
	rng      *rand.Rand
	now      int64
	nodes    map[uint64]*Node
	ids      []uint64
	inflight []envelope
	cut      map[[2]uint64]bool
	timers   []timer
	nextReq  uint64
	Dir      *Directory
	Stats    Stats
}

// Stats counts what happened, for tests and the demo.
type Stats struct {
	Sent, Delivered, Dropped int
	Splits                   int
	Elections                int
}

// New starts a cluster with one region covering every key, replicated on all nodes.
func New(cfg Config) *Cluster {
	if cfg.Nodes < 1 {
		cfg.Nodes = 3
	}
	if cfg.MaxDelay < 1 {
		cfg.MaxDelay = 1
	}
	c := &Cluster{
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(cfg.Seed)),
		nodes: map[uint64]*Node{},
		cut:   map[[2]uint64]bool{},
		Dir:   newDirectory(),
	}
	first := kv.Region{ID: c.Dir.AllocID(), Epoch: 1}
	for i := 1; i <= cfg.Nodes; i++ {
		first.Peers = append(first.Peers, uint64(i))
	}
	for _, id := range first.Peers {
		n := &Node{ID: id, c: c, alive: true, peers: map[uint64]*Peer{}, disk: map[uint64]*disk{}}
		c.nodes[id] = n
		c.ids = append(c.ids, id)
		n.create(first.Clone(), map[string]string{}, false)
	}
	c.Dir.Report(first, 0)
	return c
}

// Now is the current step.
func (c *Cluster) Now() int64 { return c.now }

// Rand is the cluster's seeded random source, shared with workloads so one seed fixes a run.
func (c *Cluster) Rand() *rand.Rand { return c.rng }

// NodeIDs lists the nodes in ID order.
func (c *Cluster) NodeIDs() []uint64 { return slices.Clone(c.ids) }

// Node returns a node by ID.
func (c *Cluster) Node(id uint64) *Node { return c.nodes[id] }

// NextReqID hands out unique request IDs.
func (c *Cluster) NextReqID() uint64 {
	c.nextReq++
	return c.nextReq
}

// After runs fn at the start of the step `steps` from now.
func (c *Cluster) After(steps int64, fn func()) {
	c.timers = append(c.timers, timer{at: c.now + max(steps, 1), fn: fn})
}

// Step advances the simulation by one tick: deliver due messages and timers, tick every live
// replica, then let each replica persist, send and apply what Raft produced.
func (c *Cluster) Step() {
	c.now++
	var later []envelope
	due := c.inflight
	c.inflight = nil
	for _, e := range due {
		if e.at > c.now {
			later = append(later, e)
			continue
		}
		c.deliver(e)
	}
	c.inflight = append(later, c.inflight...)
	var pending []timer
	timers := c.timers
	c.timers = nil
	for _, t := range timers {
		if t.at <= c.now {
			t.fn()
		} else {
			pending = append(pending, t)
		}
	}
	c.timers = append(pending, c.timers...)
	for _, id := range c.ids {
		if n := c.nodes[id]; n.alive {
			n.tick()
		}
	}
	for _, id := range c.ids {
		if n := c.nodes[id]; n.alive {
			n.handleReady()
		}
	}
}

// Run steps the cluster n times.
func (c *Cluster) Run(n int) {
	for range n {
		c.Step()
	}
}

// RunUntil steps until cond holds or limit steps have passed; it reports whether cond held.
func (c *Cluster) RunUntil(cond func() bool, limit int) bool {
	for range limit {
		if cond() {
			return true
		}
		c.Step()
	}
	return cond()
}

func (c *Cluster) send(from, region uint64, m raftpb.Message) {
	c.Stats.Sent++
	if c.cut[[2]uint64{from, m.To}] || (c.cfg.DropRate > 0 && c.rng.Float64() < c.cfg.DropRate) {
		c.Stats.Dropped++
		return
	}
	at := c.now + 1 + int64(c.rng.Intn(c.cfg.MaxDelay))
	c.inflight = append(c.inflight, envelope{at: at, from: from, region: region, msg: m})
}

func (c *Cluster) deliver(e envelope) {
	n := c.nodes[e.msg.To]
	if n == nil || !n.alive || c.cut[[2]uint64{e.from, e.msg.To}] {
		c.Stats.Dropped++
		return
	}
	p := n.peers[e.region]
	if p == nil {
		// The region was split on the sender but this node has not applied the split yet;
		// Raft resends once the replica exists.
		c.Stats.Dropped++
		return
	}
	c.Stats.Delivered++
	_ = p.raft.Step(e.msg)
}

// Isolate cuts every link to and from node id (a network partition of one node).
func (c *Cluster) Isolate(id uint64) {
	for _, other := range c.ids {
		if other != id {
			c.cut[[2]uint64{id, other}] = true
			c.cut[[2]uint64{other, id}] = true
		}
	}
}

// Heal restores every link.
func (c *Cluster) Heal() { c.cut = map[[2]uint64]bool{} }

// Crash stops a node: its in-memory state is lost, its disk (Raft logs) survives.
func (c *Cluster) Crash(id uint64) { c.nodes[id].crash() }

// Restart brings a crashed node back from its disk.
func (c *Cluster) Restart(id uint64) { c.nodes[id].restart() }

// Leader returns the node that leads the region, according to the live replicas, or 0.
func (c *Cluster) Leader(region uint64) uint64 {
	for _, id := range c.ids {
		n := c.nodes[id]
		if p := n.peers[region]; n.alive && p != nil && p.isLeader() {
			return id
		}
	}
	return 0
}

// Replica returns node id's replica of a region (nil if the node is down or has none).
func (c *Cluster) Replica(node, region uint64) *Peer {
	n := c.nodes[node]
	if n == nil || !n.alive {
		return nil
	}
	return n.peers[region]
}

// Directory is the routing table clients read: the newest known version of every region and its
// leader. Leaders report to it after elections and splits. (M2 turns it into a placement driver.)
type Directory struct {
	regions map[uint64]kv.Region
	leaders map[uint64]uint64
	nextID  uint64
	Lookups int
}

func newDirectory() *Directory {
	return &Directory{regions: map[uint64]kv.Region{}, leaders: map[uint64]uint64{}}
}

// AllocID returns a new, never used region ID.
func (d *Directory) AllocID() uint64 {
	d.nextID++
	return d.nextID
}

// Report records a region version and (if not 0) its leader; older epochs never replace newer ones.
func (d *Directory) Report(r kv.Region, leader uint64) {
	if old, ok := d.regions[r.ID]; ok && old.Epoch > r.Epoch {
		return
	}
	d.regions[r.ID] = r.Clone()
	if leader != 0 {
		d.leaders[r.ID] = leader
	}
}

// Locate finds the region holding key and its last reported leader.
func (d *Directory) Locate(key string) (kv.Region, uint64, bool) {
	d.Lookups++
	var best kv.Region
	found := false
	for _, r := range d.regions {
		if r.Contains(key) && (!found || r.Epoch > best.Epoch) {
			best, found = r, true
		}
	}
	return best.Clone(), d.leaders[best.ID], found
}

// Regions lists the known regions ordered by start key.
func (d *Directory) Regions() []kv.Region {
	out := make([]kv.Region, 0, len(d.regions))
	for _, r := range d.regions {
		out = append(out, r.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}
