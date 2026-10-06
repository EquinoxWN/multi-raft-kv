package cluster

import (
	"fmt"
	"testing"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// waitLeader steps until region has a leader and returns it.
func waitLeader(t *testing.T, c *Cluster, region uint64) uint64 {
	t.Helper()
	var leader uint64
	if !c.RunUntil(func() bool { leader = c.Leader(region); return leader != 0 }, 500) {
		t.Fatalf("region %d elected no leader", region)
	}
	return leader
}

// propose sends cmd to node and steps until it is answered.
func propose(t *testing.T, c *Cluster, node uint64, cmd kv.Command) kv.Result {
	t.Helper()
	var res kv.Result
	done := false
	if cmd.ReqID == 0 {
		cmd.ReqID = c.NextReqID()
	}
	c.Node(node).Propose(cmd, func(r kv.Result) { res, done = r, true })
	if !c.RunUntil(func() bool { return done }, 500) {
		t.Fatalf("no answer to %+v", cmd)
	}
	return res
}

// put writes through the current leader of region 1's successor holding key (no client).
func put(t *testing.T, c *Cluster, region kv.Region, key, value string) {
	t.Helper()
	leader := waitLeader(t, c, region.ID)
	r := propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: key, Value: value})
	if r.Err != nil {
		t.Fatalf("put %s: %v", key, r.Err)
	}
}

// converged reports whether every live replica of every region has the same range, epoch and data.
func converged(c *Cluster) error {
	for _, r := range c.Dir.Regions() {
		var ref *Peer
		for _, id := range c.NodeIDs() {
			p := c.Replica(id, r.ID)
			if p == nil {
				if c.Node(id).Alive() {
					return fmt.Errorf("node %d has no replica of %v", id, r)
				}
				continue
			}
			if ref == nil {
				ref = p
				continue
			}
			if p.Region().String() != ref.Region().String() {
				return fmt.Errorf("node %d sees %v, another node %v", id, p.Region(), ref.Region())
			}
			if fmt.Sprint(p.Data()) != fmt.Sprint(ref.Data()) {
				return fmt.Errorf("region %d differs on node %d", r.ID, id)
			}
		}
	}
	return nil
}
