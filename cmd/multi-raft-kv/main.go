// Command multi-raft-kv runs the simulated cluster: it writes keys until regions split, crashes
// a leader and restarts it, and checks many fault-injected histories for linearizability.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/EquinoxWN/multi-raft-kv/internal/client"
	"github.com/EquinoxWN/multi-raft-kv/internal/cluster"
	"github.com/EquinoxWN/multi-raft-kv/internal/lincheck"
)

func main() {
	keys := flag.Int("keys", 2000, "keys to write")
	split := flag.Int("split", 250, "split a region above this many keys")
	seed := flag.Int64("seed", 1, "seed for faults, delays and the workload")
	seeds := flag.Int("seeds", 25, "fault-injected histories to check for linearizability")
	flag.Parse()
	if err := run(*keys, *split, *seed, *seeds); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(keys, split int, seed int64, seeds int) error {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 2, SplitKeys: split, Seed: seed})
	cl := client.New(c)
	fmt.Printf("3 nodes, one Raft group per region, regions split above %d keys (seed %d)\n\n", split, seed)

	start := c.Now()
	for i := range keys {
		if err := cl.Put(key(i), fmt.Sprint(i)); err != nil {
			return fmt.Errorf("put %s: %w", key(i), err)
		}
	}
	c.Run(50)
	fmt.Printf("1. Wrote %d keys in %d steps; the region split %d times:\n\n", keys, c.Now()-start, len(c.Dir.Regions())-1)
	table(c)

	// Crash the node that leads the most regions.
	leads := map[uint64]int{}
	for _, r := range c.Dir.Regions() {
		leads[c.Leader(r.ID)]++
	}
	victim, led := uint64(0), 0
	for _, id := range c.NodeIDs() {
		if leads[id] > led {
			victim, led = id, leads[id]
		}
	}
	c.Crash(victim)
	crashed := c.Now()
	c.RunUntil(func() bool {
		for _, r := range c.Dir.Regions() {
			if l := c.Leader(r.ID); l == 0 || l == victim {
				return false
			}
		}
		return true
	}, 1000)
	fmt.Printf("\n2. Crashed node %d, leader of %d of the regions: every region had a new leader after %d steps (election timeout: 10 to 20).\n", victim, led, c.Now()-crashed)
	for i := range keys {
		v, ok, err := cl.Get(key(i))
		if err != nil || !ok || v != fmt.Sprint(i) {
			return fmt.Errorf("after the crash, %s = %q, %v, %v", key(i), v, ok, err)
		}
	}
	for i := range 100 {
		if err := cl.Put(key(i), "updated"); err != nil {
			return fmt.Errorf("write after the crash: %w", err)
		}
	}
	fmt.Printf("   All %d keys read back through Raft on the surviving majority; 100 more writes succeeded.\n", keys)

	c.Restart(victim)
	back := c.Now()
	ok := c.RunUntil(func() bool { return same(c) }, 2000)
	fmt.Printf("\n3. Restarted node %d from its Raft logs: all replicas identical again after %d steps (%v).\n", victim, c.Now()-back, ok)
	fmt.Printf("   Client retries: %d not-leader, %d node-down, %d stale-epoch, %d key-not-in-region; directory lookups: %d.\n",
		cl.Stats.NotLeader, cl.Stats.NodeDown, cl.Stats.StaleEpoch, cl.Stats.KeyNotInRegion, c.Dir.Lookups)

	linear, ops, unknown, regions := 0, 0, 0, 0
	for s := int64(1); s <= int64(seeds); s++ {
		h, hc := lincheck.Run(s, 1500)
		if h.Check() {
			linear++
		}
		ops += h.Completed()
		unknown += h.Unknown
		regions = max(regions, len(hc.Dir.Regions()))
	}
	fmt.Printf("\n4. Linearizability (Porcupine): %d of %d histories linearizable under crashes, partitions, 2%% message loss\n", linear, seeds)
	fmt.Printf("   and splits (up to %d regions): %d completed operations, %d writes with an unknown outcome.\n", regions, ops, unknown)
	if linear != seeds {
		return fmt.Errorf("%d histories were not linearizable", seeds-linear)
	}
	return nil
}

func key(i int) string { return fmt.Sprintf("user%05d", i) }

func table(c *cluster.Cluster) {
	fmt.Printf("   %-7s %-28s %-6s %-7s %s\n", "Region", "Range", "Epoch", "Leader", "Keys")
	for _, r := range c.Dir.Regions() {
		end := fmt.Sprintf("%q", r.End)
		if r.End == "" {
			end = "+inf"
		}
		leader := c.Leader(r.ID)
		keys := 0
		if p := c.Replica(leader, r.ID); p != nil {
			keys = len(p.Data())
		}
		fmt.Printf("   %-7d %-28s %-6d node %-2d %d\n", r.ID, fmt.Sprintf("[%q, %s)", r.Start, end), r.Epoch, leader, keys)
	}
}

// same reports whether every live replica of every region holds the same data.
func same(c *cluster.Cluster) bool {
	for _, r := range c.Dir.Regions() {
		var ref map[string]string
		for _, id := range c.NodeIDs() {
			p := c.Replica(id, r.ID)
			if p == nil {
				return false
			}
			d := p.Data()
			if ref != nil && fmt.Sprint(d) != fmt.Sprint(ref) {
				return false
			}
			ref = d
		}
	}
	return true
}
