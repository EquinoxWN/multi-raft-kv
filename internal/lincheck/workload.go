package lincheck

import (
	"fmt"

	"github.com/EquinoxWN/multi-raft-kv/internal/client"
	"github.com/EquinoxWN/multi-raft-kv/internal/cluster"
	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// Run drives four clients and a nemesis (one crash or one partition at a time, then healing)
// against a 3-node cluster that splits regions above 6 keys, and returns the recorded history.
func Run(seed int64, steps int) (*History, *cluster.Cluster) {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 3, DropRate: 0.02, SplitKeys: 6, Seed: seed})
	rng := c.Rand()
	h := NewHistory()
	const clients = 4
	busy := make([]bool, clients)
	writes := 0
	cls := make([]*client.Client, clients)
	for i := range cls {
		cls[i] = client.New(c)
		cls[i].Timeout = 150
	}
	down, isolated := uint64(0), uint64(0)
	for step := 0; step < steps; step++ {
		if step < steps-300 {
			for i := range clients {
				if busy[i] {
					continue
				}
				key := fmt.Sprintf("k%02d", rng.Intn(24))
				in := Input{Op: kv.OpGet, Key: key}
				switch x := rng.Intn(10); {
				case x < 6:
					writes++
					in = Input{Op: kv.OpPut, Key: key, Value: fmt.Sprintf("c%d-%d", i, writes)}
				case x < 7:
					in = Input{Op: kv.OpDelete, Key: key}
				}
				busy[i] = true
				token := h.Call(i, in)
				cls[i].Start(in.Op, in.Key, in.Value, func(r kv.Result) {
					h.Return(token, r)
					busy[i] = false
				})
			}
		}
		if step%120 == 60 && step < steps-300 {
			// One fault at a time, so a majority stays reachable.
			switch {
			case down != 0:
				c.Restart(down)
				down = 0
			case isolated != 0:
				c.Heal()
				isolated = 0
			case rng.Intn(2) == 0:
				down = uint64(rng.Intn(3) + 1)
				c.Crash(down)
			default:
				isolated = uint64(rng.Intn(3) + 1)
				c.Isolate(isolated)
			}
		}
		if step == steps-300 {
			c.Heal()
			if down != 0 {
				c.Restart(down)
			}
		}
		c.Step()
	}
	return h, c
}
