package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

func TestOneRegionIsReplicatedOnEveryNode(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, Seed: 1})
	region := c.Dir.Regions()[0]
	if len(region.Peers) != 3 || region.Start != "" || region.End != "" {
		t.Fatalf("first region: %v %v", region, region.Peers)
	}
	put(t, c, region, "apple", "red")
	c.Run(20)
	for _, id := range c.NodeIDs() {
		if got := c.Replica(id, region.ID).Data()["apple"]; got != "red" {
			t.Errorf("node %d has %q", id, got)
		}
	}
	if err := converged(c); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyTheLeaderAcceptsProposalsAndFollowersNameIt(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, Seed: 2})
	region := c.Dir.Regions()[0]
	leader := waitLeader(t, c, region.ID)
	c.Run(5)
	for _, id := range c.NodeIDs() {
		if id == leader {
			continue
		}
		r := propose(t, c, id, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: "k", Value: "v"})
		var nl *kv.NotLeaderError
		if !errors.As(r.Err, &nl) || nl.Leader != leader {
			t.Fatalf("follower %d answered %v, want not leader with hint %d", id, r.Err, leader)
		}
	}
}

func TestRequestsWithAnOldEpochOrAForeignKeyAreRefused(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 1, Seed: 3})
	region := c.Dir.Regions()[0]
	leader := waitLeader(t, c, region.ID)
	r := propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch + 7, Key: "k"})
	var se *kv.StaleEpochError
	if !errors.As(r.Err, &se) {
		t.Fatalf("got %v, want stale epoch", r.Err)
	}
	r = propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: 99, Epoch: 1, Key: "k"})
	var nf *kv.RegionNotFoundError
	if !errors.As(r.Err, &nf) {
		t.Fatalf("got %v, want region not found", r.Err)
	}
	r = propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: ""})
	if !errors.Is(r.Err, kv.ErrInvalid) {
		t.Fatalf("got %v, want invalid", r.Err)
	}
}

func TestALeaderCrashElectsANewLeaderAndLosesNoAcknowledgedWrite(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, Seed: 4})
	region := c.Dir.Regions()[0]
	for i := range 20 {
		put(t, c, region, fmt.Sprintf("k%02d", i), fmt.Sprint(i))
	}
	old := waitLeader(t, c, region.ID)
	c.Crash(old)
	var next uint64
	if !c.RunUntil(func() bool { next = c.Leader(region.ID); return next != 0 && next != old }, 500) {
		t.Fatal("no new leader after the crash")
	}
	put(t, c, region, "after", "crash")
	for i := range 20 {
		if got := c.Replica(next, region.ID).Data()[fmt.Sprintf("k%02d", i)]; got != fmt.Sprint(i) {
			t.Fatalf("new leader lost k%02d", i)
		}
	}
	c.Restart(old)
	if !c.RunUntil(func() bool { return converged(c) == nil }, 500) {
		t.Fatalf("restarted node did not catch up: %v", converged(c))
	}
	if c.Replica(old, region.ID).Data()["after"] != "crash" {
		t.Fatal("restarted node missed the write made while it was down")
	}
}

func TestAnIsolatedLeaderCannotCommitAndStepsDown(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, Seed: 5})
	region := c.Dir.Regions()[0]
	put(t, c, region, "k", "before")
	old := waitLeader(t, c, region.ID)
	c.Isolate(old)
	var res kv.Result
	answered := false
	c.Node(old).Propose(kv.Command{ReqID: c.NextReqID(), Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: "k", Value: "lost"},
		func(r kv.Result) { res, answered = r, true })
	var next uint64
	if !c.RunUntil(func() bool {
		for _, id := range c.NodeIDs() {
			if p := c.Replica(id, region.ID); id != old && p.isLeader() {
				next = id
				return true
			}
		}
		return false
	}, 500) {
		t.Fatal("the majority elected no leader")
	}
	c.RunUntil(func() bool { return answered }, 500)
	if !answered || !errors.Is(res.Err, kv.ErrUnknown) {
		t.Fatalf("the cut-off leader answered %v (answered=%v), want unknown outcome", res.Err, answered)
	}
	put(t, c, region, "k", "after")
	c.Heal()
	if !c.RunUntil(func() bool { return converged(c) == nil }, 500) {
		t.Fatal(converged(c))
	}
	for _, id := range c.NodeIDs() {
		if got := c.Replica(id, region.ID).Data()["k"]; got != "after" {
			t.Fatalf("node %d has %q: the uncommitted write survived", id, got)
		}
	}
	_ = next
}

func TestRegionsSplitAtTheMedianAndEveryReplicaAgrees(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, SplitKeys: 8, Seed: 6})
	for i := 0; i < 60; {
		key := fmt.Sprintf("key%03d", i)
		var region kv.Region
		for _, r := range c.Dir.Regions() {
			if r.Contains(key) {
				region = r
			}
		}
		leader := waitLeader(t, c, region.ID)
		r := propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: key, Value: "v"})
		var se *kv.StaleEpochError
		if errors.As(r.Err, &se) {
			c.Run(10) // the split is still being reported; try again with the new map
			continue
		}
		if r.Err != nil {
			t.Fatalf("put %s: %v", key, r.Err)
		}
		i++
	}
	c.Run(50)
	regions := c.Dir.Regions()
	if len(regions) < 4 {
		t.Fatalf("60 keys with a limit of 8 gave only %d regions", len(regions))
	}
	if regions[0].Start != "" || regions[len(regions)-1].End != "" {
		t.Fatalf("regions do not cover the key space: %v", regions)
	}
	total := 0
	for i, r := range regions {
		if i > 0 && regions[i-1].End != r.Start {
			t.Fatalf("gap or overlap between %v and %v", regions[i-1], r)
		}
		if r.Epoch < 2 {
			t.Fatalf("%v took part in a split but has epoch %d", r, r.Epoch)
		}
		leader := waitLeader(t, c, r.ID)
		data := c.Replica(leader, r.ID).Data()
		if len(data) > 8+1 {
			t.Errorf("%v holds %d keys", r, len(data))
		}
		for k := range data {
			if !r.Contains(k) {
				t.Fatalf("%v holds foreign key %s", r, k)
			}
		}
		total += len(data)
	}
	if total != 60 {
		t.Fatalf("regions hold %d keys, want 60", total)
	}
	if err := converged(c); err != nil {
		t.Fatal(err)
	}
	if c.Stats.Splits < 3*(len(regions)-1) {
		t.Fatalf("each split should be applied on all 3 nodes: %d splits for %d regions", c.Stats.Splits, len(regions))
	}
}

func TestARestartedNodeReplaysSplitsFromItsLog(t *testing.T) {
	c := New(Config{Nodes: 3, MaxDelay: 2, SplitKeys: 5, Seed: 7})
	for i := range 30 {
		key := fmt.Sprintf("k%02d", i)
		for {
			var region kv.Region
			for _, r := range c.Dir.Regions() {
				if r.Contains(key) {
					region = r
				}
			}
			leader := waitLeader(t, c, region.ID)
			r := propose(t, c, leader, kv.Command{Op: kv.OpPut, RegionID: region.ID, Epoch: region.Epoch, Key: key, Value: "v"})
			if r.Err == nil {
				break
			}
			c.Run(10)
		}
	}
	c.Run(30)
	before := len(c.Dir.Regions())
	c.Crash(3)
	c.Run(30)
	c.Restart(3)
	if !c.RunUntil(func() bool { return converged(c) == nil }, 800) {
		t.Fatal(converged(c))
	}
	if got := len(c.Node(3).Regions()); got != before {
		t.Fatalf("node 3 rebuilt %d regions, want %d", got, before)
	}
}

func TestADirectoryNeverGoesBackToAnOlderEpoch(t *testing.T) {
	d := newDirectory()
	d.Report(kv.Region{ID: 1, End: "m", Epoch: 2}, 1)
	d.Report(kv.Region{ID: 1, Epoch: 1}, 2)
	r, leader, ok := d.Locate("a")
	if !ok || r.Epoch != 2 || leader != 1 {
		t.Fatalf("Locate = %v %d %v", r, leader, ok)
	}
	if _, _, ok := d.Locate("z"); ok {
		t.Fatal("no region covers z")
	}
}
