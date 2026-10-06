package client

import (
	"errors"
	"fmt"
	"testing"

	"github.com/EquinoxWN/multi-raft-kv/internal/cluster"
	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

func TestPutGetDeleteThroughWhicheverNodeLeads(t *testing.T) {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 2, Seed: 11})
	cl := New(c)
	if err := cl.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := cl.Get("a")
	if err != nil || !ok || v != "1" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	if err := cl.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cl.Get("a"); ok || err != nil {
		t.Fatalf("deleted key still found (%v)", err)
	}
	if err := cl.Put("", "x"); !errors.Is(err, kv.ErrInvalid) {
		t.Fatalf("empty key: %v", err)
	}
}

func TestAClientWithAStaleMapRecoversAfterSplits(t *testing.T) {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 2, SplitKeys: 6, Seed: 12})
	stale := New(c)
	if err := stale.Put("key000", "first"); err != nil {
		t.Fatal(err)
	}
	if stale.CachedRegions() != 1 {
		t.Fatal("the client should have cached the single region")
	}
	writer := New(c)
	for i := range 80 {
		if err := writer.Put(fmt.Sprintf("key%03d", i), fmt.Sprint(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if n := len(c.Dir.Regions()); n < 5 {
		t.Fatalf("expected splits, have %d regions", n)
	}
	// The stale client still believes one region covers everything.
	for i := 0; i < 80; i += 7 {
		v, ok, err := stale.Get(fmt.Sprintf("key%03d", i))
		if err != nil || !ok || v != fmt.Sprint(i) {
			t.Fatalf("key%03d through the stale client: %q %v %v", i, v, ok, err)
		}
	}
	if stale.Stats.StaleEpoch+stale.Stats.KeyNotInRegion == 0 {
		t.Fatal("the stale client should have been told its map was outdated")
	}
}

func TestRequestsSurviveALeaderCrash(t *testing.T) {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 2, Seed: 13})
	cl := New(c)
	if err := cl.Put("k", "v1"); err != nil {
		t.Fatal(err)
	}
	c.Crash(c.Leader(c.Dir.Regions()[0].ID))
	if err := cl.Put("k", "v2"); err != nil {
		t.Fatalf("put after crash: %v", err)
	}
	if v, _, err := cl.Get("k"); err != nil || v != "v2" {
		t.Fatalf("Get = %q %v", v, err)
	}
	if cl.Stats.NotLeader+cl.Stats.NodeDown == 0 {
		t.Fatal("the client should have been redirected away from the dead leader")
	}
}

func TestWithoutAMajorityRequestsFailWithoutBeingApplied(t *testing.T) {
	c := cluster.New(cluster.Config{Nodes: 3, MaxDelay: 2, Seed: 14})
	cl := New(c)
	cl.Timeout = 120
	if err := cl.Put("k", "v1"); err != nil {
		t.Fatal(err)
	}
	c.Crash(1)
	c.Crash(2)
	c.Crash(3)
	err := cl.Put("k", "v2")
	if !errors.Is(err, kv.ErrUnavailable) {
		t.Fatalf("with every node down: %v, want unavailable", err)
	}
}
