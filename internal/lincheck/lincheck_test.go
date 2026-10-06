package lincheck

import (
	"testing"

	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

func TestTheCheckerRejectsAStaleRead(t *testing.T) {
	h := NewHistory()
	w := h.Call(0, Input{Op: kv.OpPut, Key: "x", Value: "1"})
	h.Return(w, kv.Result{})
	r := h.Call(1, Input{Op: kv.OpGet, Key: "x"})
	h.Return(r, kv.Result{Found: false}) // the write had completed: x must be visible
	if h.Check() {
		t.Fatal("a read that misses a completed write must not be linearizable")
	}
}

func TestOverlappingOperationsMayOrderEitherWay(t *testing.T) {
	h := NewHistory()
	w := h.Call(0, Input{Op: kv.OpPut, Key: "x", Value: "1"})
	r := h.Call(1, Input{Op: kv.OpGet, Key: "x"})
	h.Return(r, kv.Result{Found: false})
	h.Return(w, kv.Result{})
	if !h.Check() {
		t.Fatal("a read concurrent with a write may return the old value")
	}
}

func TestAnUnknownWriteMayHaveHappenedOrNot(t *testing.T) {
	for _, seen := range []bool{true, false} {
		h := NewHistory()
		w := h.Call(0, Input{Op: kv.OpPut, Key: "x", Value: "1"})
		h.Return(w, kv.Result{Err: kv.ErrUnknown})
		r := h.Call(1, Input{Op: kv.OpGet, Key: "x"})
		h.Return(r, kv.Result{Value: "1", Found: seen})
		if !h.Check() {
			t.Fatalf("seen=%v: an unknown write may take effect at any later time or never", seen)
		}
	}
	if h := NewHistory(); h.Check() != true || h.Completed() != 0 {
		t.Fatal("an empty history is linearizable")
	}
}

func TestHistoriesUnderCrashesPartitionsAndSplitsAreLinearizable(t *testing.T) {
	seeds := 25
	if testing.Short() {
		seeds = 5
	}
	totalOps, totalUnknown, maxRegions := 0, 0, 0
	for seed := int64(1); seed <= int64(seeds); seed++ {
		h, c := Run(seed, 1500)
		if !h.Check() {
			t.Fatalf("seed %d: history is not linearizable", seed)
		}
		if h.Completed() < 200 {
			t.Fatalf("seed %d: only %d operations completed", seed, h.Completed())
		}
		totalOps += h.Completed()
		totalUnknown += h.Unknown
		maxRegions = max(maxRegions, len(c.Dir.Regions()))
	}
	if maxRegions < 3 {
		t.Fatalf("the workload should split regions; most seen: %d", maxRegions)
	}
	t.Logf("%d seeds: %d completed operations, %d writes with unknown outcome, up to %d regions", seeds, totalOps, totalUnknown, maxRegions)
}
