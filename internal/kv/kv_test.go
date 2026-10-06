package kv

import (
	"errors"
	"strings"
	"testing"
)

func TestRegionContainsIsHalfOpen(t *testing.T) {
	r := Region{ID: 2, Start: "b", End: "d", Epoch: 3}
	for key, want := range map[string]bool{"a": false, "b": true, "c": true, "czz": true, "d": false, "e": false} {
		if got := r.Contains(key); got != want {
			t.Errorf("Contains(%q) = %v, want %v", key, got, want)
		}
	}
	all := Region{ID: 1}
	for _, key := range []string{"\x00", "a", "zzzz"} {
		if !all.Contains(key) {
			t.Errorf("the unbounded region must contain %q", key)
		}
	}
	if got := r.String(); got != `region 2 ["b", "d") epoch 3` {
		t.Errorf("String() = %s", got)
	}
}

func TestCloneDoesNotShareThePeerSlice(t *testing.T) {
	r := Region{Peers: []uint64{1, 2, 3}}
	c := r.Clone()
	c.Peers[0] = 9
	if r.Peers[0] != 1 {
		t.Fatal("Clone shares Peers")
	}
}

func TestCommandsRoundTripAndBadEntriesAreErrors(t *testing.T) {
	c := Command{ReqID: 7, Op: OpSplit, RegionID: 1, Epoch: 2, Key: "m", NewRegionID: 5}
	got, err := DecodeCommand(c.Encode())
	if err != nil || got != c {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	for _, bad := range []string{"", "{", "[]", `{"op":"drop-table"}`, `{"op":1}`} {
		if _, err := DecodeCommand([]byte(bad)); err == nil {
			t.Errorf("DecodeCommand(%q) should fail", bad)
		}
	}
}

func TestValidateRejectsWhatTheClusterNeverAccepts(t *testing.T) {
	ok := []struct {
		op         Op
		key, value string
	}{{OpPut, "k", "v"}, {OpGet, "k", ""}, {OpDelete, strings.Repeat("k", MaxKeyBytes), ""}}
	for _, c := range ok {
		if err := Validate(c.op, c.key, c.value); err != nil {
			t.Errorf("Validate(%v) = %v", c, err)
		}
	}
	bad := []struct {
		op         Op
		key, value string
	}{
		{OpSplit, "k", ""},
		{OpPut, "", "v"},
		{OpPut, strings.Repeat("k", MaxKeyBytes+1), "v"},
		{OpPut, "k", strings.Repeat("v", MaxValueBytes+1)},
	}
	for _, c := range bad {
		if err := Validate(c.op, c.key, c.value); !errors.Is(err, ErrInvalid) {
			t.Errorf("Validate(%v %q...) = %v, want ErrInvalid", c.op, c.key[:min(len(c.key), 5)], err)
		}
	}
}

func TestOnlyRefusalsAreRetryable(t *testing.T) {
	retry := []error{
		&NotLeaderError{RegionID: 1, Leader: 2},
		&StaleEpochError{RegionID: 1},
		&KeyNotInRegionError{Key: "k"},
		&RegionNotFoundError{RegionID: 1, Node: 3},
		&NodeDownError{Node: 2},
	}
	for _, err := range retry {
		if !Retryable(err) || err.Error() == "" {
			t.Errorf("%T should be retryable with a message", err)
		}
	}
	for _, err := range []error{ErrUnknown, ErrUnavailable, ErrInvalid, errors.New("other")} {
		if Retryable(err) {
			t.Errorf("%v must not be retried", err)
		}
	}
}
