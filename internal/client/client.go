// Package client routes requests to region leaders: it caches the region map, follows "not
// leader" hints, refreshes its routing after "stale epoch" and "key not in region" replies, and
// retries only when the reply proves the request was not applied.
package client

import (
	"errors"

	"github.com/EquinoxWN/multi-raft-kv/internal/cluster"
	"github.com/EquinoxWN/multi-raft-kv/internal/kv"
)

// Stats counts how often each kind of retry happened.
type Stats struct {
	Requests, NotLeader, StaleEpoch, KeyNotInRegion, NotFound, NodeDown, Unknown int
}

// Client is one caller's view of the cluster.
type Client struct {
	c       *cluster.Cluster
	regions map[uint64]kv.Region
	leaders map[uint64]uint64
	// Timeout is how many steps a request may take, including retries.
	Timeout int64
	Stats   Stats
}

// New creates a client with an empty routing cache.
func New(c *cluster.Cluster) *Client {
	return &Client{c: c, regions: map[uint64]kv.Region{}, leaders: map[uint64]uint64{}, Timeout: 200}
}

// CachedRegions is how many regions the client currently knows.
func (cl *Client) CachedRegions() int { return len(cl.regions) }

// Start sends one request and calls done exactly once with its result. Errors that prove the
// request was not applied are retried until Timeout; then done gets kv.ErrUnavailable. A
// request that reached a Raft log without a reply ends with kv.ErrUnknown and is never resent.
func (cl *Client) Start(op kv.Op, key, value string, done func(kv.Result)) {
	cl.Stats.Requests++
	if err := kv.Validate(op, key, value); err != nil {
		done(kv.Result{Err: err})
		return
	}
	deadline := cl.c.Now() + cl.Timeout
	reqID := cl.c.NextReqID()
	finished := false
	finish := func(r kv.Result) {
		if !finished {
			finished = true
			done(r)
		}
	}
	var attempt func()
	attempt = func() {
		if finished {
			return
		}
		if cl.c.Now() >= deadline {
			finish(kv.Result{Err: kv.ErrUnavailable})
			return
		}
		region, ok := cl.route(key)
		if !ok {
			cl.c.After(2, attempt)
			return
		}
		node := cl.leaders[region.ID]
		if node == 0 {
			node = region.Peers[int(reqID+uint64(cl.c.Now()))%len(region.Peers)]
		}
		sent := cl.c.Now()
		cmd := kv.Command{ReqID: reqID, Op: op, RegionID: region.ID, Epoch: region.Epoch, Key: key, Value: value}
		cl.c.Node(node).Propose(cmd, func(r kv.Result) {
			if finished {
				return
			}
			if r.Err == nil || !kv.Retryable(r.Err) {
				if errors.Is(r.Err, kv.ErrUnknown) {
					cl.Stats.Unknown++
				}
				finish(r)
				return
			}
			cl.learn(region, node, r.Err)
			// Back off a little when the refusal came at once, so elections can finish.
			wait := int64(1)
			if cl.c.Now() == sent {
				wait = 2
			}
			cl.c.After(wait, attempt)
		})
		// A proposal that never comes back is an unknown outcome once the deadline passes.
		cl.c.After(deadline-cl.c.Now()+1, func() { finish(kv.Result{Err: kv.ErrUnknown}) })
	}
	attempt()
}

// route finds the region for key, asking the directory when the cache has no answer.
func (cl *Client) route(key string) (kv.Region, bool) {
	for _, r := range cl.regions {
		if r.Contains(key) {
			return r, true
		}
	}
	r, leader, ok := cl.c.Dir.Locate(key)
	if !ok {
		return kv.Region{}, false
	}
	cl.regions[r.ID] = r
	if leader != 0 {
		cl.leaders[r.ID] = leader
	}
	return r, true
}

// learn updates the cache from a refusal.
func (cl *Client) learn(region kv.Region, node uint64, err error) {
	var nl *kv.NotLeaderError
	var se *kv.StaleEpochError
	var kn *kv.KeyNotInRegionError
	var nf *kv.RegionNotFoundError
	var nd *kv.NodeDownError
	switch {
	case errors.As(err, &nl):
		cl.Stats.NotLeader++
		if nl.Leader != 0 && nl.Leader != node {
			cl.leaders[region.ID] = nl.Leader
		} else {
			delete(cl.leaders, region.ID)
		}
	case errors.As(err, &se):
		cl.Stats.StaleEpoch++
		delete(cl.regions, region.ID)
		for _, r := range se.Current {
			// Newer views replace older ones; a region that lost its upper half is now shorter.
			if old, ok := cl.regions[r.ID]; !ok || r.Epoch >= old.Epoch {
				cl.regions[r.ID] = r
			}
		}
	case errors.As(err, &kn):
		cl.Stats.KeyNotInRegion++
		delete(cl.regions, region.ID)
	case errors.As(err, &nf):
		cl.Stats.NotFound++
		delete(cl.regions, region.ID)
		delete(cl.leaders, region.ID)
	case errors.As(err, &nd):
		cl.Stats.NodeDown++
		delete(cl.leaders, region.ID)
	}
}

// Do runs one request to completion, stepping the cluster while it waits.
func (cl *Client) Do(op kv.Op, key, value string) kv.Result {
	var out kv.Result
	done := false
	cl.Start(op, key, value, func(r kv.Result) { out, done = r, true })
	cl.c.RunUntil(func() bool { return done }, int(cl.Timeout)+10)
	return out
}

// Put stores value under key.
func (cl *Client) Put(key, value string) error { return cl.Do(kv.OpPut, key, value).Err }

// Get reads key through the region's Raft log, so the answer is linearizable.
func (cl *Client) Get(key string) (string, bool, error) {
	r := cl.Do(kv.OpGet, key, "")
	return r.Value, r.Found, r.Err
}

// Delete removes key.
func (cl *Client) Delete(key string) error { return cl.Do(kv.OpDelete, key, "").Err }
