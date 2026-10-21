package harness

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/wguo7/raft-kv/kv"
	"github.com/wguo7/raft-kv/raft"
)

// Cluster is an in-process KV cluster on a simulated network.
type Cluster struct {
	ids    []int
	Net    *raft.MemNetwork
	stores map[int]*raft.MemStorage
	timing Timing

	mu       sync.Mutex
	servers  map[int]*kv.Server
	nodes    map[int]*raft.Node
	appliedN map[int]uint64    // last index applied per live node
	log      map[uint64][]byte // index -> committed entry
	leaders  map[uint64]int    // term -> leader
	elected  int               // number of elections won
	votes    map[[2]uint64]int // (term, voter) -> candidate
	restored int               // snapshots delivered to state machines
	errs     []string
}

type Timing struct {
	Heartbeat, ElectionMin, ElectionMax time.Duration
	// MaxBatch caps entries per AppendEntries. Small batches replicate a
	// leader's backlog one entry at a time, which exposes commit-rule bugs
	// that large batches hide.
	MaxBatch int
	// SnapshotEvery compacts the log every this many entries (0 = never).
	// Small values force lagging and restarted nodes through InstallSnapshot.
	SnapshotEvery uint64
}

var DefaultTiming = Timing{Heartbeat: 20 * time.Millisecond, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond}

func NewCluster(n int, seed int64, timing Timing) *Cluster {
	c := &Cluster{
		Net:      raft.NewMemNetwork(seed),
		stores:   map[int]*raft.MemStorage{},
		timing:   timing,
		servers:  map[int]*kv.Server{},
		nodes:    map[int]*raft.Node{},
		appliedN: map[int]uint64{},
		log:      map[uint64][]byte{},
		leaders:  map[uint64]int{},
		votes:    map[[2]uint64]int{},
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, i)
		c.stores[i] = raft.NewMemStorage()
	}
	for _, id := range c.ids {
		c.Start(id)
	}
	return c
}

func (c *Cluster) IDs() []int { return c.ids }

// Start boots node id from whatever its storage holds.
func (c *Cluster) Start(id int) {
	raw := make(chan raft.ApplyMsg, 256)
	toKV := make(chan raft.ApplyMsg, 256)
	srv := kv.NewServer(toKV, c.timing.SnapshotEvery)
	node, err := raft.New(raft.Config{
		ID: id, Peers: c.ids,
		HeartbeatInterval:  c.timing.Heartbeat,
		ElectionTimeoutMin: c.timing.ElectionMin,
		ElectionTimeoutMax: c.timing.ElectionMax,
		MaxBatch:           c.timing.MaxBatch,
		Storage:            c.stores[id],
		Transport:          c.Net.Endpoint(id),
		ApplyCh:            raw,
		Observer:           c.observe,
	})
	if err != nil {
		panic(err)
	}
	srv.SetNode(node)
	c.mu.Lock()
	c.servers[id], c.nodes[id], c.appliedN[id] = srv, node, 0
	c.mu.Unlock()
	go func() {
		for m := range raw {
			c.recordApply(id, node, m)
			toKV <- m
		}
	}()
	c.Net.Register(id, node)
}

// Crash kills node id; writes it had not synced are lost.
func (c *Cluster) Crash(id int) {
	c.Net.Unregister(id)
	c.mu.Lock()
	node, srv := c.nodes[id], c.servers[id]
	delete(c.nodes, id)
	delete(c.servers, id)
	c.mu.Unlock()
	if node == nil {
		return
	}
	node.Stop()
	srv.Close()
	c.stores[id].Crash()
}

func (c *Cluster) Shutdown() {
	for _, id := range c.ids {
		c.Crash(id)
	}
}

func (c *Cluster) Up(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id] != nil
}

func (c *Cluster) observe(e raft.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch e.Kind {
	case raft.EventVoted:
		k := [2]uint64{e.Term, uint64(e.Node)}
		if prev, ok := c.votes[k]; ok && prev != e.Candidate {
			c.errs = append(c.errs, fmt.Sprintf("vote safety: node %d voted for %d and %d in term %d", e.Node, prev, e.Candidate, e.Term))
		}
		c.votes[k] = e.Candidate
		return
	case raft.EventBecameLeader:
	default:
		return
	}
	// Leader completeness: a new leader's log holds every entry any node has
	// applied (applied implies committed).
	for idx, data := range c.log {
		if idx <= e.SnapshotIndex {
			continue // compacted; covered by its snapshot
		}
		if got, ok := e.EntryAt(idx); !ok || !bytes.Equal(got.Data, data) {
			c.errs = append(c.errs, fmt.Sprintf("leader completeness: node %d leads term %d without committed index %d", e.Node, e.Term, idx))
			break
		}
	}
	c.elected++
	if prev, ok := c.leaders[e.Term]; ok && prev != e.Node {
		c.errs = append(c.errs, fmt.Sprintf("election safety: nodes %d and %d both led term %d", prev, e.Node, e.Term))
	}
	c.leaders[e.Term] = e.Node
}

func (c *Cluster) recordApply(id int, node *raft.Node, m raft.ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nodes[id] != node {
		return // stale goroutine of a crashed incarnation
	}
	if m.Snapshot != nil {
		if m.Index < c.appliedN[id] {
			c.errs = append(c.errs, fmt.Sprintf("node %d installed snapshot %d after applying %d", id, m.Index, c.appliedN[id]))
		}
		c.appliedN[id] = m.Index
		c.restored++
		return
	}
	if m.Index != c.appliedN[id]+1 {
		c.errs = append(c.errs, fmt.Sprintf("node %d applied index %d after %d", id, m.Index, c.appliedN[id]))
	}
	c.appliedN[id] = m.Index
	if prev, ok := c.log[m.Index]; ok && !bytes.Equal(prev, m.Data) {
		c.errs = append(c.errs, fmt.Sprintf("state machine safety: index %d applied with different commands", m.Index))
	}
	c.log[m.Index] = m.Data
}

func (c *Cluster) Errors() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.errs...)
}

func (c *Cluster) SnapshotsRestored() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.restored
}

func (c *Cluster) Elections() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.elected
}

// Leader returns the live node that claims leadership in the highest term.
func (c *Cluster) Leader() int {
	c.mu.Lock()
	nodes := make(map[int]*raft.Node, len(c.nodes))
	for k, v := range c.nodes {
		nodes[k] = v
	}
	c.mu.Unlock()
	best, bestTerm := 0, uint64(0)
	for id, n := range nodes {
		if s := n.Status(); s.Role == raft.Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best
}

// Converged reports whether every live node has applied the same last index.
func (c *Cluster) Converged() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	var want uint64
	first := true
	for id := range c.nodes {
		if first {
			want, first = c.appliedN[id], false
		} else if c.appliedN[id] != want {
			return false
		}
	}
	return true
}

// Client retries one command across servers until it applies or the deadline
// passes. Retries reuse (ClientID, Seq) so the command applies at most once.
type Client struct {
	c        *Cluster
	id       uint64
	seq      uint64
	leader   int
	rng      *rand.Rand
	attempt  time.Duration
	deadline time.Duration
}

func (c *Cluster) NewClient(id uint64, seed int64) *Client {
	return &Client{c: c, id: id, rng: rand.New(rand.NewSource(seed)),
		attempt: 300 * time.Millisecond, deadline: 2 * time.Second}
}

// Do returns the result with ok=true once the command applies. With ok=false,
// maybeApplied reports whether some attempt timed out (so the command may
// still apply later) as opposed to every attempt being rejected outright.
func (cl *Client) Do(op kv.OpType, key, value string) (res kv.Result, ok bool, maybeApplied bool) {
	cl.seq++
	cmd := kv.Command{Type: op, Key: key, Value: value, ClientID: cl.id, Seq: cl.seq}
	end := time.Now().Add(cl.deadline)
	for time.Now().Before(end) {
		target := cl.leader
		if target == 0 {
			target = cl.c.ids[cl.rng.Intn(len(cl.c.ids))]
		}
		cl.c.mu.Lock()
		srv := cl.c.servers[target]
		cl.c.mu.Unlock()
		if srv == nil {
			cl.leader = 0
			time.Sleep(5 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), cl.attempt)
		r, err := srv.Submit(ctx, cmd)
		cancel()
		switch err {
		case nil:
			cl.leader = target
			return r, true, true
		case kv.ErrTimeout:
			maybeApplied = true
		}
		// Follow the leader hint if the server has one, else pick at random.
		cl.leader = 0
		if hint := srv.Node().Status().LeaderID; hint > 0 && hint != target {
			cl.leader = hint
		} else {
			time.Sleep(time.Duration(5+cl.rng.Intn(15)) * time.Millisecond)
		}
	}
	return kv.Result{}, false, maybeApplied
}
