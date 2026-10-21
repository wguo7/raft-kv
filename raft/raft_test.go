package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testCluster struct {
	t       *testing.T
	net     *MemNetwork
	ids     []int
	nodes   map[int]*Node
	stores  map[int]*MemStorage
	mu      sync.Mutex
	applied map[uint64][]byte // index -> data, must agree across nodes
	byNode  map[int]uint64    // last index applied per node
	leaders map[uint64]int    // term -> leader (election safety)
	errs    []string
}

func newTestCluster(t *testing.T, n int, seed int64) *testCluster {
	c := &testCluster{
		t: t, net: NewMemNetwork(seed),
		nodes: map[int]*Node{}, stores: map[int]*MemStorage{},
		applied: map[uint64][]byte{}, byNode: map[int]uint64{}, leaders: map[uint64]int{},
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, i)
		c.stores[i] = NewMemStorage()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) start(id int) {
	applyCh := make(chan ApplyMsg, 64)
	node, err := New(Config{
		ID: id, Peers: c.ids,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
		Storage:            c.stores[id],
		Transport:          c.net.Endpoint(id),
		ApplyCh:            applyCh,
		Observer:           c.observe,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.byNode[id] = 0
	c.mu.Unlock()
	c.net.Register(id, node)
	go func() {
		for m := range applyCh {
			c.onApply(id, m)
		}
	}()
}

func (c *testCluster) observe(e Event) {
	if e.Kind != EventBecameLeader {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.leaders[e.Term]; ok && prev != e.Node {
		c.errs = append(c.errs, fmt.Sprintf("two leaders in term %d: %d and %d", e.Term, prev, e.Node))
	}
	c.leaders[e.Term] = e.Node
}

func (c *testCluster) onApply(id int, m ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m.Snapshot != nil {
		c.byNode[id] = m.Index
		return
	}
	if m.Index != c.byNode[id]+1 {
		c.errs = append(c.errs, fmt.Sprintf("node %d applied %d after %d", id, m.Index, c.byNode[id]))
	}
	c.byNode[id] = m.Index
	if prev, ok := c.applied[m.Index]; ok && !bytes.Equal(prev, m.Data) {
		c.errs = append(c.errs, fmt.Sprintf("index %d applied as %q and %q", m.Index, prev, m.Data))
	}
	c.applied[m.Index] = m.Data
}

// crash stops a node and drops its unsynced writes.
func (c *testCluster) crash(id int) {
	c.net.Unregister(id)
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
	c.stores[id].Crash()
}

func (c *testCluster) stopAll() {
	c.mu.Lock()
	nodes := c.nodes
	c.nodes = map[int]*Node{}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
}

func (c *testCluster) checkErrs() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.errs {
		c.t.Error(e)
	}
	if len(c.errs) > 0 {
		c.t.FailNow()
	}
}

// leader returns a node that believes it leads in the highest term, if any.
func (c *testCluster) leader() (int, uint64) {
	c.mu.Lock()
	nodes := make(map[int]*Node, len(c.nodes))
	for k, v := range c.nodes {
		nodes[k] = v
	}
	c.mu.Unlock()
	best, bestTerm := 0, uint64(0)
	for id, n := range nodes {
		s := n.Status()
		if s.Role == Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best, bestTerm
}

func (c *testCluster) waitLeader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, _ := c.leader(); id != 0 {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %v", timeout)
	return 0
}

// commit proposes data until it is applied on at least `want` nodes.
func (c *testCluster) commit(data []byte, want int, timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, _ := c.leader()
		c.mu.Lock()
		n := c.nodes[id]
		c.mu.Unlock()
		if n == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		idx, _, err := n.Propose(data)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		for t2 := time.Now().Add(time.Second); time.Now().Before(t2); {
			if c.appliedCount(idx, data) >= want {
				return idx
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	c.t.Fatalf("could not commit %q on %d nodes", data, want)
	return 0
}

func (c *testCluster) appliedCount(idx uint64, data []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.applied[idx]; !ok || !bytes.Equal(d, data) {
		return 0
	}
	cnt := 0
	for id := range c.nodes {
		if c.byNode[id] >= idx {
			cnt++
		}
	}
	return cnt
}

func TestInitialElection(t *testing.T) {
	c := newTestCluster(t, 3, 1)
	l := c.waitLeader(2 * time.Second)
	_, term := c.leader()
	time.Sleep(500 * time.Millisecond)
	if l2, term2 := c.leader(); l2 != l || term2 != term {
		t.Fatalf("leader changed without failures: %d/%d -> %d/%d", l, term, l2, term2)
	}
	c.checkErrs()
}

func TestReElection(t *testing.T) {
	c := newTestCluster(t, 5, 2)
	l := c.waitLeader(2 * time.Second)
	var rest []int
	for _, id := range c.ids {
		if id != l {
			rest = append(rest, id)
		}
	}
	c.net.Partition([]int{l}, rest)
	time.Sleep(400 * time.Millisecond)
	l2, _ := c.leader()
	if l2 == l || l2 == 0 {
		// the old leader may still think it leads only until check-quorum fires
		t.Fatalf("expected a new leader among %v, got %d", rest, l2)
	}
	c.net.Heal()
	c.commit([]byte("after-heal"), 5, 3*time.Second)
	c.checkErrs()
}

func TestNoQuorumNoCommit(t *testing.T) {
	c := newTestCluster(t, 5, 3)
	c.commit([]byte("a"), 5, 2*time.Second)
	l := c.waitLeader(time.Second)
	others := []int{}
	for _, id := range c.ids {
		if id != l {
			others = append(others, id)
		}
	}
	// Leader keeps one follower: 2 of 5 is not a quorum.
	c.net.Partition([]int{l, others[0]}, others[1:])
	c.mu.Lock()
	n := c.nodes[l]
	c.mu.Unlock()
	idx, _, err := n.Propose([]byte("minority"))
	if err == nil {
		time.Sleep(300 * time.Millisecond)
		if c.appliedCount(idx, []byte("minority")) > 0 {
			t.Fatal("entry committed without a quorum")
		}
	}
	c.net.Heal()
	c.commit([]byte("b"), 5, 3*time.Second)
	c.checkErrs()
}

func TestCrashRestartReplaysLog(t *testing.T) {
	c := newTestCluster(t, 3, 4)
	for i := 0; i < 20; i++ {
		c.commit([]byte(fmt.Sprintf("v%d", i)), 3, 2*time.Second)
	}
	for _, id := range c.ids {
		c.crash(id)
	}
	c.mu.Lock()
	c.byNode = map[int]uint64{}
	c.mu.Unlock()
	for _, id := range c.ids {
		c.start(id)
	}
	c.commit([]byte("after"), 3, 3*time.Second)
	c.checkErrs()
}

// TestChaos runs proposals while randomly partitioning, crashing (with loss of
// unsynced writes) and restarting nodes on a lossy network, then heals and
// checks that every node applied the same log.
func TestChaos(t *testing.T) {
	seeds := 3
	if testing.Short() {
		seeds = 1
	}
	for s := 0; s < seeds; s++ {
		t.Run(fmt.Sprint("seed", s), func(t *testing.T) { chaos(t, int64(100+s)) })
	}
}

func chaos(t *testing.T, seed int64) {
	c := newTestCluster(t, 5, seed)
	c.net.SetUnreliable(0.05, 3*time.Millisecond)
	rng := rand.New(rand.NewSource(seed))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id, _ := c.leader()
				c.mu.Lock()
				n := c.nodes[id]
				c.mu.Unlock()
				if n != nil {
					n.Propose([]byte(fmt.Sprintf("w%d-%d", w, i)))
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	down := map[int]bool{}
	for step := 0; step < 30; step++ {
		switch rng.Intn(4) {
		case 0:
			perm := rng.Perm(5)
			k := 1 + rng.Intn(2)
			var a, b []int
			for i, p := range perm {
				if i < k {
					a = append(a, p+1)
				} else {
					b = append(b, p+1)
				}
			}
			c.net.Partition(a, b)
		case 1:
			c.net.Heal()
		case 2:
			id := 1 + rng.Intn(5)
			if !down[id] && len(down) < 2 {
				c.crash(id)
				down[id] = true
			}
		case 3:
			for id := range down {
				c.mu.Lock()
				c.byNode[id] = 0
				c.mu.Unlock()
				c.start(id)
				delete(down, id)
				break
			}
		}
		time.Sleep(time.Duration(50+rng.Intn(200)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	for id := range down {
		c.mu.Lock()
		c.byNode[id] = 0
		c.mu.Unlock()
		c.start(id)
	}
	c.net.Heal()
	c.net.SetUnreliable(0, 0)
	c.commit([]byte("final"), 5, 5*time.Second)
	c.checkErrs()
}

func TestFileStorageRecoversFromTornWrite(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir)
	if _, err := s.Load(); err != nil {
		t.Fatal(err)
	}
	s.SetState(7, 3)
	s.Append([]Entry{{1, 1, []byte("a")}, {2, 1, []byte("b")}, {3, 2, []byte("c")}})
	s.TruncateFrom(3)
	s.Append([]Entry{{3, 3, []byte("c2")}, {4, 3, nil}})
	s.Sync()
	s.Close()

	// Simulate a crash in the middle of writing one more record.
	names, _ := filepath.Glob(filepath.Join(dir, "wal-*"))
	f, _ := os.OpenFile(names[len(names)-1], os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{1, 2, 3, 4, 1, 5, 0, 0})
	f.Close()

	s2, _ := OpenFileStorage(dir)
	st, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Term != 7 || st.VotedFor != 3 {
		t.Fatalf("state = %d/%d", st.Term, st.VotedFor)
	}
	want := []Entry{{1, 1, []byte("a")}, {2, 1, []byte("b")}, {3, 3, []byte("c2")}, {4, 3, nil}}
	checkEntries(t, st.Entries, want)
	s2.Append([]Entry{{5, 3, []byte("e")}})
	s2.Sync()
	s2.Close()
	s3, _ := OpenFileStorage(dir)
	st, _ = s3.Load()
	checkEntries(t, st.Entries, append(want, Entry{5, 3, []byte("e")}))
}

func TestFileStorageSnapshotCompactsWAL(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir)
	s.Load()
	for i := uint64(1); i <= 100; i++ {
		s.Append([]Entry{{i, 1, []byte(fmt.Sprint(i))}})
	}
	s.Sync()
	if err := s.SaveSnapshot(60, 1, []byte("state@60")); err != nil {
		t.Fatal(err)
	}
	s.Append([]Entry{{101, 2, []byte("101")}})
	s.Sync()
	// The first segment still holds 61..100, so it must survive this snapshot
	// and be deleted by one that covers it.
	if err := s.SaveSnapshot(101, 2, []byte("state@101")); err != nil {
		t.Fatal(err)
	}
	s.SaveSnapshot(50, 1, []byte("stale")) // ignored
	s.Append([]Entry{{102, 2, []byte("102")}})
	s.Sync()
	s.Close()
	names, _ := filepath.Glob(filepath.Join(dir, "wal-*"))
	if len(names) != 1 {
		t.Fatalf("segments after compaction: %v", names)
	}
	s2, _ := OpenFileStorage(dir)
	st, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.SnapIndex != 101 || st.SnapTerm != 2 || string(st.Snapshot) != "state@101" {
		t.Fatalf("snapshot = %d/%d/%q", st.SnapIndex, st.SnapTerm, st.Snapshot)
	}
	checkEntries(t, st.Entries, []Entry{{102, 2, []byte("102")}})
}

// A follower that installs a snapshot past the end of its log truncates and
// then appends after a gap; replay must accept that.
func TestFileStorageGapAfterInstalledSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir)
	s.Load()
	s.Append([]Entry{{1, 1, nil}, {2, 1, nil}})
	s.Sync()
	s.SaveSnapshot(50, 3, []byte("snap"))
	s.TruncateFrom(51)
	s.Append([]Entry{{51, 3, []byte("x")}})
	s.Sync()
	s.Close()
	s2, _ := OpenFileStorage(dir)
	st, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	checkEntries(t, st.Entries, []Entry{{51, 3, []byte("x")}})
}

func checkEntries(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries %+v, want %+v", len(got), got, want)
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
