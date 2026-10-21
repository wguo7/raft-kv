package harness

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/wguo7/raft-kv/kv"
)

var (
	flagSeeds    = flag.Int("seeds", 10, "number of randomized runs")
	flagFirst    = flag.Int64("seed", 1, "first seed")
	flagDuration = flag.Duration("fault-time", 3*time.Second, "fault-injection time per run")
)

// TestJepsen runs randomized fault schedules. Each seed fixes the schedule of
// faults (goroutine timing still varies), so a failing seed is a starting
// point for reproduction:
//
//	go test ./harness -run TestJepsen -seed 42 -seeds 1 -v
func TestJepsen(t *testing.T) {
	seeds := *flagSeeds
	if testing.Short() {
		seeds = 2
	}
	var totalOps, unknown, elections atomic.Int64
	for i := 0; i < seeds; i++ {
		seed := *flagFirst + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			r := runOnce(t, seed, *flagDuration)
			totalOps.Add(int64(r.ops))
			unknown.Add(int64(r.unknown))
			elections.Add(int64(r.elections))
		})
	}
	t.Cleanup(func() {
		t.Logf("%d runs, %d client ops (%d with unknown outcome), %d elections",
			seeds, totalOps.Load(), unknown.Load(), elections.Load())
	})
}

type runResult struct{ ops, unknown, elections int }

type nemesis struct {
	name string
	do   func(c *Cluster, rng *rand.Rand)
}

func others(ids []int, x int) []int {
	var out []int
	for _, id := range ids {
		if id != x {
			out = append(out, id)
		}
	}
	return out
}

var nemeses = []nemesis{
	{"partition-random", func(c *Cluster, rng *rand.Rand) {
		perm := rng.Perm(len(c.ids))
		k := 1 + rng.Intn(len(c.ids)/2)
		var a, b []int
		for i, p := range perm {
			if i < k {
				a = append(a, c.ids[p])
			} else {
				b = append(b, c.ids[p])
			}
		}
		c.Net.Partition(a, b)
	}},
	{"isolate-leader", func(c *Cluster, rng *rand.Rand) {
		if l := c.Leader(); l != 0 {
			c.Net.Partition([]int{l}, others(c.ids, l))
		}
	}},
	{"leader-with-minority", func(c *Cluster, rng *rand.Rand) {
		// The leader keeps one follower: it can still reach someone but must
		// not commit, while the other three elect a new leader.
		l := c.Leader()
		if l == 0 {
			return
		}
		rest := others(c.ids, l)
		rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
		c.Net.Partition([]int{l, rest[0]}, rest[1:])
	}},
	{"bridge", func(c *Cluster, rng *rand.Rand) {
		// Two halves that can each see one bridge node, which sees everyone.
		// perm[0] is the bridge; perm[1:3] and perm[3:] cannot see each other.
		perm := rng.Perm(len(c.ids))
		c.Net.Heal()
		for i := 1; i < len(perm); i++ {
			for j := 1; j < len(perm); j++ {
				if (i <= 2) != (j <= 2) {
					c.Net.Block(c.ids[perm[i]], c.ids[perm[j]])
				}
			}
		}
	}},
	{"one-way", func(c *Cluster, rng *rand.Rand) {
		// The leader can send but not hear back from anyone.
		if l := c.Leader(); l != 0 {
			c.Net.Heal()
			for _, id := range others(c.ids, l) {
				c.Net.Block(id, l)
			}
		}
	}},
	{"heal", func(c *Cluster, rng *rand.Rand) { c.Net.Heal() }},
	{"crash-random", func(c *Cluster, rng *rand.Rand) {
		down := 0
		for _, id := range c.ids {
			if !c.Up(id) {
				down++
			}
		}
		if down < 2 {
			c.Crash(c.ids[rng.Intn(len(c.ids))])
		}
	}},
	{"crash-restart", func(c *Cluster, rng *rand.Rand) {
		// Kill and immediately reboot a node, often mid-election, so it must
		// rely on what it persisted (term, vote, synced log).
		id := c.ids[rng.Intn(len(c.ids))]
		if c.Up(id) {
			c.Crash(id)
			time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond)
			c.Start(id)
		}
	}},
	{"crash-leader", func(c *Cluster, rng *rand.Rand) {
		if l := c.Leader(); l != 0 {
			c.Crash(l)
		}
	}},
	{"restart-all-down", func(c *Cluster, rng *rand.Rand) {
		for _, id := range c.ids {
			if !c.Up(id) {
				c.Start(id)
			}
		}
	}},
	{"lossy", func(c *Cluster, rng *rand.Rand) {
		c.Net.SetUnreliable(0.1, 5*time.Millisecond)
	}},
	{"slow", func(c *Cluster, rng *rand.Rand) {
		// Delays near the heartbeat interval reorder messages and make
		// candidates overlap.
		c.Net.SetUnreliable(0.02, 25*time.Millisecond)
	}},
	{"reliable", func(c *Cluster, rng *rand.Rand) { c.Net.SetUnreliable(0, 0) }},
}

func runOnce(t *testing.T, seed int64, dur time.Duration) runResult {
	timing := DefaultTiming
	timing.MaxBatch = []int{1, 2, 8, 4096}[seed%4]
	c := NewCluster(5, seed, timing)
	defer c.Shutdown()
	rng := rand.New(rand.NewSource(seed))
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	var mu sync.Mutex
	var history []porcupine.Operation
	var unknown int
	keys := []string{"a", "b", "c"}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const clients = 6
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.NewClient(uint64(seed)<<16|uint64(i), seed*31+int64(i))
			crng := rand.New(rand.NewSource(seed*97 + int64(i)))
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				in := opInput{Key: keys[crng.Intn(len(keys))]}
				var op kv.OpType
				switch r := crng.Intn(10); {
				case r < 4:
					in.Op, op = "get", kv.OpGet
				case r < 7:
					in.Op, op, in.Value = "put", kv.OpPut, fmt.Sprintf("%d.%d ", i, j)
				default:
					in.Op, op, in.Value = "append", kv.OpAppend, fmt.Sprintf("%d.%d ", i, j)
				}
				call := now()
				res, ok, maybe := cl.Do(op, in.Key, in.Value)
				ret := now()
				mu.Lock()
				switch {
				case ok:
					history = append(history, porcupine.Operation{ClientId: i, Input: in,
						Call: call, Output: opOutput{Value: res.Value}, Return: ret})
				case maybe && in.Op != "get":
					// May have applied at any later time: return "never".
					unknown++
					history = append(history, porcupine.Operation{ClientId: i, Input: in,
						Call: call, Output: opOutput{Unknown: true}, Return: math.MaxInt64})
				}
				mu.Unlock()
			}
		}(i)
	}

	var schedule []string
	for time.Since(start) < dur {
		time.Sleep(time.Duration(30+rng.Intn(250)) * time.Millisecond)
		n := nemeses[rng.Intn(len(nemeses))]
		schedule = append(schedule, n.name)
		n.do(c, rng)
	}
	// Recovery: everyone back up, network healthy. The cluster must make
	// progress again (liveness) before clients stop.
	c.Net.Heal()
	c.Net.SetUnreliable(0, 0)
	for _, id := range c.ids {
		if !c.Up(id) {
			c.Start(id)
		}
	}
	probe := c.NewClient(uint64(seed)<<16|0xffff, seed)
	if _, ok, _ := probe.Do(kv.OpGet, "a", ""); !ok {
		t.Errorf("seed %d: no progress 2s after healing; faults: %v", seed, schedule)
	}
	close(stop)
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for !c.Converged() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !c.Converged() {
		t.Errorf("seed %d: nodes did not converge after healing", seed)
	}
	for _, e := range c.Errors() {
		t.Errorf("seed %d: %s; faults: %v", seed, e, schedule)
	}
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 60*time.Second)
	switch res {
	case porcupine.Illegal:
		path := fmt.Sprintf("%s/nonlinearizable-seed%d.html", t.TempDir(), seed)
		porcupine.VisualizePath(kvModel, info, path)
		t.Errorf("seed %d: history not linearizable (%d ops); faults: %v; see %s", seed, len(history), schedule, path)
	case porcupine.Unknown:
		t.Logf("seed %d: linearizability check timed out", seed)
	}
	return runResult{ops: len(history), unknown: unknown, elections: c.Elections()}
}
