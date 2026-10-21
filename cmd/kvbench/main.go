// kvbench measures write throughput and latency: many concurrent client
// sessions each issue PUTs back to back; a write counts once the leader
// reports it committed on a majority and applied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wguo7/raft-kv/kv"
)

func main() {
	nodes := flag.String("nodes", "localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085", "comma-separated host:port")
	clients := flag.Int("clients", 256, "concurrent client sessions")
	dur := flag.Duration("duration", 20*time.Second, "measured duration")
	warmup := flag.Duration("warmup", 3*time.Second, "unmeasured warm-up")
	keys := flag.Int("keys", 100000, "key space")
	size := flag.Int("value-size", 64, "value size in bytes")
	out := flag.String("json", "", "write results as JSON to this file")
	flag.Parse()

	addrs := strings.Split(*nodes, ",")
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *clients, MaxConnsPerHost: 0}}
	value := strings.Repeat("x", *size)

	var measuring atomic.Bool
	var errs atomic.Int64
	lat := make([][]time.Duration, *clients)
	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*dur)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := kv.NewClient(addrs, hc)
			rng := rand.New(rand.NewSource(int64(i)))
			for ctx.Err() == nil {
				k := fmt.Sprintf("k%d", rng.Intn(*keys))
				t0 := time.Now()
				rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
				err := c.Put(rctx, k, value)
				rcancel()
				if !measuring.Load() {
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				lat[i] = append(lat[i], time.Since(t0))
			}
		}(i)
	}
	time.Sleep(*warmup)
	termBefore := maxTerm(addrs)
	measuring.Store(true)
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)
	// Each new term means an election started; a stable run keeps one term.
	elections := maxTerm(addrs) - termBefore

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(p*float64(len(all)-1))].Microseconds()) / 1000
	}
	res := map[string]any{
		"clients": *clients, "value_size": *size, "seconds": elapsed.Seconds(),
		"writes": len(all), "errors": errs.Load(), "new_terms": elections,
		"writes_per_sec": float64(len(all)) / elapsed.Seconds(),
		"p50_ms":         pct(0.50), "p99_ms": pct(0.99), "p999_ms": pct(0.999), "max_ms": pct(1),
	}
	fmt.Printf("clients=%d writes=%d errors=%d new_terms=%d in %.1fs: %.0f writes/s, p50 %.2f ms, p99 %.2f ms, max %.2f ms\n",
		*clients, len(all), errs.Load(), elections, elapsed.Seconds(), res["writes_per_sec"], pct(0.5), pct(0.99), pct(1))
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func maxTerm(addrs []string) int64 {
	var max int64
	for _, a := range addrs {
		resp, err := http.Get("http://" + a + "/status")
		if err != nil {
			continue
		}
		var st struct{ Term int64 }
		json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if st.Term > max {
			max = st.Term
		}
	}
	return max
}
