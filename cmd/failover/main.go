// failover measures how long writes stall when the leader dies. Each trial
// finds the leader, keeps one client writing back to back, SIGKILLs the
// leader's container, and records the time from the kill to the first write
// acknowledged by the new leader. The killed node is then restarted and
// allowed to catch up before the next trial.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wguo7/raft-kv/kv"
)

type status struct {
	ID       int    `json:"id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Leader   int    `json:"leader"`
	IsLeader bool   `json:"is_leader"`
}

func main() {
	nodes := flag.String("nodes", "localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085", "HTTP address of node 1..n")
	containers := flag.String("containers", "raft-kv-node1-1,raft-kv-node2-1,raft-kv-node3-1,raft-kv-node4-1,raft-kv-node5-1", "container of node 1..n")
	trials := flag.Int("trials", 20, "number of leader kills")
	out := flag.String("json", "", "write per-trial results to this file")
	flag.Parse()
	addrs := strings.Split(*nodes, ",")
	ctrs := strings.Split(*containers, ",")
	hc := &http.Client{Timeout: 200 * time.Millisecond}

	var restores, gaps []float64
	for t := 1; t <= *trials; t++ {
		leader := waitHealthy(hc, addrs)
		restore, gap := trial(addrs, ctrs[leader-1])
		restores = append(restores, restore)
		gaps = append(gaps, gap)
		log.Printf("trial %2d: killed node %d, first write after kill %.0f ms (client-visible gap %.0f ms)", t, leader, restore, gap)
		if err := exec.Command("docker", "start", ctrs[leader-1]).Run(); err != nil {
			log.Fatalf("restart %s: %v", ctrs[leader-1], err)
		}
	}
	summary := func(name string, v []float64) map[string]float64 {
		s := append([]float64(nil), v...)
		sort.Float64s(s)
		under := 0
		for _, x := range s {
			if x < 300 {
				under++
			}
		}
		m := map[string]float64{"min": s[0], "p50": s[len(s)/2], "p90": s[int(0.9*float64(len(s)-1))], "max": s[len(s)-1],
			"under_300ms": float64(under), "trials": float64(len(s))}
		fmt.Printf("%s: min %.0f, median %.0f, p90 %.0f, max %.0f ms; %d of %d under 300 ms\n",
			name, m["min"], m["p50"], m["p90"], m["max"], under, len(s))
		return m
	}
	res := map[string]any{
		"restore_ms": restores, "gap_ms": gaps,
		"restore_summary": summary("kill -> first committed write", restores),
		"gap_summary":     summary("last write before kill -> first write after", gaps),
	}
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		os.WriteFile(*out, b, 0o644)
	}
}

// waitHealthy blocks until every node answers, all agree on one leader, and a
// write succeeds. It returns the leader's id.
func waitHealthy(hc *http.Client, addrs []string) int {
	c := kv.NewClient(addrs, nil)
	for {
		leaders := map[int]bool{}
		ok := true
		for _, a := range addrs {
			var s status
			resp, err := hc.Get("http://" + a + "/status")
			if err != nil {
				ok = false
				break
			}
			json.NewDecoder(resp.Body).Decode(&s)
			resp.Body.Close()
			leaders[s.Leader] = true
		}
		if ok && len(leaders) == 1 && !leaders[0] && !leaders[-1] {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := c.Put(ctx, "health", "ok")
			cancel()
			if err == nil {
				for l := range leaders {
					time.Sleep(500 * time.Millisecond) // let the restarted node finish catching up
					return l
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func trial(addrs []string, container string) (restoreMs, gapMs float64) {
	c := kv.NewClient(addrs, nil)
	c.Attempt = 100 * time.Millisecond
	var mu sync.Mutex
	var acks []time.Time
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.Put(ctx, "failover", fmt.Sprint(i))
			cancel()
			if err == nil {
				mu.Lock()
				acks = append(acks, time.Now())
				mu.Unlock()
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)

	killStart := time.Now()
	if err := exec.Command("docker", "kill", container).Run(); err != nil {
		log.Fatalf("kill %s: %v", container, err)
	}
	killDone := time.Now()

	deadline := time.Now().Add(10 * time.Second)
	var first, lastBefore time.Time
	for first.IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		for _, a := range acks {
			// Only acks after docker kill returned can come from the new
			// leader; timing starts from before the kill was issued.
			if a.After(killDone) {
				first = a
				break
			}
			if a.Before(killStart) {
				lastBefore = a
			}
		}
		mu.Unlock()
	}
	close(stop)
	<-done
	if first.IsZero() {
		log.Fatal("no write succeeded within 10s of the kill")
	}
	return ms(first.Sub(killStart)), ms(first.Sub(lastBefore))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
