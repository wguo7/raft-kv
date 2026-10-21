// kvnode runs one member of a Raft KV cluster.
//
//	kvnode -id 1 -cluster 1=node1:7000:8080,2=node2:7000:8080,3=node3:7000:8080 -data /data
//
// Each cluster entry is id=host:raftPort:httpPort.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wguo7/raft-kv/kv"
	"github.com/wguo7/raft-kv/raft"
	"github.com/wguo7/raft-kv/transport"
)

func main() {
	id := flag.Int("id", 0, "this node's id")
	cluster := flag.String("cluster", "", "comma-separated id=host:raftPort:httpPort")
	dataDir := flag.String("data", "data", "directory for the WAL and state file")
	heartbeat := flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
	elMin := flag.Duration("election-min", 150*time.Millisecond, "minimum election timeout")
	elMax := flag.Duration("election-max", 300*time.Millisecond, "maximum election timeout")
	timeout := flag.Duration("request-timeout", time.Second, "max wait for a client command to commit")
	snapEvery := flag.Uint64("snapshot-every", 100000, "take a snapshot and compact the log every N entries (0 = never)")
	verbose := flag.Bool("v", false, "log raft events")
	flag.Parse()

	raftAddrs, httpAddrs, err := parseCluster(*cluster)
	if err != nil {
		log.Fatal(err)
	}
	if _, ok := raftAddrs[*id]; !ok {
		log.Fatalf("id %d is not in -cluster", *id)
	}
	var peers []int
	for p := range raftAddrs {
		peers = append(peers, p)
	}

	store, err := raft.OpenFileStorage(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	applyCh := make(chan raft.ApplyMsg, 4096)
	srv := kv.NewServer(applyCh, *snapEvery)
	cfg := raft.Config{
		ID: *id, Peers: peers,
		HeartbeatInterval:  *heartbeat,
		ElectionTimeoutMin: *elMin,
		ElectionTimeoutMax: *elMax,
		Storage:            store,
		Transport:          transport.NewTCP(raftAddrs),
		ApplyCh:            applyCh,
	}
	if *verbose {
		cfg.Logf = log.Printf
	}
	node, err := raft.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	srv.SetNode(node)

	_, raftPort, _ := net.SplitHostPort(raftAddrs[*id])
	l, err := net.Listen("tcp", ":"+raftPort)
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(transport.Serve(l, node)) }()

	_, httpPort, _ := net.SplitHostPort(httpAddrs[*id])
	log.Printf("node %d: raft on :%s, http on :%s, %d peers", *id, raftPort, httpPort, len(peers))
	h := &kv.HTTPHandler{Server: srv, LeaderAddrs: httpAddrs, Timeout: *timeout}
	log.Fatal(http.ListenAndServe(":"+httpPort, h))
}

func parseCluster(s string) (raftAddrs, httpAddrs map[int]string, err error) {
	raftAddrs, httpAddrs = map[int]string{}, map[int]string{}
	for _, part := range strings.Split(s, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		hp := strings.Split(addr, ":")
		if !ok || len(hp) != 3 {
			return nil, nil, fmt.Errorf("bad cluster entry %q, want id=host:raftPort:httpPort", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, nil, err
		}
		raftAddrs[id] = hp[0] + ":" + hp[1]
		httpAddrs[id] = hp[0] + ":" + hp[2]
	}
	return raftAddrs, httpAddrs, nil
}
