# raft-kv

A linearizable key-value store replicated with Raft, written in Go with no dependencies outside the standard library (the test harness uses the Porcupine linearizability checker). It is a from-scratch implementation for learning and demonstrating how consensus behaves under failure, with a Jepsen-style fault-injection harness and benchmarks on a 5-node Docker cluster.

## Run it

```sh
docker compose up -d --build                 # 5 nodes, HTTP on localhost:8081-8085
curl -X PUT -d hello localhost:8081/kv/greeting
curl localhost:8081/kv/greeting              # a follower answers 421 with X-Leader set
curl localhost:8081/status

docker compose run --rm bench -clients 256 -duration 20s    # write benchmark, inside the cluster network
go run ./cmd/failover -trials 30                            # kill the leader 30 times, time recovery

go test ./...                                               # unit tests, rule tests, 10 harness runs
go test ./harness -run TestJepsen -args -seeds 300          # 300 randomized fault runs
python3 scripts/mutation_test.py                            # does the harness catch planted bugs?
```

## Results

Measured on 2026-10-20 on one Apple M-series Mac: five node containers and the load generator in a 6-CPU Linux VM (colima, a Docker runtime for macOS), each node with its WAL on its own Docker volume. Raw output is in `results/`.

| Measurement | Result |
|---|---|
| Write throughput, 256 concurrent clients, three 20 s runs | 93,148, 84,412 and 67,083 acknowledged writes/s |
| Write latency in those runs | p50 2.3 to 3.3 ms, p99 6.8 to 9.0 ms, max 88 ms |
| Elections during those runs | 0 |
| Write throughput, 16 clients | 12,345 writes/s, p50 0.97 ms |
| Leader killed with `docker kill`, 30 trials | first committed write 189 to 351 ms after the kill: median 253 ms, p90 295 ms, 27 of 30 under 300 ms |
| Memory per node after 5.6 million writes | 58 to 89 MB (1.2 GB before log compaction was added) |
| Fault-injection harness, 300 seeded runs | 10.4 million client operations, 1,567 elections, 44,196 snapshot installs; every history linearizable, no safety violation |
| Mutation test, 7 planted Raft bugs | harness alone catches 4 or 5 of 7 depending on the run; harness plus rule tests catch 7 of 7 (`results/mutation.txt`) |

A write counts once the leader has it on a majority of disks and has applied it. Throughput varies run to run by about 30% because the VM shares the host with other work. Failover time is dominated by the election timeout, 150 to 300 ms, the range the Raft paper suggests. Lowering it shortens failover at the cost of more spurious elections on a loaded host.

## Design

The Raft core (`raft/`) does leader election with pre-vote, so a node that was partitioned away cannot inflate the term and depose a healthy leader when it returns. Leaders step down when they lose contact with a majority (check-quorum), so clients stop sending them writes that cannot commit. A new leader appends a no-op entry so that entries from earlier terms commit (Raft paper, section 5.4.2). A follower that rejects AppendEntries says where its log diverges, so the leader backs up a whole term per round trip.

For durability and throughput, the leader appends proposals to its write-ahead log without syncing and a single flusher goroutine syncs them in batches (group commit); the leader counts itself toward a commit majority only up to what it has synced. Followers sync before acknowledging. Term and vote are written atomically (temp file, fsync, rename, directory fsync) before any reply that depends on them. Each WAL record carries a CRC32, and a torn record at the tail is cut off on restart.

The KV layer snapshots its map and client-session table every 100,000 entries. The node then starts a new WAL segment and deletes segments the snapshot covers. A follower that has fallen behind the leader's snapshot receives it through InstallSnapshot.

In the KV layer (`kv/`), every operation, reads included, goes through the log, so reads are linearizable without leases. Each client session carries a sequence number, and a retried command returns its first result instead of applying twice.

Nodes talk to each other with net/rpc over TCP, and clients use HTTP.

## Testing

In the fault-injection harness (`harness/`), each run starts five nodes in one process on a simulated network and runs six clients doing gets, puts and appends on three keys. Meanwhile a nemesis (Jepsen's term for the fault injector) picks faults from a seed: random partitions, isolating the leader, leaving the leader with one follower, a bridge topology, one-way links, crashes that lose unsynced writes, crash-and-reboot mid-election, 10% message loss and delays near the heartbeat interval. Batch size and snapshot interval also vary per seed. After the faults the network heals and every node restarts. The harness then checks:

- the client history is linearizable (checked per key with Porcupine, a linearizability checker, where an operation whose outcome the client never learned may take effect at any later time),
- no two nodes lead the same term, and no node votes twice in a term,
- every new leader holds every entry any node has applied,
- every node applies the same command at each index, in order,
- the cluster makes progress within 2 seconds of healing and all nodes converge.

The network is simulated in-process, not a real one cut with iptables as Jepsen does; timing on real sockets is covered by the Docker benchmarks instead.

The rule tests (`raft/rules_test.go`) are three deterministic tests for rules whose violations need timing too precise for random faults to hit reliably: a vote survives a crash, a leader never commits an earlier-term entry by counting replicas (Raft paper, figure 8), and a follower commits only the prefix the leader's RPC vouched for.

The mutation test (`scripts/mutation_test.py`) reintroduces seven classic Raft bugs one at a time into a scratch copy and reports which test layer catches each.

## Bugs found while building it

The partition harness passed the finished Raft core on its first runs. The bugs below came from code review and from running the Docker benchmark.

1. Followers acknowledged entries they had not synced. A follower that already held an entry (written while it was leader, never synced) replied success without an fsync, so the leader could count a replica that a crash would erase. Found in review before the first test run.
2. Election storms after a restart. After all nodes restarted with millions of entries, the new leader committed the whole backlog at once and the apply loop copied it while holding the node lock. Heartbeats stalled past the election timeout and leadership changed repeatedly (the term rose from 7 to 12 within a few seconds). Fixed by applying in batches of 1,024.
3. Elections under sustained load. Without log compaction, each node held every entry in memory, 1.2 GB per node after a few million writes. The likely cause of the stalls is that growing that slice copied hundreds of megabytes under the node lock, but this was not measured directly. A benchmark run saw 4 elections in 20 seconds and a 2.2 s worst-case write. Fixed by snapshots and a segmented WAL. The same runs now show 0 elections.
4. A slow fsync could trigger an election. A follower reset its election timer before syncing, so a sync longer than the timeout made it campaign against a live leader as soon as the sync finished. Fixed by restarting the timer after the sync.

The harness itself was too weak at first: it caught 3 of the 7 planted bugs. Adding vote and leader-completeness checks, crash-and-reboot faults, slow links, and small replication batches raised that to 4 or 5, and the three rule tests cover the rest.

## Limitations

- Membership is fixed at startup; there is no joint-consensus reconfiguration.
- Snapshots are sent in one RPC, not in chunks, which is fine for the benchmark's 100,000 keys but not for large state.
- Reads go through the log. ReadIndex or leader leases would make them cheaper.
- The Docker numbers come from one machine, so network latency between nodes is far below a real deployment's. The disk is the VM's virtual disk, and an fsync inside the VM may return before the data reaches the Mac's physical disk. For a conservative number: the same five nodes run as plain processes on macOS, where Go's fsync forces data to the physical disk (F_FULLFSYNC), sustained 11,838 acknowledged writes/s with 256 clients (p50 21 ms, p99 32 ms) in an 8-second run.
