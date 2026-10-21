#!/usr/bin/env python3
"""Check that the fault-injection harness catches known Raft bugs.

Each mutant reintroduces one classic Raft bug into a scratch copy of the repo,
then runs the randomized harness on it. A mutant is "caught" when any seed
fails (non-linearizable history, two leaders in a term, divergent logs, or no
progress after healing). The real code is never modified.

    python3 scripts/mutation_test.py [--seeds 40]
"""
import argparse
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent

MUTANTS = [
    ("vote-ignores-log-freshness",
     "Election restriction dropped: a candidate with a stale log can win.",
     "raft/raft.go",
     "\tupToDate := args.LastLogTerm > n.lastTerm() ||\n\t\t(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())",
     "\tupToDate := true"),
    ("double-vote",
     "A node may vote for two candidates in the same term.",
     "raft/raft.go",
     "if (n.votedFor == noVote || n.votedFor == args.CandidateID) && upToDate {",
     "if upToDate {"),
    ("vote-not-persisted",
     "Term and vote are lost on crash, so a restarted node can vote again.",
     "raft/storage_mem.go",
     "\tm.log = after(m.synced, m.snapIndex)\n",
     "\tm.log = after(m.synced, m.snapIndex)\n\tm.term, m.votedFor = 0, noVote\n"),
    ("commit-old-term-by-count",
     "Leader commits entries from earlier terms by counting replicas (Raft fig. 8).",
     "raft/raft.go",
     "if candidate > n.commitIndex && n.termAt(candidate) == n.term {",
     "if candidate > n.commitIndex {"),
    ("follower-commit-past-verified",
     "Follower advances commitIndex past the entries the leader vouched for.",
     "raft/raft.go",
     "\t\tif lastNew < c {\n\t\t\tc = lastNew\n\t\t}",
     "\t\tif n.lastIndex() < c {\n\t\t\tc = n.lastIndex()\n\t\t}"),
    ("ack-before-fsync",
     "Follower acknowledges entries without syncing them to disk.",
     "raft/raft.go",
     "\tif changed || lastNew > n.durableIndex {\n\t\tif err := n.cfg.Storage.Sync(); err != nil {\n\t\t\treturn nil, err\n\t\t}",
     "\tif changed || lastNew > n.durableIndex {\n\t\tif false {\n\t\t\treturn nil, nil\n\t\t}"),
    ("no-dedup",
     "KV layer applies retried commands twice.",
     "kv/server.go",
     "if cmd.Seq != 0 && cmd.Seq <= sess.seq {",
     "if false {"),
]


def run(seeds: int, run_time: str) -> int:
    rows = []
    for name, desc, path, old, new in MUTANTS:
        with tempfile.TemporaryDirectory() as tmp:
            dst = pathlib.Path(tmp) / "repo"
            shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns(".git", "results", "bin"))
            f = dst / path
            src = f.read_text()
            if src.count(old) != 1:
                print(f"{name}: anchor not found in {path}", file=sys.stderr)
                return 2
            f.write_text(src.replace(old, new))
            t0 = time.time()
            p = subprocess.run(
                ["go", "test", "-count=1", "-run", "TestJepsen", "./harness",
                 "-args", "-seeds", str(seeds), "-fault-time", run_time],
                cwd=dst, capture_output=True, text=True)
            out = p.stdout + p.stderr
            failed_seeds = out.count("--- FAIL: TestJepsen/seed=")
            caught = p.returncode != 0
            kind = ""
            for marker in ("not linearizable", "election safety", "state machine safety",
                           "no progress", "did not converge", "panic"):
                if marker in out:
                    kind = marker
                    break
            rows.append((name, desc, caught, failed_seeds, seeds, kind, time.time() - t0))
            print(f"{name:32s} {'CAUGHT' if caught else 'missed':7s} "
                  f"{failed_seeds:3d}/{seeds} seeds failed  first signal: {kind or '-'}", flush=True)
    caught = sum(r[2] for r in rows)
    print(f"\n{caught}/{len(rows)} mutants caught")
    return 0 if caught == len(rows) else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=40)
    ap.add_argument("--run", default="3s", help="fault-injection time per seed")
    a = ap.parse_args()
    sys.exit(run(a.seeds, a.run))
