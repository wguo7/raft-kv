package raft

import (
	"context"
	"testing"
	"time"
)

// Deterministic checks of single Raft rules whose violations need timing too
// precise for the randomized harness to hit reliably.

type silentTransport struct{}

func (silentTransport) RequestVote(ctx context.Context, _ int, _ *RequestVoteArgs) (*RequestVoteReply, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (silentTransport) AppendEntries(ctx context.Context, _ int, _ *AppendEntriesArgs) (*AppendEntriesReply, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (silentTransport) InstallSnapshot(ctx context.Context, _ int, _ *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// quietNode starts node 1 of {1,2,3} with timeouts long enough that it never
// campaigns during the test.
func quietNode(t *testing.T, st *MemStorage) *Node {
	t.Helper()
	n, err := New(Config{
		ID: 1, Peers: []int{1, 2, 3},
		HeartbeatInterval:  time.Hour,
		ElectionTimeoutMin: time.Hour,
		ElectionTimeoutMax: 2 * time.Hour,
		Storage:            st,
		Transport:          silentTransport{},
		ApplyCh:            make(chan ApplyMsg, 16),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

func TestVoteSurvivesCrash(t *testing.T) {
	st := NewMemStorage()
	n := quietNode(t, st)
	r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: 2})
	if !r.VoteGranted {
		t.Fatal("first vote in term 5 not granted")
	}
	n.Stop()
	st.Crash()
	n = quietNode(t, st)
	r, _ = n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: 3})
	if r.VoteGranted {
		t.Fatal("voted for a second candidate in term 5 after a restart")
	}
}

// Raft paper figure 8: a leader must not commit an entry from an earlier term
// just because a majority stores it.
func TestLeaderDoesNotCommitOldTermByCount(t *testing.T) {
	st := NewMemStorage()
	st.SetState(4, 1)
	st.Append([]Entry{{Index: 1, Term: 2, Data: []byte("old")}})
	st.Sync()
	n := quietNode(t, st)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.becomeLeader() // appends a no-op at index 2, term 4
	n.durableIndex = 1
	n.matchIndex[2] = 1
	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed index %d (term 2) by counting replicas in term 4", n.commitIndex)
	}
	n.durableIndex, n.matchIndex[2] = 2, 2
	n.advanceCommit()
	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d after the term-4 no-op reached a majority, want 2", n.commitIndex)
	}
}

// A follower may only commit up to the last entry the leader's RPC vouched
// for; entries past it may be stale leftovers from an old leader.
func TestFollowerCommitsOnlyVerifiedPrefix(t *testing.T) {
	st := NewMemStorage()
	st.SetState(1, noVote)
	st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}})
	st.Sync()
	n := quietNode(t, st)
	r, err := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3,
	})
	if err != nil || !r.Success {
		t.Fatalf("heartbeat rejected: %+v %v", r, err)
	}
	if got := n.Status().CommitIndex; got != 1 {
		t.Fatalf("commitIndex = %d, want 1 (entries 2-3 were not verified)", got)
	}
}
