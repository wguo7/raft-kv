// Package raft implements the Raft consensus protocol: leader election with
// pre-vote, log replication with batched group commit, and durable state.
package raft

import (
	"context"
	"time"
)

// Entry is one slot of the replicated log. Data == nil marks the no-op entry a
// new leader appends to commit entries from earlier terms.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// ApplyMsg delivers a committed entry to the state machine, in index order.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
	// PreVote asks "would you vote for me in Term?" without changing any state
	// on the receiver. A node only starts a real election after winning a
	// pre-vote, so a node that was cut off cannot inflate terms and depose a
	// healthy leader when it rejoins.
	PreVote bool
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// On failure the follower reports where its log diverges so the leader can
	// skip back a whole term at a time instead of one index per round trip.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport sends RPCs to peers. Implementations must be safe for concurrent
// use and must honor ctx cancellation.
type Transport interface {
	RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Storage persists term, vote and log. Append and TruncateFrom may buffer;
// nothing is durable until Sync returns. SetState must be durable on return.
type Storage interface {
	Load() (term uint64, votedFor int, entries []Entry, err error)
	SetState(term uint64, votedFor int) error
	Append(entries []Entry) error
	TruncateFrom(index uint64) error
	Sync() error
	Close() error
}

type Config struct {
	ID    int
	Peers []int // every member, including ID

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
	MaxBatch           int // max entries per AppendEntries

	Storage   Storage
	Transport Transport
	ApplyCh   chan<- ApplyMsg

	// Observer, if set, is told about role changes (used by the test harness
	// to check election safety). It is called with the node lock held and must
	// not call back into the node.
	Observer func(Event)
	Logf     func(format string, args ...any)
}

type EventKind int

const (
	EventBecameLeader EventKind = iota
	EventBecameFollower
	EventBecameCandidate
)

type Event struct {
	Kind EventKind
	Node int
	Term uint64
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeoutMin
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = 4096
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}
