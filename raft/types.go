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
// When Snapshot is non-nil the message instead carries a snapshot covering
// everything through Index; the state machine replaces its state with it.
type ApplyMsg struct {
	Index    uint64
	Term     uint64
	Data     []byte
	Snapshot []byte
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

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport sends RPCs to peers. Implementations must be safe for concurrent
// use and must honor ctx cancellation.
type Transport interface {
	RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// PersistentState is what a node recovers from storage on start.
type PersistentState struct {
	Term      uint64
	VotedFor  int
	SnapIndex uint64 // last index covered by Snapshot (0 if none)
	SnapTerm  uint64
	Snapshot  []byte
	Entries   []Entry // entries after SnapIndex, in order
}

// Storage persists term, vote, snapshot and log. Append and TruncateFrom may
// buffer; nothing is durable until Sync returns. SetState and SaveSnapshot
// must be durable on return. SaveSnapshot may discard entries through index
// and must ignore a snapshot older than the one it has.
type Storage interface {
	Load() (PersistentState, error)
	SaveSnapshot(index, term uint64, data []byte) error
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
	EventVoted // Node granted its vote in Term to Candidate
)

type Event struct {
	Kind      EventKind
	Node      int
	Term      uint64
	Candidate int
	// SnapshotIndex is the last index compacted into the node's snapshot;
	// EntryAt cannot see entries at or below it.
	SnapshotIndex uint64
	// EntryAt reads the node's log. It is only valid during the callback.
	EntryAt func(index uint64) (Entry, bool)
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
