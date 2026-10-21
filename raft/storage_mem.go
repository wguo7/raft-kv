package raft

import (
	"sort"
	"sync"
)

// MemStorage keeps state in memory and models fsync: Crash discards every
// Append and TruncateFrom since the last Sync, the way a power loss would.
// Term, vote and snapshots are durable as soon as they are written.
type MemStorage struct {
	mu        sync.Mutex
	term      uint64
	votedFor  int
	snapIndex uint64
	snapTerm  uint64
	snapshot  []byte
	log       []Entry // as written (indexes > snapIndex, ascending)
	synced    []Entry // as of the last Sync
}

func NewMemStorage() *MemStorage { return &MemStorage{votedFor: noVote} }

func (m *MemStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return PersistentState{
		Term: m.term, VotedFor: m.votedFor,
		SnapIndex: m.snapIndex, SnapTerm: m.snapTerm, Snapshot: m.snapshot,
		Entries: after(m.log, m.snapIndex),
	}, nil
}

// after returns a copy of the entries with Index > i.
func after(log []Entry, i uint64) []Entry {
	k := sort.Search(len(log), func(j int) bool { return log[j].Index > i })
	return append([]Entry(nil), log[k:]...)
}

func (m *MemStorage) SetState(term uint64, votedFor int) error {
	m.mu.Lock()
	m.term, m.votedFor = term, votedFor
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) SaveSnapshot(index, term uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.snapIndex {
		return nil
	}
	m.snapIndex, m.snapTerm, m.snapshot = index, term, data
	m.log = after(m.log, index)
	m.synced = after(m.synced, index)
	return nil
}

func (m *MemStorage) Append(entries []Entry) error {
	m.mu.Lock()
	m.log = append(m.log, entries...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := sort.Search(len(m.log), func(j int) bool { return m.log[j].Index >= index })
	m.log = m.log[:k:k]
	return nil
}

func (m *MemStorage) Sync() error {
	m.mu.Lock()
	m.synced = append([]Entry(nil), m.log...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) Close() error { return nil }

// Crash drops unsynced log writes.
func (m *MemStorage) Crash() {
	m.mu.Lock()
	m.log = after(m.synced, m.snapIndex)
	m.mu.Unlock()
}
