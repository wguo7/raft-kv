package raft

import "sync"

// MemStorage keeps state in memory and models fsync: Crash discards every
// Append and TruncateFrom since the last Sync, the way a power loss would.
type MemStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor int
	log      []Entry // as written
	synced   []Entry // as of the last Sync
}

func NewMemStorage() *MemStorage { return &MemStorage{votedFor: noVote} }

func (m *MemStorage) Load() (uint64, int, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, append([]Entry(nil), m.log...), nil
}

func (m *MemStorage) SetState(term uint64, votedFor int) error {
	m.mu.Lock()
	m.term, m.votedFor = term, votedFor
	m.mu.Unlock()
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
	if index >= 1 && int(index-1) < len(m.log) {
		m.log = m.log[: index-1 : index-1]
	}
	return nil
}

func (m *MemStorage) Sync() error {
	m.mu.Lock()
	m.synced = append([]Entry(nil), m.log...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) Close() error { return nil }

// Crash drops unsynced log writes. Term and vote are always durable.
func (m *MemStorage) Crash() {
	m.mu.Lock()
	m.log = append([]Entry(nil), m.synced...)
	m.mu.Unlock()
}
