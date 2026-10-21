// Package kv is a linearizable key-value state machine replicated with Raft.
// Every operation, reads included, goes through the log; per-client sequence
// numbers make retried writes apply exactly once.
package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/wguo7/raft-kv/raft"
)

type OpType uint8

const (
	OpGet OpType = iota + 1
	OpPut
	OpAppend
)

type Command struct {
	Type     OpType
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

type Result struct {
	Value string
	Found bool
}

var (
	ErrNotLeader      = errors.New("kv: not leader")
	ErrLeadershipLost = errors.New("kv: leadership changed before commit; retry")
	ErrTimeout        = errors.New("kv: timed out waiting for commit")
)

func (c *Command) Encode() []byte {
	b := make([]byte, 0, 1+8+8+4+len(c.Key)+4+len(c.Value))
	b = append(b, byte(c.Type))
	b = binary.LittleEndian.AppendUint64(b, c.ClientID)
	b = binary.LittleEndian.AppendUint64(b, c.Seq)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Value)))
	return append(b, c.Value...)
}

func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if len(b) < 21 {
		return c, fmt.Errorf("kv: short command (%d bytes)", len(b))
	}
	c.Type = OpType(b[0])
	c.ClientID = binary.LittleEndian.Uint64(b[1:])
	c.Seq = binary.LittleEndian.Uint64(b[9:])
	kl := int(binary.LittleEndian.Uint32(b[17:]))
	b = b[21:]
	if len(b) < kl+4 {
		return c, errors.New("kv: truncated key")
	}
	c.Key = string(b[:kl])
	vl := int(binary.LittleEndian.Uint32(b[kl:]))
	b = b[kl+4:]
	if len(b) != vl {
		return c, errors.New("kv: truncated value")
	}
	c.Value = string(b)
	return c, nil
}

type session struct {
	seq    uint64
	result Result
}

type applied struct {
	term    uint64
	result  Result
	unknown bool // replaced by a snapshot: may or may not have applied
}

type waiter struct {
	term uint64
	ch   chan applied
}

// Server applies committed commands and lets callers wait for their own.
type Server struct {
	node *raft.Node

	mu          sync.Mutex
	data        map[string]string
	sessions    map[uint64]session
	waiters     map[uint64]waiter
	lastApplied uint64
	done        chan struct{}

	snapshotEvery uint64 // entries between snapshots; 0 disables them
	lastSnapshot  uint64
}

// NewServer consumes applyCh (the channel given to the raft node) until it is
// closed or Close is called. Every snapshotEvery applied entries it hands the
// node a snapshot so the log can be compacted (0 disables snapshots).
func NewServer(applyCh <-chan raft.ApplyMsg, snapshotEvery uint64) *Server {
	s := &Server{
		snapshotEvery: snapshotEvery,
		data:          map[string]string{},
		sessions:      map[uint64]session{},
		waiters:       map[uint64]waiter{},
		done:          make(chan struct{}),
	}
	go s.applyLoop(applyCh)
	return s
}

// SetNode attaches the raft node; it is separate from NewServer because the
// node needs the apply channel before it exists.
func (s *Server) SetNode(n *raft.Node) { s.node = n }

func (s *Server) Node() *raft.Node { return s.node }

func (s *Server) Close() { close(s.done) }

// Submit replicates cmd and returns its result once it is applied. A returned
// error other than ErrTimeout means the command did not commit through this
// server; with ErrTimeout the outcome is unknown and the caller should retry
// with the same (ClientID, Seq).
func (s *Server) Submit(ctx context.Context, cmd Command) (Result, error) {
	s.mu.Lock()
	idx, term, err := s.node.Propose(cmd.Encode())
	if err != nil {
		s.mu.Unlock()
		return Result{}, ErrNotLeader
	}
	ch := make(chan applied, 1)
	s.waiters[idx] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	select {
	case a := <-ch:
		if a.unknown {
			return Result{}, ErrTimeout
		}
		if a.term != term {
			return Result{}, ErrLeadershipLost
		}
		return a.result, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, idx)
		s.mu.Unlock()
		return Result{}, ErrTimeout
	}
}

func (s *Server) applyLoop(applyCh <-chan raft.ApplyMsg) {
	for {
		select {
		case <-s.done:
			return
		case m, ok := <-applyCh:
			if !ok {
				return
			}
			s.apply(m)
		}
	}
}

func (s *Server) apply(m raft.ApplyMsg) {
	if m.Snapshot != nil {
		s.restore(m)
		return
	}
	s.mu.Lock()
	s.lastApplied = m.Index
	var res Result
	if m.Data != nil {
		cmd, err := DecodeCommand(m.Data)
		if err != nil {
			panic(err) // a committed entry we cannot read means divergence
		}
		res = s.execute(cmd)
	}
	if w, ok := s.waiters[m.Index]; ok {
		delete(s.waiters, m.Index)
		w.ch <- applied{term: m.Term, result: res}
	}
	var snap []byte
	if s.snapshotEvery > 0 && m.Index-s.lastSnapshot >= s.snapshotEvery {
		snap = s.encodeState()
		s.lastSnapshot = m.Index
	}
	s.mu.Unlock()
	if snap != nil {
		// Outside s.mu: the node writes the snapshot to disk. State cannot
		// change meanwhile because only this goroutine applies entries.
		if err := s.node.Snapshot(m.Index, snap); err != nil {
			panic(fmt.Sprintf("kv: save snapshot: %v", err))
		}
	}
}

// snapState is the serialized form of the state machine.
type snapState struct {
	Data    map[string]string
	Seqs    map[uint64]uint64
	Results map[uint64]Result
}

func (s *Server) encodeState() []byte {
	st := snapState{Data: s.data, Seqs: map[uint64]uint64{}, Results: map[uint64]Result{}}
	for id, sess := range s.sessions {
		st.Seqs[id], st.Results[id] = sess.seq, sess.result
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// restore replaces the state with a snapshot from the leader or from disk.
func (s *Server) restore(m raft.ApplyMsg) {
	var st snapState
	if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&st); err != nil {
		panic(fmt.Sprintf("kv: bad snapshot: %v", err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = st.Data
	if s.data == nil {
		s.data = map[string]string{}
	}
	s.sessions = map[uint64]session{}
	for id, seq := range st.Seqs {
		s.sessions[id] = session{seq: seq, result: st.Results[id]}
	}
	s.lastApplied, s.lastSnapshot = m.Index, m.Index
	// Commands waiting on indexes the snapshot covers may or may not be in
	// it; their clients must retry (the session table dedups the retry).
	for idx, w := range s.waiters {
		if idx <= m.Index {
			delete(s.waiters, idx)
			w.ch <- applied{unknown: true}
		}
	}
}

func (s *Server) execute(cmd Command) Result {
	sess := s.sessions[cmd.ClientID]
	if cmd.Seq != 0 && cmd.Seq <= sess.seq {
		// A retry of a command that already applied: return the first result
		// instead of applying it twice.
		return sess.result
	}
	var res Result
	switch cmd.Type {
	case OpGet:
		res.Value, res.Found = s.data[cmd.Key]
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		res.Found = true
	case OpAppend:
		s.data[cmd.Key] += cmd.Value
		res.Value, res.Found = s.data[cmd.Key], true
	}
	if cmd.Seq != 0 {
		s.sessions[cmd.ClientID] = session{seq: cmd.Seq, result: res}
	}
	return res
}

// LastApplied reports the index of the last applied entry (for tests).
func (s *Server) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}
