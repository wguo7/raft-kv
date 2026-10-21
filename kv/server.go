// Package kv is a linearizable key-value state machine replicated with Raft.
// Every operation, reads included, goes through the log; per-client sequence
// numbers make retried writes apply exactly once.
package kv

import (
	"context"
	"encoding/binary"
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
	term   uint64
	result Result
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
}

// NewServer consumes applyCh (the channel given to the raft node) until it is
// closed or Close is called.
func NewServer(applyCh <-chan raft.ApplyMsg) *Server {
	s := &Server{
		data:     map[string]string{},
		sessions: map[uint64]session{},
		waiters:  map[uint64]waiter{},
		done:     make(chan struct{}),
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
