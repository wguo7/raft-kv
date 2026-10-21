package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// MemNetwork connects in-process nodes and lets tests cut links, drop
// messages and add latency. Links are directional: Block(a, b) drops every
// message a sends to b (a's requests and a's replies) but not b to a.
type MemNetwork struct {
	mu       sync.Mutex
	handlers map[int]Handler
	blocked  map[[2]int]bool
	dropRate float64 // chance a request or a reply is lost
	maxDelay time.Duration
	rng      *rand.Rand
}

// Handler is the receive side of a node (implemented by *Node).
type Handler interface {
	HandleRequestVote(*RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(*AppendEntriesArgs) (*AppendEntriesReply, error)
}

var ErrUnreachable = errors.New("memnet: unreachable")

func NewMemNetwork(seed int64) *MemNetwork {
	return &MemNetwork{
		handlers: map[int]Handler{},
		blocked:  map[[2]int]bool{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (m *MemNetwork) Register(id int, h Handler) {
	m.mu.Lock()
	m.handlers[id] = h
	m.mu.Unlock()
}

// Unregister makes id unreachable, as if its process died.
func (m *MemNetwork) Unregister(id int) {
	m.mu.Lock()
	delete(m.handlers, id)
	m.mu.Unlock()
}

func (m *MemNetwork) SetUnreliable(dropRate float64, maxDelay time.Duration) {
	m.mu.Lock()
	m.dropRate, m.maxDelay = dropRate, maxDelay
	m.mu.Unlock()
}

// Partition allows traffic only within each group.
func (m *MemNetwork) Partition(groups ...[]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	group := map[int]int{}
	var all []int
	for g, ids := range groups {
		for _, id := range ids {
			group[id] = g
			all = append(all, id)
		}
	}
	m.blocked = map[[2]int]bool{}
	for _, a := range all {
		for _, b := range all {
			if group[a] != group[b] {
				m.blocked[[2]int{a, b}] = true
			}
		}
	}
}

// Block cuts the a->b direction only (asymmetric partition).
func (m *MemNetwork) Block(a, b int) {
	m.mu.Lock()
	m.blocked[[2]int{a, b}] = true
	m.mu.Unlock()
}

func (m *MemNetwork) Heal() {
	m.mu.Lock()
	m.blocked = map[[2]int]bool{}
	m.mu.Unlock()
}

// Endpoint returns the Transport node `from` uses to reach its peers.
func (m *MemNetwork) Endpoint(from int) Transport { return &memEndpoint{m, from} }

type memEndpoint struct {
	net  *MemNetwork
	from int
}

// route decides the fate of one message: delay, whether it is delivered, and
// the receiving handler.
func (m *MemNetwork) route(from, to int) (time.Duration, bool, Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var d time.Duration
	if m.maxDelay > 0 {
		d = time.Duration(m.rng.Int63n(int64(m.maxDelay)))
	}
	h, ok := m.handlers[to]
	lost := !ok || m.blocked[[2]int{from, to}] || m.rng.Float64() < m.dropRate
	return d, !lost, h
}

func wait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func call[A, R any](ctx context.Context, e *memEndpoint, to int, args A, fn func(Handler, A) (R, error)) (R, error) {
	var zero R
	d, ok, h := e.net.route(e.from, to)
	if !ok {
		// A lost message looks like silence: the caller waits for its timeout.
		<-ctx.Done()
		return zero, ErrUnreachable
	}
	if err := wait(ctx, d); err != nil {
		return zero, err
	}
	reply, err := fn(h, args)
	if err != nil {
		return zero, err
	}
	d, ok, _ = e.net.route(to, e.from)
	if !ok {
		<-ctx.Done()
		return zero, ErrUnreachable
	}
	if err := wait(ctx, d); err != nil {
		return zero, err
	}
	return reply, nil
}

func (e *memEndpoint) RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return call(ctx, e, to, args, Handler.HandleRequestVote)
}

func (e *memEndpoint) AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	// The receiver keeps the slice it is given; copy so sender and receiver
	// never share a backing array (a real network would serialize).
	cp := *args
	cp.Entries = append([]Entry(nil), args.Entries...)
	return call(ctx, e, to, &cp, Handler.HandleAppendEntries)
}
