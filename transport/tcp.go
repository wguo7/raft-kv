// Package transport carries Raft RPCs between processes over TCP using
// net/rpc (gob encoding, one multiplexed connection per peer).
package transport

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"

	"github.com/wguo7/raft-kv/raft"
)

// Serve answers Raft RPCs on l until l is closed.
func Serve(l net.Listener, h raft.Handler) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &service{h}); err != nil {
		return err
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go srv.ServeConn(conn)
	}
}

type service struct{ h raft.Handler }

func (s *service) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r, err := s.h.HandleRequestVote(args)
	if err != nil {
		return err
	}
	*reply = *r
	return nil
}

func (s *service) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r, err := s.h.HandleAppendEntries(args)
	if err != nil {
		return err
	}
	*reply = *r
	return nil
}

func (s *service) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r, err := s.h.HandleInstallSnapshot(args)
	if err != nil {
		return err
	}
	*reply = *r
	return nil
}

// TCP is a raft.Transport that dials peers lazily and redials after a
// connection error or timeout.
type TCP struct {
	addrs map[int]string
	mu    sync.Mutex
	conns map[int]*rpc.Client
}

func NewTCP(addrs map[int]string) *TCP {
	return &TCP{addrs: addrs, conns: map[int]*rpc.Client{}}
}

func (t *TCP) client(ctx context.Context, to int) (*rpc.Client, error) {
	t.mu.Lock()
	c := t.conns[to]
	t.mu.Unlock()
	if c != nil {
		return c, nil
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", t.addrs[to])
	if err != nil {
		return nil, err
	}
	conn.(*net.TCPConn).SetNoDelay(true)
	c = rpc.NewClient(conn)
	t.mu.Lock()
	if old := t.conns[to]; old != nil {
		t.mu.Unlock()
		c.Close()
		return old, nil
	}
	t.conns[to] = c
	t.mu.Unlock()
	return c, nil
}

func (t *TCP) drop(to int, c *rpc.Client) {
	t.mu.Lock()
	if t.conns[to] == c {
		delete(t.conns, to)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *TCP) call(ctx context.Context, to int, method string, args, reply any) error {
	c, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		var se rpc.ServerError
		if call.Error != nil && !errors.As(call.Error, &se) {
			t.drop(to, c) // broken connection; redial next time
		}
		return call.Error
	case <-ctx.Done():
		// A peer that stops answering (crashed host, dropped packets) may
		// leave the connection open; start fresh next time.
		t.drop(to, c)
		return ctx.Err()
	}
}

func (t *TCP) RequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var r raft.RequestVoteReply
	return &r, t.call(ctx, to, "Raft.RequestVote", args, &r)
}

func (t *TCP) AppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var r raft.AppendEntriesReply
	return &r, t.call(ctx, to, "Raft.AppendEntries", args, &r)
}

func (t *TCP) InstallSnapshot(ctx context.Context, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var r raft.InstallSnapshotReply
	return &r, t.call(ctx, to, "Raft.InstallSnapshot", args, &r)
}
