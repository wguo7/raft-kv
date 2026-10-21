package kv

import (
	"testing"

	"github.com/wguo7/raft-kv/raft"
)

func TestCommandRoundTrip(t *testing.T) {
	in := Command{Type: OpAppend, Key: "k\x00ey", Value: "", ClientID: 42, Seq: 7}
	out, err := DecodeCommand(in.Encode())
	if err != nil || out != in {
		t.Fatalf("got %+v, %v", out, err)
	}
	if _, err := DecodeCommand(in.Encode()[:10]); err == nil {
		t.Fatal("short command decoded")
	}
}

func TestRetriedCommandAppliesOnce(t *testing.T) {
	s := &Server{data: map[string]string{}, sessions: map[uint64]session{}}
	defer func() {
		// The session table survives a snapshot round trip.
		var t2 Server
		t2.waiters = map[uint64]waiter{}
		t2.restore(raft.ApplyMsg{Index: 9, Snapshot: s.encodeState()})
		if r := t2.execute(Command{Type: OpAppend, Key: "x", Value: "b", ClientID: 1, Seq: 2}); r.Value != "ab" {
			t.Errorf("after restore, retried seq 2 returned %q, want cached %q", r.Value, "ab")
		}
	}()
	a := Command{Type: OpAppend, Key: "x", Value: "a", ClientID: 1, Seq: 1}
	s.execute(a)
	if r := s.execute(a); r.Value != "a" {
		t.Fatalf("retry returned %q, want first result %q", r.Value, "a")
	}
	s.execute(Command{Type: OpAppend, Key: "x", Value: "b", ClientID: 1, Seq: 2})
	if got := s.data["x"]; got != "ab" {
		t.Fatalf("x = %q, want %q", got, "ab")
	}
}
