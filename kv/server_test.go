package kv

import "testing"

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
