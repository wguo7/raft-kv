package kv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/wguo7/raft-kv/raft"
)

// HTTP API:
//
//	GET  /kv/{key}             read (through the log)
//	PUT  /kv/{key}             write the request body
//	POST /kv/{key}?op=append   append the request body
//	GET  /status               node role, term, leader, commit index
//
// Optional headers X-Client-ID and X-Seq identify a client session so a
// retried request applies once. A follower answers 421 with X-Leader set to
// the leader's HTTP address when it knows it.
type HTTPHandler struct {
	Server      *Server
	LeaderAddrs map[int]string // raft id -> HTTP host:port
	Timeout     time.Duration
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/status" {
		h.status(w)
		return
	}
	if len(r.URL.Path) <= len("/kv/") || r.URL.Path[:4] != "/kv/" {
		http.NotFound(w, r)
		return
	}
	cmd := Command{Key: r.URL.Path[4:]}
	switch {
	case r.Method == http.MethodGet:
		cmd.Type = OpGet
	case r.Method == http.MethodPut:
		cmd.Type = OpPut
	case r.Method == http.MethodPost && r.URL.Query().Get("op") == "append":
		cmd.Type = OpAppend
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if cmd.Type != OpGet {
		b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cmd.Value = string(b)
	}
	cmd.ClientID, _ = strconv.ParseUint(r.Header.Get("X-Client-ID"), 10, 64)
	cmd.Seq, _ = strconv.ParseUint(r.Header.Get("X-Seq"), 10, 64)
	if cmd.ClientID == 0 {
		cmd.ClientID, cmd.Seq = rand.Uint64(), 0
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	res, err := h.Server.Submit(ctx, cmd)
	switch {
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrLeadershipLost):
		if addr, ok := h.LeaderAddrs[h.Server.Node().Status().LeaderID]; ok {
			w.Header().Set("X-Leader", addr)
		}
		http.Error(w, err.Error(), http.StatusMisdirectedRequest)
	case errors.Is(err, ErrTimeout):
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case cmd.Type == OpGet && !res.Found:
		http.NotFound(w, r)
	default:
		io.WriteString(w, res.Value)
	}
}

func (h *HTTPHandler) status(w http.ResponseWriter) {
	s := h.Server.Node().Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": s.ID, "role": s.Role.String(), "term": s.Term, "leader": s.LeaderID,
		"leader_http": h.LeaderAddrs[s.LeaderID],
		"commit":      s.CommitIndex, "last_index": s.LastIndex,
		"is_leader": s.Role == raft.Leader,
	})
}
