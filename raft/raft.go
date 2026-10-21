package raft

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

var ErrNotLeader = errors.New("raft: not leader")
var ErrStopped = errors.New("raft: node stopped")

const noVote = -1

type Node struct {
	cfg Config
	id  int

	mu       sync.Mutex
	role     Role
	term     uint64
	votedFor int
	leaderID int
	log      []Entry // log[0] is a sentinel at index 0, term 0

	commitIndex uint64
	lastApplied uint64
	// durableIndex is the highest index this node has synced to storage. A
	// leader counts itself toward a commit quorum only up to here.
	durableIndex uint64

	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	electionDeadline  time.Time
	lastLeaderContact time.Time
	leaderSince       time.Time
	lastAck           map[int]time.Time // leader: last reply from each peer

	rng       *rand.Rand
	applyCond *sync.Cond
	replicate map[int]chan struct{}
	flush     chan struct{}
	stop      chan struct{}
	stopped   bool
	wg        sync.WaitGroup
}

// New loads persisted state from cfg.Storage and starts the node's
// background goroutines.
func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	term, votedFor, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		term:       term,
		votedFor:   votedFor,
		leaderID:   noVote,
		log:        append([]Entry{{}}, entries...),
		nextIndex:  map[int]uint64{},
		matchIndex: map[int]uint64{},
		lastAck:    map[int]time.Time{},
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)*7919)),
		replicate:  map[int]chan struct{}{},
		flush:      make(chan struct{}, 1),
		stop:       make(chan struct{}),
	}
	n.durableIndex = n.lastIndex()
	n.applyCond = sync.NewCond(&n.mu)
	n.resetElectionTimer()

	for _, p := range cfg.Peers {
		if p == n.id {
			continue
		}
		ch := make(chan struct{}, 1)
		n.replicate[p] = ch
		n.wg.Add(1)
		go n.replicator(p, ch)
	}
	n.wg.Add(3)
	go n.ticker()
	go n.applier()
	go n.flusher()
	return n, nil
}

// Stop halts all goroutines. Storage is left open for the caller.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stop)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends data to the leader's log. It returns the index and term the
// entry will commit at if leadership holds; the caller learns the outcome by
// watching ApplyMsg for that index and comparing the term.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: data}
	n.log = append(n.log, e)
	if err := n.cfg.Storage.Append([]Entry{e}); err != nil {
		return 0, 0, err
	}
	signal(n.flush)
	for _, ch := range n.replicate {
		signal(ch)
	}
	return e.Index, e.Term, nil
}

type Status struct {
	ID          int
	Role        Role
	Term        uint64
	LeaderID    int
	CommitIndex uint64
	LastIndex   uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{n.id, n.role, n.term, n.leaderID, n.commitIndex, n.lastIndex()}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }

func (n *Node) termAt(i uint64) uint64 { return n.log[i].Term }

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) electionTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	return lo + time.Duration(n.rng.Int63n(int64(hi-lo)))
}

func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(n.electionTimeout())
}

func (n *Node) emit(kind EventKind) {
	if n.cfg.Observer != nil {
		n.cfg.Observer(Event{Kind: kind, Node: n.id, Term: n.term, Candidate: n.votedFor, EntryAt: n.entryAt})
	}
}

func (n *Node) entryAt(i uint64) (Entry, bool) {
	if i == 0 || i > n.lastIndex() {
		return Entry{}, false
	}
	return n.log[i], true
}

// setTermAndVote persists before the in-memory change becomes visible to any
// RPC reply, so a crash cannot make this node vote twice in one term.
func (n *Node) setTermAndVote(term uint64, votedFor int) error {
	if term == n.term && votedFor == n.votedFor {
		return nil
	}
	if err := n.cfg.Storage.SetState(term, votedFor); err != nil {
		return err
	}
	n.term, n.votedFor = term, votedFor
	return nil
}

func (n *Node) becomeFollower(term uint64) error {
	if term > n.term {
		if err := n.setTermAndVote(term, noVote); err != nil {
			return err
		}
	}
	if n.role != Follower {
		n.role = Follower
		n.emit(EventBecameFollower)
	}
	return nil
}

// ---- election ----

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 5)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if n.role != Leader && time.Now().After(n.electionDeadline) {
			n.resetElectionTimer()
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				n.campaign()
			}()
		}
		if n.role == Leader && !n.hasQuorumContact() {
			// Check quorum: a leader cut off from a majority steps down so
			// clients stop sending it writes that cannot commit.
			n.cfg.Logf("node %d: lost quorum contact in term %d, stepping down", n.id, n.term)
			n.becomeFollower(n.term)
			n.leaderID = noVote
		}
		n.mu.Unlock()
	}
}

func (n *Node) hasQuorumContact() bool {
	window := n.cfg.ElectionTimeoutMax
	now := time.Now()
	if now.Sub(n.leaderSince) < window {
		return true
	}
	alive := 1
	for _, p := range n.cfg.Peers {
		if p != n.id && now.Sub(n.lastAck[p]) < window {
			alive++
		}
	}
	return alive >= n.quorum()
}

// campaign runs a pre-vote round and, if it wins, a real election.
func (n *Node) campaign() {
	n.mu.Lock()
	startTerm := n.term
	n.mu.Unlock()
	if !n.collectVotes(startTerm, true) {
		return
	}

	n.mu.Lock()
	if n.stopped || n.term != startTerm || n.role == Leader {
		n.mu.Unlock()
		return
	}
	if err := n.setTermAndVote(n.term+1, n.id); err != nil {
		n.cfg.Logf("node %d: persist vote: %v", n.id, err)
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.leaderID = noVote
	n.resetElectionTimer()
	n.emit(EventBecameCandidate)
	electionTerm := n.term
	n.mu.Unlock()

	if !n.collectVotes(electionTerm, false) {
		return
	}
	n.mu.Lock()
	if n.term == electionTerm && n.role == Candidate {
		n.becomeLeader()
	}
	n.mu.Unlock()
}

// collectVotes asks every peer for a (pre-)vote in term+1 (pre-vote) or term
// and reports whether a quorum granted it. It returns early once the outcome
// is decided.
func (n *Node) collectVotes(term uint64, preVote bool) bool {
	n.mu.Lock()
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
		PreVote:      preVote,
	}
	if preVote {
		args.Term = term + 1
	}
	n.mu.Unlock()

	results := make(chan bool, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p == n.id {
			continue
		}
		go func(p int) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, p, args)
			if err != nil {
				results <- false
				return
			}
			n.mu.Lock()
			if !n.stopped && reply.Term > n.term && !reply.VoteGranted {
				n.becomeFollower(reply.Term)
			}
			n.mu.Unlock()
			results <- reply.VoteGranted
		}(p)
	}

	granted, denied := 1, 0
	need := n.quorum()
	for granted < need && denied <= len(n.cfg.Peers)-need {
		select {
		case ok := <-results:
			if ok {
				granted++
			} else {
				denied++
			}
		case <-n.stop:
			return false
		}
	}
	return granted >= need
}

// HandleRequestVote is the receiver side of RequestVote.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}

	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())

	if args.PreVote {
		// Leader stickiness: refuse while a live leader has been heard from
		// within the minimum election timeout. Never changes local state.
		heardRecently := n.role == Leader ||
			time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin
		grant := args.Term > n.term && upToDate && !heardRecently
		return &RequestVoteReply{Term: n.term, VoteGranted: grant}, nil
	}

	if args.Term > n.term {
		if err := n.becomeFollower(args.Term); err != nil {
			return nil, err
		}
	}
	reply := &RequestVoteReply{Term: n.term}
	if args.Term < n.term {
		return reply, nil
	}
	if (n.votedFor == noVote || n.votedFor == args.CandidateID) && upToDate {
		if err := n.setTermAndVote(n.term, args.CandidateID); err != nil {
			return nil, err
		}
		// Reset only when granting: resetting on every request lets a
		// candidate with a stale log keep a better candidate from timing out.
		n.resetElectionTimer()
		reply.VoteGranted = true
		n.emit(EventVoted)
	}
	return reply, nil
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	n.leaderSince = time.Now()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.emit(EventBecameLeader)
	n.cfg.Logf("node %d: leader for term %d", n.id, n.term)
	// A no-op in the new term lets the leader commit entries left over from
	// earlier terms (it may only count replicas for current-term entries).
	e := Entry{Index: n.lastIndex() + 1, Term: n.term}
	n.log = append(n.log, e)
	if err := n.cfg.Storage.Append([]Entry{e}); err != nil {
		n.cfg.Logf("node %d: append no-op: %v", n.id, err)
	}
	signal(n.flush)
	for _, ch := range n.replicate {
		signal(ch)
	}
}

// ---- replication ----

func (n *Node) replicator(peer int, trigger chan struct{}) {
	defer n.wg.Done()
	hb := time.NewTimer(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-trigger:
		case <-hb.C:
		}
		if !hb.Stop() {
			select {
			case <-hb.C:
			default:
			}
		}
		hb.Reset(n.cfg.HeartbeatInterval)
		n.replicateOnce(peer, trigger)
	}
}

func (n *Node) replicateOnce(peer int, trigger chan struct{}) {
	n.mu.Lock()
	if n.role != Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	end := n.lastIndex() + 1
	if end-next > uint64(n.cfg.MaxBatch) {
		end = next + uint64(n.cfg.MaxBatch)
	}
	args := &AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.termAt(next - 1),
		Entries:      append([]Entry(nil), n.log[next:end]...),
		LeaderCommit: n.commitIndex,
	}
	term := n.term
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		return
	}
	if n.term != term || n.role != Leader {
		return
	}
	n.lastAck[peer] = time.Now()
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		n.advanceCommit()
		if n.nextIndex[peer] <= n.lastIndex() {
			signal(trigger)
		}
		return
	}
	// Back up using the follower's conflict hint.
	next = reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := n.lastIndex(); i > 0; i-- {
			if n.termAt(i) == reply.ConflictTerm {
				next = i + 1
				break
			}
			if n.termAt(i) < reply.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	if next <= n.matchIndex[peer] {
		next = n.matchIndex[peer] + 1
	}
	if next > n.lastIndex()+1 {
		next = n.lastIndex() + 1
	}
	n.nextIndex[peer] = next
	signal(trigger)
}

// advanceCommit moves commitIndex to the highest index stored on a quorum,
// counting only entries from the current term (Raft paper section 5.4.2).
func (n *Node) advanceCommit() {
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p == n.id {
			matches = append(matches, n.durableIndex)
		} else {
			matches = append(matches, n.matchIndex[p])
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate > n.commitIndex && n.termAt(candidate) == n.term {
		n.commitIndex = candidate
		n.applyCond.Broadcast()
		// Followers learn the new commit index on the next heartbeat; push it
		// now so they can apply without waiting.
		for _, ch := range n.replicate {
			signal(ch)
		}
	}
}

// HandleAppendEntries is the receiver side of AppendEntries.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &AppendEntriesReply{Term: n.term}
	if args.Term < n.term {
		return reply, nil
	}
	if args.Term > n.term || n.role != Follower {
		if err := n.becomeFollower(args.Term); err != nil {
			return nil, err
		}
		reply.Term = n.term
	}
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()

	if args.PrevLogIndex > n.lastIndex() {
		reply.ConflictIndex = n.lastIndex() + 1
		return reply, nil
	}
	if t := n.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && n.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return reply, nil
	}

	changed := false
	for i, e := range args.Entries {
		if e.Index <= n.lastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue // already have it; a reordered RPC must not truncate
			}
			if e.Index <= n.commitIndex {
				// Would mean a committed entry is being overwritten.
				panic("raft: leader tried to overwrite a committed entry")
			}
			if err := n.cfg.Storage.TruncateFrom(e.Index); err != nil {
				return nil, err
			}
			n.log = n.log[:e.Index]
		}
		rest := args.Entries[i:]
		if err := n.cfg.Storage.Append(rest); err != nil {
			return nil, err
		}
		n.log = append(n.log, rest...)
		changed = true
		break
	}
	// Sync even if nothing changed when the entries we are about to vouch for
	// are not yet durable (e.g. written while we were leader, never synced).
	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if changed || lastNew > n.durableIndex {
		if err := n.cfg.Storage.Sync(); err != nil {
			return nil, err
		}
		n.durableIndex = n.lastIndex()
		// The fsync ran under the lock and may have been slow; restart the
		// timer so this follower does not campaign against a live leader the
		// moment the lock is released.
		n.lastLeaderContact = time.Now()
		n.resetElectionTimer()
	}

	// Commit only up to the last entry this RPC vouched for: anything past it
	// in our log may be stale and is not known to match the leader.
	if args.LeaderCommit > n.commitIndex {
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply, nil
}

// flusher syncs the leader's appended entries in batches (group commit), so
// many proposals share one fsync.
func (n *Node) flusher() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stop:
			return
		case <-n.flush:
		}
		n.mu.Lock()
		target, term, role := n.lastIndex(), n.term, n.role
		n.mu.Unlock()
		if role != Leader {
			continue
		}
		if err := n.cfg.Storage.Sync(); err != nil {
			n.cfg.Logf("node %d: sync: %v", n.id, err)
			continue
		}
		n.mu.Lock()
		// A leader never truncates its own log within its term, so entries up
		// to target are still the ones that were synced.
		if n.term == term && n.role == Leader && target > n.durableIndex {
			n.durableIndex = target
			n.advanceCommit()
		}
		n.mu.Unlock()
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		batch := append([]Entry(nil), n.log[n.lastApplied+1:n.commitIndex+1]...)
		n.mu.Unlock()

		for _, e := range batch {
			select {
			case n.cfg.ApplyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Data: e.Data}:
			case <-n.stop:
				return
			}
		}
		n.mu.Lock()
		n.lastApplied = batch[len(batch)-1].Index
		n.mu.Unlock()
	}
}
