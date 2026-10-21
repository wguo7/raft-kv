package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// FileStorage keeps three kinds of file in one directory:
//
//	state            term and vote, replaced atomically
//	snapshot         latest snapshot, replaced atomically
//	wal-NNNNNNNN     write-ahead log segments, append-only
//
// Each WAL record carries a CRC32; on load a torn or corrupt tail of the last
// segment is cut off. Saving a snapshot starts a new segment and deletes
// segments whose entries are all covered by the snapshot.
//
// Record layout: crc u32 | kind u8 | index u64 | term u64 | len u32 | data
type FileStorage struct {
	dir string

	// syncMu orders Sync against segment rotation, so the file being fsynced
	// is never closed underneath it. mu guards everything else and is held
	// only briefly, never across an fsync of the active segment.
	syncMu sync.Mutex
	mu     sync.Mutex
	seg    *os.File
	buf    *bufio.Writer
	segs   []segment // oldest first; the last one is active
	snap   uint64    // index of the saved snapshot
}

type segment struct {
	seq      int
	maxIndex uint64 // highest index ever appended to it
}

const (
	recAppend   byte = 1
	recTruncate byte = 2
	recHeader        = 4 + 1 + 8 + 8 + 4
)

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (s *FileStorage) segPath(seq int) string {
	return filepath.Join(s.dir, fmt.Sprintf("wal-%08d", seq))
}

func (s *FileStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st PersistentState
	var err error
	if st.Term, st.VotedFor, err = s.loadState(); err != nil {
		return st, err
	}
	if st.SnapIndex, st.SnapTerm, st.Snapshot, err = s.loadSnapshot(); err != nil {
		return st, err
	}
	s.snap = st.SnapIndex

	names, _ := filepath.Glob(filepath.Join(s.dir, "wal-*"))
	sort.Strings(names)
	var log []Entry
	for i, name := range names {
		var seq int
		if _, err := fmt.Sscanf(filepath.Base(name), "wal-%08d", &seq); err != nil {
			continue
		}
		last := i == len(names)-1
		maxIdx, err := replaySegment(name, &log, last)
		if err != nil {
			return st, err
		}
		s.segs = append(s.segs, segment{seq: seq, maxIndex: maxIdx})
	}
	k := sort.Search(len(log), func(j int) bool { return log[j].Index > st.SnapIndex })
	st.Entries = log[k:]

	if len(s.segs) == 0 {
		s.segs = []segment{{seq: 1}}
	}
	active := s.segs[len(s.segs)-1]
	f, err := os.OpenFile(s.segPath(active.seq), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return st, err
	}
	s.seg, s.buf = f, bufio.NewWriterSize(f, 1<<20)
	return st, nil
}

// replaySegment applies one segment's records to log. In the last segment a
// bad record is treated as a torn write and the file is cut there.
func replaySegment(name string, log *[]Entry, last bool) (uint64, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var good int64
	var maxIdx uint64
	hdr := make([]byte, recHeader)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break
		}
		n := binary.LittleEndian.Uint32(hdr[21:])
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			break
		}
		crc := crc32.NewIEEE()
		crc.Write(hdr[4:])
		crc.Write(data)
		if crc.Sum32() != binary.LittleEndian.Uint32(hdr[0:]) {
			break
		}
		idx := binary.LittleEndian.Uint64(hdr[5:])
		switch hdr[4] {
		case recAppend:
			l := *log
			if len(l) > 0 && idx != l[len(l)-1].Index+1 {
				if idx <= l[len(l)-1].Index {
					return 0, fmt.Errorf("%s: append at %d after %d without truncate", name, idx, l[len(l)-1].Index)
				}
				// A gap: an installed snapshot covers the missing indexes,
				// so everything before it is superseded.
				l = l[:0]
			}
			if n == 0 {
				data = nil
			}
			*log = append(l, Entry{Index: idx, Term: binary.LittleEndian.Uint64(hdr[13:]), Data: data})
			if idx > maxIdx {
				maxIdx = idx
			}
		case recTruncate:
			l := *log
			k := sort.Search(len(l), func(j int) bool { return l[j].Index >= idx })
			*log = l[:k]
		default:
			return 0, fmt.Errorf("%s: bad record kind %d", name, hdr[4])
		}
		good += int64(recHeader) + int64(n)
	}
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if good < st.Size() {
		if !last {
			return 0, fmt.Errorf("%s: corrupt record at offset %d", name, good)
		}
		if err := f.Truncate(good); err != nil {
			return 0, err
		}
	}
	return maxIdx, nil
}

func (s *FileStorage) loadState() (uint64, int, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, "state"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, noVote, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if len(b) != 16 {
		return 0, 0, fmt.Errorf("state file: %d bytes", len(b))
	}
	return binary.LittleEndian.Uint64(b), int(int64(binary.LittleEndian.Uint64(b[8:]))), nil
}

func (s *FileStorage) loadSnapshot() (uint64, uint64, []byte, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, "snapshot"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	if len(b) < 20 || crc32.ChecksumIEEE(b[4:]) != binary.LittleEndian.Uint32(b) {
		return 0, 0, nil, errors.New("snapshot file is corrupt")
	}
	return binary.LittleEndian.Uint64(b[4:]), binary.LittleEndian.Uint64(b[12:]), b[20:], nil
}

// writeAtomic replaces dir/name with b durably (temp file, fsync, rename,
// fsync the directory).
func writeAtomic(dir, name string, b []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *FileStorage) SetState(term uint64, votedFor int) error {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, term)
	binary.LittleEndian.PutUint64(b[8:], uint64(int64(votedFor)))
	return writeAtomic(s.dir, "state", b)
}

func (s *FileStorage) SaveSnapshot(index, term uint64, data []byte) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	stale := index <= s.snap
	s.mu.Unlock()
	if stale {
		return nil
	}
	b := make([]byte, 20, 20+len(data))
	binary.LittleEndian.PutUint64(b[4:], index)
	binary.LittleEndian.PutUint64(b[12:], term)
	b = append(b, data...)
	binary.LittleEndian.PutUint32(b, crc32.ChecksumIEEE(b[4:]))
	if err := writeAtomic(s.dir, "snapshot", b); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = index
	// Seal the active segment and start a new one.
	if err := s.buf.Flush(); err != nil {
		return err
	}
	if err := s.seg.Sync(); err != nil {
		return err
	}
	s.seg.Close()
	next := s.segs[len(s.segs)-1].seq + 1
	f, err := os.OpenFile(s.segPath(next), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.seg, s.buf = f, bufio.NewWriterSize(f, 1<<20)
	s.segs = append(s.segs, segment{seq: next})
	if err := syncDir(s.dir); err != nil {
		return err
	}
	// Delete sealed segments the snapshot fully covers.
	kept := s.segs[:0]
	for i, sg := range s.segs {
		if i < len(s.segs)-1 && sg.maxIndex <= index {
			os.Remove(s.segPath(sg.seq))
			continue
		}
		kept = append(kept, sg)
	}
	s.segs = kept
	return nil
}

func (s *FileStorage) writeRecord(kind byte, idx, term uint64, data []byte) error {
	hdr := make([]byte, recHeader)
	hdr[4] = kind
	binary.LittleEndian.PutUint64(hdr[5:], idx)
	binary.LittleEndian.PutUint64(hdr[13:], term)
	binary.LittleEndian.PutUint32(hdr[21:], uint32(len(data)))
	crc := crc32.NewIEEE()
	crc.Write(hdr[4:])
	crc.Write(data)
	binary.LittleEndian.PutUint32(hdr[0:], crc.Sum32())
	if _, err := s.buf.Write(hdr); err != nil {
		return err
	}
	_, err := s.buf.Write(data)
	return err
}

func (s *FileStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := &s.segs[len(s.segs)-1]
	for _, e := range entries {
		if err := s.writeRecord(recAppend, e.Index, e.Term, e.Data); err != nil {
			return err
		}
		if e.Index > active.maxIndex {
			active.maxIndex = e.Index
		}
	}
	return nil
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeRecord(recTruncate, index, 0, nil)
}

// Sync flushes buffered records under the lock, then fsyncs without it so
// new appends can proceed while the disk works.
func (s *FileStorage) Sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	err := s.buf.Flush()
	f := s.seg
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return f.Sync()
}

func (s *FileStorage) Close() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seg == nil {
		return nil
	}
	s.buf.Flush()
	return s.seg.Close()
}

var _ Storage = (*FileStorage)(nil)
var _ Storage = (*MemStorage)(nil)
