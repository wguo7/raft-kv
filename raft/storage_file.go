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
	"sync"
)

// FileStorage keeps term and vote in a small state file (replaced atomically)
// and the log in an append-only write-ahead log. Each WAL record carries a
// CRC32; on load, a torn or corrupt tail is cut off.
//
// Record layout: crc u32 | kind u8 | index u64 | term u64 | len u32 | data
type FileStorage struct {
	mu  sync.Mutex
	dir string
	wal *os.File
	buf *bufio.Writer
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
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, wal: f}, nil
}

func (s *FileStorage) Load() (uint64, int, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	term, votedFor, err := s.loadState()
	if err != nil {
		return 0, 0, nil, err
	}
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return 0, 0, nil, err
	}
	r := bufio.NewReader(s.wal)
	var log []Entry
	var good int64
	hdr := make([]byte, recHeader)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break
		}
		kind := hdr[4]
		idx := binary.LittleEndian.Uint64(hdr[5:])
		t := binary.LittleEndian.Uint64(hdr[13:])
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
		switch kind {
		case recAppend:
			if idx != uint64(len(log))+1 {
				return 0, 0, nil, fmt.Errorf("wal: append at %d, expected %d", idx, len(log)+1)
			}
			if n == 0 {
				data = nil
			}
			log = append(log, Entry{Index: idx, Term: t, Data: data})
		case recTruncate:
			if idx >= 1 && int(idx-1) < len(log) {
				log = log[:idx-1]
			}
		default:
			return 0, 0, nil, fmt.Errorf("wal: bad record kind %d", kind)
		}
		good += int64(recHeader) + int64(n)
	}
	if err := s.wal.Truncate(good); err != nil {
		return 0, 0, nil, err
	}
	if _, err := s.wal.Seek(good, io.SeekStart); err != nil {
		return 0, 0, nil, err
	}
	s.buf = bufio.NewWriterSize(s.wal, 1<<20)
	return term, votedFor, log, nil
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

func (s *FileStorage) SetState(term uint64, votedFor int) error {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, term)
	binary.LittleEndian.PutUint64(b[8:], uint64(int64(votedFor)))
	tmp := filepath.Join(s.dir, "state.tmp")
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
	if err := os.Rename(tmp, filepath.Join(s.dir, "state")); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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
	for _, e := range entries {
		if err := s.writeRecord(recAppend, e.Index, e.Term, e.Data); err != nil {
			return err
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
	s.mu.Lock()
	err := s.buf.Flush()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.wal.Sync()
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buf != nil {
		s.buf.Flush()
	}
	return s.wal.Close()
}
