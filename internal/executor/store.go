package executor

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

// Status is the lifecycle state of one invocation request.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusTimeout   Status = "timeout"
	StatusCancelled Status = "cancelled"
	// StatusUnknown is assigned at recovery time to requests that were
	// queued/running when the process stopped: their outcome is uncertain.
	StatusUnknown Status = "unknown"
)

func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusTimeout, StatusCancelled, StatusUnknown:
		return true
	}
	return false
}

// Record is the durable, queryable result of one request.
type Record struct {
	ID         string          `json:"id"`
	Function   string          `json:"function"`
	Args       json.RawMessage `json:"args,omitempty"`
	Status     Status          `json:"status"`
	Result     any             `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
}

// Store keeps records in memory and appends every mutation to a JSONL
// file so state survives restarts. On load, non-terminal records are
// reclassified as StatusUnknown.
type Store struct {
	mu   sync.Mutex
	recs map[string]*Record
	file *os.File
	enc  *json.Encoder
}

func OpenStore(path string) (*Store, error) {
	s := &Store{recs: map[string]*Record{}}
	if path == "" {
		return s, nil
	}
	if _, err := os.Stat(path); err == nil {
		if err := s.load(path); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.file = f
	s.enc = json.NewEncoder(f)
	return s, nil
}

func (s *Store) load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return err
		}
		r := rec
		s.recs[r.ID] = &r
	}
	if err := sc.Err(); err != nil {
		return err
	}
	// Crash recovery: anything not terminal is uncertain.
	for _, r := range s.recs {
		if !r.Status.Terminal() {
			r.Status = StatusUnknown
			r.Error = "process restarted before outcome was recorded"
			now := time.Now()
			r.FinishedAt = &now
		}
	}
	return nil
}

// persist must be called with mu held.
func (s *Store) persist(r *Record) {
	if s.enc == nil {
		return
	}
	// Best-effort durability: a failed append is surfaced nowhere else,
	// so we ignore the error but never corrupt in-memory state.
	_ = s.enc.Encode(r)
	_ = s.file.Sync()
}

func (s *Store) Put(r *Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *r
	s.recs[r.ID] = &cp
	s.persist(&cp)
}

// CASStatus atomically moves id from one of the allowed source statuses
// to the next status, applying mutate to fill result fields. Returns
// false if the record is missing or no longer in a source status — this
// is what prevents a late result from overwriting a timeout.
func (s *Store) CASStatus(id string, from []Status, to Status, mutate func(*Record)) (*Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recs[id]
	if !ok {
		return nil, false
	}
	allowed := false
	for _, f := range from {
		if r.Status == f {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, false
	}
	r.Status = to
	if mutate != nil {
		mutate(r)
	}
	s.persist(r)
	cp := *r
	return &cp, true
}

func (s *Store) Get(id string) (*Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recs[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (s *Store) List() []*Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Record, 0, len(s.recs))
	for _, r := range s.recs {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

var ErrNotFound = errors.New("record not found")
