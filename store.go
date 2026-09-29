package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu      sync.RWMutex
	path    string
	records map[string]Invocation
}

type persistedState struct {
	Records []Invocation `json:"records"`
}

func OpenStore(path string) (*Store, error) {
	store := &Store{path: path, records: make(map[string]Invocation)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return store, nil
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	for _, record := range state.Records {
		store.records[record.ID] = record
	}
	return store, nil
}

func (s *Store) Create(record Invocation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[record.ID]; exists {
		return fmt.Errorf("invocation %s already exists", record.ID)
	}
	s.records[record.ID] = record
	return s.persistLocked()
}

func (s *Store) Get(id string) (Invocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[id]
	return record.Clone(), ok
}

func (s *Store) List() []Invocation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	records := make([]Invocation, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record.Clone())
	}
	return records
}

func (s *Store) Transition(id, fromStatus, toStatus string, mutate func(*Invocation)) (Invocation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok {
		return Invocation{}, false, nil
	}
	if record.Status != fromStatus {
		return record.Clone(), false, nil
	}
	record.Status = toStatus
	if mutate != nil {
		mutate(&record)
	}
	s.records[id] = record
	if err := s.persistLocked(); err != nil {
		return record.Clone(), false, err
	}
	return record.Clone(), true, nil
}

func (s *Store) MarkRunningUncertain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for id, record := range s.records {
		if record.Status == StatusRunning {
			record.Status = StatusUncertain
			record.Error = "process restarted before the function result was recorded"
			record.FinishedAt = nowRFC3339()
			s.records[id] = record
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return s.persistLocked()
}

func (s *Store) QueuedRecords() []Invocation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var records []Invocation
	for _, record := range s.records {
		if record.Status == StatusQueued {
			records = append(records, record.Clone())
		}
	}
	return records
}

func (s *Store) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	state := persistedState{Records: make([]Invocation, 0, len(s.records))}
	for _, record := range s.records {
		state.Records = append(state.Records, record)
	}
	data, err := json.MarshalIndent(&state, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	return nil
}
