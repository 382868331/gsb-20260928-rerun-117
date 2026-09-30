package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var ErrQueueFull = errors.New("execution queue is full")

type Manager struct {
	store    *Store
	registry Registry
	slots    int
	queueCap int
	jobs     chan string

	mu       sync.Mutex
	cancels  map[string]context.CancelFunc
	active   int
	waiting  []string
	stopping bool
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewManager(store *Store, registry Registry, slots, queueSize int) (*Manager, error) {
	if slots <= 0 {
		slots = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	if err := store.MarkRunningUncertain(); err != nil {
		return nil, err
	}
	manager := &Manager{
		store:    store,
		registry: registry,
		slots:    slots,
		queueCap: queueSize,
		jobs:     make(chan string, slots),
		cancels:  make(map[string]context.CancelFunc),
		stop:     make(chan struct{}),
	}
	return manager, nil
}

func (m *Manager) Start() {
	for i := 0; i < m.slots; i++ {
		m.wg.Add(1)
		go m.worker()
	}
	queued := m.store.QueuedRecords()
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreatedAt != queued[j].CreatedAt {
			return queued[i].CreatedAt < queued[j].CreatedAt
		}
		return queued[i].ID < queued[j].ID
	})
	for _, record := range queued {
		if !m.admit(record.ID) {
			m.finishRejected(record.ID, "execution queue is full after restart")
		}
	}
}

func (m *Manager) Submit(req InvokeRequest) (Invocation, error) {
	if req.Function == "" {
		return Invocation{}, errors.New("function is required")
	}
	if _, ok := m.registry.Get(req.Function); !ok {
		return Invocation{}, fmt.Errorf("unknown function %q", req.Function)
	}
	timeoutMs := req.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	if timeoutMs > 30000 {
		timeoutMs = 30000
	}
	argument := req.Argument
	if len(argument) == 0 {
		argument = json.RawMessage(`{}`)
	}
	now := nowRFC3339()
	record := Invocation{
		ID:        newID(),
		Function:  req.Function,
		Argument:  argument,
		TimeoutMs: timeoutMs,
		Status:    StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := m.store.Create(record); err != nil {
		return Invocation{}, err
	}
	if !m.admit(record.ID) {
		return m.finishRejected(record.ID, "execution queue is full")
	}
	return record, nil
}

func (m *Manager) Get(id string) (Invocation, bool) {
	return m.store.Get(id)
}

func (m *Manager) Cancel(id string) (Invocation, bool, error) {
	record, ok := m.store.Get(id)
	if !ok {
		return Invocation{}, false, nil
	}
	switch record.Status {
	case StatusQueued:
		updated, transitioned, err := m.store.Transition(id, StatusQueued, StatusCanceled, func(next *Invocation) {
			next.Error = "canceled before execution started"
			next.FinishedAt = nowRFC3339()
			next.UpdatedAt = next.FinishedAt
		})
		if transitioned {
			// Drop it if it is still waiting. If it was already promoted into a
			// slot, the worker's queued -> running transition fails and its own
			// releaseSlot returns the slot, so nothing is refunded twice.
			m.mu.Lock()
			for i, waitingID := range m.waiting {
				if waitingID == id {
					m.waiting = append(m.waiting[:i], m.waiting[i+1:]...)
					break
				}
			}
			m.mu.Unlock()
		}
		return updated, transitioned, err
	case StatusRunning:
		m.mu.Lock()
		cancel := m.cancels[id]
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		updated, _ := m.store.Get(id)
		return updated, true, nil
	default:
		return record, false, nil
	}
}

// admit reserves capacity for a newly queued invocation. Execution slots and
// waiting-queue capacity are checked independently: a free slot always accepts
// the request, even before any worker goroutine has been scheduled, and the
// bounded wait queue is only consumed when every slot is occupied.
func (m *Manager) admit(id string) bool {
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		return false
	}
	if m.active < m.slots {
		m.active++
		m.mu.Unlock()
		select {
		case m.jobs <- id:
			return true
		case <-m.stop:
			m.releaseSlot()
			return false
		}
	}
	if len(m.waiting) < m.queueCap {
		m.waiting = append(m.waiting, id)
		m.mu.Unlock()
		return true
	}
	m.mu.Unlock()
	return false
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stop:
			return
		case id := <-m.jobs:
			m.execute(id)
		}
	}
}

func (m *Manager) execute(id string) {
	defer m.releaseSlot()
	record, ok, err := m.store.Transition(id, StatusQueued, StatusRunning, func(next *Invocation) {
		next.StartedAt = nowRFC3339()
		next.UpdatedAt = next.StartedAt
	})
	if err != nil || !ok {
		return
	}
	fn, ok := m.registry.Get(record.Function)
	if !ok {
		m.markFailed(id, fmt.Sprintf("unknown function %q", record.Function))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(record.TimeoutMs)*time.Millisecond)
	defer cancel()
	m.mu.Lock()
	m.cancels[id] = cancel
	m.mu.Unlock()

	type executionResult struct {
		value any
		err   error
	}
	done := make(chan executionResult, 1)
	go func() {
		value, callErr := fn(ctx, record.Argument)
		done <- executionResult{value: value, err: callErr}
	}()

	var output executionResult
	select {
	case output = <-done:
		// ctx may already have expired before this receive was selected; a late
		// success from the function must never flip a timeout/cancel terminal.
		if ctx.Err() != nil {
			terminalStatus := StatusTimedOut
			errMessage := "execution timed out"
			if errors.Is(ctx.Err(), context.Canceled) {
				terminalStatus = StatusCanceled
				errMessage = "execution canceled"
			}
			m.mu.Lock()
			delete(m.cancels, id)
			m.mu.Unlock()
			m.markTerminal(id, terminalStatus, nil, errMessage)
			return
		}
	case <-ctx.Done():
		errMessage := "execution timed out"
		terminalStatus := StatusTimedOut
		if errors.Is(ctx.Err(), context.Canceled) {
			errMessage = "execution canceled"
			terminalStatus = StatusCanceled
		}
		m.mu.Lock()
		delete(m.cancels, id)
		m.mu.Unlock()
		m.markTerminal(id, terminalStatus, nil, errMessage)
		return
	}
	m.mu.Lock()
	delete(m.cancels, id)
	m.mu.Unlock()
	if output.err != nil {
		if errors.Is(output.err, context.DeadlineExceeded) {
			m.markTerminal(id, StatusTimedOut, nil, "execution timed out")
		} else if errors.Is(output.err, context.Canceled) {
			m.markTerminal(id, StatusCanceled, nil, "execution canceled")
		} else {
			m.markFailed(id, output.err.Error())
		}
		return
	}
	encoded, err := json.Marshal(output.value)
	if err != nil {
		m.markFailed(id, err.Error())
		return
	}
	m.markTerminal(id, StatusSucceeded, encoded, "")
}

func (m *Manager) markFailed(id, message string) {
	m.markTerminal(id, StatusFailed, nil, message)
}

// releaseSlot returns exactly one execution slot and promotes waiting
// invocations into freed slots. Every admitted slot must be released once,
// whether it ends in success, failure, timeout, cancellation or a failed
// queued -> running transition (for example a raced queued cancel).
func (m *Manager) releaseSlot() {
	m.mu.Lock()
	m.active--
	var promoted []string
	for !m.stopping && m.active < m.slots && len(m.waiting) > 0 {
		next := m.waiting[0]
		m.waiting = m.waiting[1:]
		m.active++
		promoted = append(promoted, next)
	}
	m.mu.Unlock()
	for _, next := range promoted {
		select {
		case m.jobs <- next:
		case <-m.stop:
			m.releaseSlot()
		}
	}
}

func (m *Manager) capacitySnapshot() (active, waiting int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active, len(m.waiting)
}

func (m *Manager) finishRejected(id, message string) (Invocation, error) {
	updated, transitioned, err := m.store.Transition(id, StatusQueued, StatusRejected, func(next *Invocation) {
		next.Error = message
		next.FinishedAt = nowRFC3339()
		next.UpdatedAt = next.FinishedAt
	})
	if err != nil {
		return Invocation{}, err
	}
	if !transitioned {
		return updated, ErrQueueFull
	}
	return updated, ErrQueueFull
}

func (m *Manager) markTerminal(id, status string, result json.RawMessage, message string) {
	_, _, _ = m.store.Transition(id, StatusRunning, status, func(next *Invocation) {
		next.Result = result
		next.Error = message
		next.FinishedAt = nowRFC3339()
		next.UpdatedAt = next.FinishedAt
	})
}

func (m *Manager) Stop() {
	m.mu.Lock()
	m.stopping = true
	close(m.stop)
	cancels := make([]context.CancelFunc, 0, len(m.cancels))
	for _, cancel := range m.cancels {
		cancels = append(cancels, cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	m.wg.Wait()
}

func newID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
