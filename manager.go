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

const defaultGroup = "default"

type queuedTask struct {
	id     string
	weight int
	group  string
}

type Manager struct {
	store     *Store
	registry  Registry
	queueSize int

	mu            sync.Mutex
	capacity      int
	runningWeight int
	queue         []queuedTask
	cancels       map[string]context.CancelFunc
	lastGroup     string
	stopping      bool
	wg            sync.WaitGroup
}

func NewManager(store *Store, registry Registry, capacity, queueSize int) (*Manager, error) {
	if capacity <= 0 {
		capacity = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	if err := store.MarkRunningUncertain(); err != nil {
		return nil, err
	}
	manager := &Manager{
		store:     store,
		registry:  registry,
		capacity:  capacity,
		queueSize: queueSize,
		cancels:   make(map[string]context.CancelFunc),
	}
	return manager, nil
}

func (m *Manager) Start() {
	queued := m.store.QueuedRecords()
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreatedAt != queued[j].CreatedAt {
			return queued[i].CreatedAt < queued[j].CreatedAt
		}
		return queued[i].ID < queued[j].ID
	})
	for _, record := range queued {
		weight := normalizeWeight(record.Weight)
		if weight > m.capacity || !m.admit(record.ID, weight, record.Group) {
			m.finishRejected(record.ID, "execution queue is full after restart")
		}
	}
}

func normalizeWeight(weight int) int {
	if weight <= 0 {
		return 1
	}
	return weight
}

func normalizeGroup(group string) string {
	if group == "" {
		return defaultGroup
	}
	return group
}

func (m *Manager) Submit(req InvokeRequest) (Invocation, error) {
	if req.Function == "" {
		return Invocation{}, errors.New("function is required")
	}
	if _, ok := m.registry.Get(req.Function); !ok {
		return Invocation{}, fmt.Errorf("unknown function %q", req.Function)
	}
	weight := normalizeWeight(req.Weight)
	m.mu.Lock()
	capacity := m.capacity
	m.mu.Unlock()
	if weight > capacity {
		return Invocation{}, fmt.Errorf("weight %d exceeds current capacity %d", weight, capacity)
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
	group := normalizeGroup(req.Group)
	now := nowRFC3339()
	record := Invocation{
		ID:        newID(),
		Function:  req.Function,
		Argument:  argument,
		Weight:    weight,
		Group:     group,
		TimeoutMs: timeoutMs,
		Status:    StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := m.store.Create(record); err != nil {
		return Invocation{}, err
	}
	if !m.admit(record.ID, weight, group) {
		return m.finishRejected(record.ID, "execution queue is full")
	}
	return record, nil
}

// admit either starts the task immediately or places it in the bounded
// waiting queue. Immediate start is only allowed when the queue is empty
// and the task fits the truly remaining capacity.
func (m *Manager) admit(id string, weight int, group string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		return false
	}
	if len(m.queue) == 0 && m.runningWeight+weight <= m.capacity {
		m.runningWeight += weight
		m.wg.Add(1)
		go m.execute(id, weight)
		return true
	}
	if len(m.queue) >= m.queueSize {
		return false
	}
	m.queue = append(m.queue, queuedTask{id: id, weight: weight, group: normalizeGroup(group)})
	return true
}

// scheduleLocked drains the waiting queue while tasks fit. The oldest
// queued task acts as a starvation guard: if it does not fit, nothing
// behind it may take capacity, so a legitimate heavy task always gets its
// chance once running tasks finish. Among the remaining candidates the
// heads of each group are rotated round-robin for fairness across groups.
func (m *Manager) scheduleLocked() {
	if m.stopping {
		return
	}
	for len(m.queue) > 0 {
		available := m.capacity - m.runningWeight
		if m.queue[0].weight > available {
			return
		}
		index := m.pickLocked(available)
		if index < 0 {
			return
		}
		task := m.queue[index]
		m.queue = append(m.queue[:index], m.queue[index+1:]...)
		m.runningWeight += task.weight
		m.lastGroup = task.group
		m.wg.Add(1)
		go m.execute(task.id, task.weight)
	}
}

// pickLocked selects the next queued task that fits, rotating across task
// groups so one group cannot monopolize freed capacity.
func (m *Manager) pickLocked(available int) int {
	headByGroup := make(map[string]int)
	var groupOrder []string
	for index, task := range m.queue {
		if _, seen := headByGroup[task.group]; !seen {
			headByGroup[task.group] = index
			groupOrder = append(groupOrder, task.group)
		}
	}
	start := 0
	if m.lastGroup != "" {
		for i, group := range groupOrder {
			if group == m.lastGroup {
				start = (i + 1) % len(groupOrder)
				break
			}
		}
	}
	for offset := 0; offset < len(groupOrder); offset++ {
		group := groupOrder[(start+offset)%len(groupOrder)]
		index := headByGroup[group]
		if m.queue[index].weight <= available {
			return index
		}
	}
	return -1
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
			m.mu.Lock()
			m.removeQueuedLocked(id)
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

func (m *Manager) removeQueuedLocked(id string) {
	for index, task := range m.queue {
		if task.id == id {
			m.queue = append(m.queue[:index], m.queue[index+1:]...)
			return
		}
	}
}

// Capacity reports the configured capacity, the weight held by running
// tasks, and the remaining capacity (negative when shrunk below usage).
func (m *Manager) Capacity() (capacity, running, available int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.capacity, m.runningWeight, m.capacity - m.runningWeight
}

// SetCapacity adjusts the available capacity at runtime. Running tasks
// keep their weight; shrinking never frees them implicitly, and growth
// immediately unblocks fitting queued tasks.
func (m *Manager) SetCapacity(capacity int) error {
	if capacity <= 0 {
		return errors.New("capacity must be positive")
	}
	m.mu.Lock()
	m.capacity = capacity
	m.scheduleLocked()
	m.mu.Unlock()
	return nil
}

func (m *Manager) waitingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue)
}

func (m *Manager) runningCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	m.store.mu.RLock()
	defer m.store.mu.RUnlock()
	for _, record := range m.store.records {
		if record.Status == StatusRunning {
			count++
		}
	}
	return count
}

// execute runs a single admitted task. It is the only place that releases
// the task weight, so cancellation, timeout, and capacity changes can
// never double-free or double-count capacity.
func (m *Manager) execute(id string, weight int) {
	defer m.wg.Done()
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		m.mu.Lock()
		m.runningWeight -= weight
		delete(m.cancels, id)
		m.scheduleLocked()
		m.mu.Unlock()
	}
	defer release()

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
	if m.stopping {
		m.mu.Unlock()
		cancel()
		m.markTerminal(id, StatusCanceled, nil, "execution canceled")
		return
	}
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
	case <-ctx.Done():
		errMessage := "execution timed out"
		terminalStatus := StatusTimedOut
		if errors.Is(ctx.Err(), context.Canceled) {
			errMessage = "execution canceled"
			terminalStatus = StatusCanceled
		}
		m.markTerminal(id, terminalStatus, nil, errMessage)
		return
	}
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

func (m *Manager) finishRejected(id, message string) (Invocation, error) {
	updated, _, err := m.store.Transition(id, StatusQueued, StatusRejected, func(next *Invocation) {
		next.Error = message
		next.FinishedAt = nowRFC3339()
		next.UpdatedAt = next.FinishedAt
	})
	if err != nil {
		return Invocation{}, err
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
