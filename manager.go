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
	id       string
	group    string
	cost     int
	resolved []ResolvedDependency
}

type blockedTask struct {
	group    string
	cost     int
	ready    bool
	resolved []ResolvedDependency
}

// Manager schedules invocations against a unit-based capacity. Each task
// occupies cost units while running; queued tasks wait for enough free
// units. Capacity can be resized at runtime; shrinking never pretends
// running tasks are free, so used may temporarily exceed capacity.
type Manager struct {
	store     *Store
	registry  Registry
	queueSize int

	mu        sync.Mutex
	capacity  int
	used      int
	running   map[string]int
	cancels   map[string]context.CancelFunc
	queue     []queuedTask
	blocked   map[string]blockedTask
	lastGroup string
	stopping  bool
	wg        sync.WaitGroup
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
		running:   make(map[string]int),
		cancels:   make(map[string]context.CancelFunc),
		blocked:   make(map[string]blockedTask),
	}
	return manager, nil
}

func (m *Manager) Start() {
	queued := m.store.WaitingRecords()
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreatedAt != queued[j].CreatedAt {
			return queued[i].CreatedAt < queued[j].CreatedAt
		}
		return queued[i].ID < queued[j].ID
	})
	for _, record := range queued {
		cost, group := normalizedCostGroup(record.Cost, record.Group)
		if record.Status == StatusBlocked {
			resolved, ready := m.checkDependencies(record)
			m.mu.Lock()
			m.blocked[record.ID] = blockedTask{group: group, cost: cost, ready: ready, resolved: resolved}
			if ready {
				m.promoteReadyBlockedLocked(record)
				m.scheduleLocked()
			}
			m.mu.Unlock()
			_ = m.noteBlockedReason(record.ID, ready)
			continue
		}
		m.mu.Lock()
		resolved, _ := m.checkDependencies(record)
		resolvedReady := len(record.DependsOn) == 0 || resolved != nil
		admitted := false
		if resolvedReady {
			admitted = m.admitLocked(record.ID, group, cost, resolved)
		} else {
			m.blocked[record.ID] = blockedTask{group: group, cost: cost}
		}
		m.mu.Unlock()
		if !resolvedReady {
			if _, _, err := m.store.Transition(record.ID, StatusQueued, StatusBlocked, func(next *Invocation) {
				next.ResolvedDependencies = nil
				next.Error = "waiting for prerequisite results"
				next.UpdatedAt = nowRFC3339()
			}); err != nil {
				return
			}
			continue
		}
		if !admitted {
			m.finishRejected(record.ID, "execution queue is full after restart")
		}
	}
}

func normalizedCostGroup(cost int, group string) (int, string) {
	if cost <= 0 {
		cost = 1
	}
	if group == "" {
		group = defaultGroup
	}
	return cost, group
}

func (m *Manager) Submit(req InvokeRequest) (Invocation, error) {
	if req.Function == "" {
		return Invocation{}, errors.New("function is required")
	}
	if _, ok := m.registry.Get(req.Function); !ok {
		return Invocation{}, fmt.Errorf("unknown function %q", req.Function)
	}
	if req.Cost < 0 {
		return Invocation{}, errors.New("cost must be non-negative")
	}
	if err := validateDependencies(req.DependsOn); err != nil {
		return Invocation{}, err
	}
	cost, group := normalizedCostGroup(req.Cost, req.Group)
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
	id := newID()
	if err := m.validateDependencyGraph(id, req.DependsOn); err != nil {
		return Invocation{}, err
	}
	record := Invocation{
		ID:        id,
		Function:  req.Function,
		Argument:  argument,
		Cost:      cost,
		Group:     group,
		DependsOn: append([]DependencyRequirement(nil), req.DependsOn...),
		TimeoutMs: timeoutMs,
		Status:    StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.mu.Lock()
	capacity := m.capacity
	m.mu.Unlock()
	if cost > capacity {
		return Invocation{}, fmt.Errorf("cost %d exceeds current capacity %d", cost, capacity)
	}
	if err := m.store.Create(record); err != nil {
		return Invocation{}, err
	}
	m.mu.Lock()
	resolved, dependenciesReady := m.checkDependencies(record)
	admitted := true
	if dependenciesReady {
		admitted = m.admitLocked(record.ID, group, cost, resolved)
	} else {
		m.blocked[record.ID] = blockedTask{group: group, cost: cost}
	}
	m.mu.Unlock()
	if !admitted {
		return m.finishRejected(record.ID, "execution queue is full")
	}
	if !dependenciesReady {
		if _, _, err := m.store.Transition(record.ID, StatusQueued, StatusBlocked, func(next *Invocation) {
			next.Error = "waiting for prerequisite results"
			next.UpdatedAt = nowRFC3339()
		}); err != nil {
			return Invocation{}, err
		}
		record.Status = StatusBlocked
	}
	return record, nil
}

// admitLocked either starts the task immediately or appends it to the
// bounded waiting queue. With a zero-size queue only tasks that can be
// scheduled right now are accepted.
func (m *Manager) admitLocked(id, group string, cost int, resolved []ResolvedDependency) bool {
	if m.stopping {
		return false
	}
	if len(m.queue) == 0 && m.used+cost <= m.capacity {
		m.startLocked(id, cost, resolved)
		return true
	}
	if len(m.queue) >= m.queueSize {
		return false
	}
	m.queue = append(m.queue, queuedTask{id: id, group: group, cost: cost, resolved: resolved})
	return true
}

func (m *Manager) startLocked(id string, cost int, resolved []ResolvedDependency) {
	m.used += cost
	m.running[id] = cost
	m.wg.Add(1)
	go m.execute(id, cost, resolved)
}

func (m *Manager) checkDependencies(record Invocation) ([]ResolvedDependency, bool) {
	if len(record.DependsOn) == 0 {
		return nil, true
	}
	resolved := make([]ResolvedDependency, 0, len(record.DependsOn))
	for _, dep := range record.DependsOn {
		prerequisite, ok := m.store.Get(dep.ID)
		if !ok || prerequisite.Status != StatusSucceeded || !artifactPresent(prerequisite.Result, dep.Artifact) {
			return nil, false
		}
		resolved = append(resolved, ResolvedDependency{
			ID:              dep.ID,
			Artifact:        dep.Artifact,
			SourceUpdatedAt: prerequisite.UpdatedAt,
		})
	}
	return resolved, true
}

func artifactPresent(result json.RawMessage, artifact string) bool {
	if artifact == "" {
		return len(result) > 0
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(result, &values); err != nil {
		return false
	}
	value, ok := values[artifact]
	return ok && !isNullJSON(value)
}

func isNullJSON(value json.RawMessage) bool {
	return string(value) == "null"
}

func (m *Manager) canAdmitLocked(cost int) bool {
	if m.stopping {
		return false
	}
	return (len(m.queue) == 0 && m.used+cost <= m.capacity) || len(m.queue) < m.queueSize
}

func (m *Manager) promoteReadyBlockedLocked(records ...Invocation) {
	var candidates []Invocation
	if len(records) > 0 {
		candidates = records
	} else {
		for _, record := range m.storeListReadyBlockedLocked() {
			candidates = append(candidates, record)
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].CreatedAt != candidates[j].CreatedAt {
				return candidates[i].CreatedAt < candidates[j].CreatedAt
			}
			return candidates[i].ID < candidates[j].ID
		})
	}
	for _, record := range candidates {
		cost, group := normalizedCostGroup(record.Cost, record.Group)
		entry, ok := m.blocked[record.ID]
		if !ok || !entry.ready || !m.canAdmitLocked(cost) {
			continue
		}
		_, transitioned, err := m.store.Transition(record.ID, StatusBlocked, StatusQueued, func(next *Invocation) {
			next.ResolvedDependencies = append([]ResolvedDependency(nil), entry.resolved...)
			next.Error = ""
			next.UpdatedAt = nowRFC3339()
		})
		if err != nil || !transitioned {
			continue
		}
		delete(m.blocked, record.ID)
		m.admitLocked(record.ID, group, cost, entry.resolved)
	}
}

func (m *Manager) storeListReadyBlockedLocked() map[string]Invocation {
	candidates := make(map[string]Invocation)
	for id, entry := range m.blocked {
		if !entry.ready {
			continue
		}
		if record, ok := m.store.Get(id); ok {
			candidates[id] = record
		}
	}
	return candidates
}

func (m *Manager) advanceDependents(prerequisiteID string) {
	dependents := m.store.List()
	sort.Slice(dependents, func(i, j int) bool {
		if dependents[i].CreatedAt != dependents[j].CreatedAt {
			return dependents[i].CreatedAt < dependents[j].CreatedAt
		}
		return dependents[i].ID < dependents[j].ID
	})
	for _, record := range dependents {
		if record.Status != StatusBlocked || !hasDependency(record.DependsOn, prerequisiteID) {
			continue
		}
		resolved, ready := m.checkDependencies(record)
		m.mu.Lock()
		entry, ok := m.blocked[record.ID]
		if ok {
			entry.ready = ready
			entry.resolved = resolved
			m.blocked[record.ID] = entry
		}
		if ready {
			m.promoteReadyBlockedLocked(record)
			m.scheduleLocked()
		}
		m.mu.Unlock()
		_ = m.noteBlockedReason(record.ID, ready)
	}
}

func (m *Manager) resolvedDependenciesStillValid(resolved []ResolvedDependency) bool {
	for _, snapshot := range resolved {
		prerequisite, ok := m.store.Get(snapshot.ID)
		if !ok || prerequisite.Status != StatusSucceeded || prerequisite.UpdatedAt != snapshot.SourceUpdatedAt || !artifactPresent(prerequisite.Result, snapshot.Artifact) {
			return false
		}
	}
	return true
}

func hasDependency(deps []DependencyRequirement, id string) bool {
	for _, dep := range deps {
		if dep.ID == id {
			return true
		}
	}
	return false
}

func (m *Manager) noteBlockedReason(id string, ready bool) error {
	if ready {
		return nil
	}
	_, _, err := m.store.Transition(id, StatusBlocked, StatusBlocked, func(next *Invocation) {
		next.Error = "waiting for prerequisite results"
		next.UpdatedAt = nowRFC3339()
	})
	return err
}

func validateDependencies(deps []DependencyRequirement) error {
	seen := make(map[string]bool)
	for _, dep := range deps {
		if dep.ID == "" {
			return errors.New("dependency id is required")
		}
		if seen[dep.ID] {
			return fmt.Errorf("duplicate dependency %q", dep.ID)
		}
		seen[dep.ID] = true
	}
	return nil
}

func (m *Manager) validateDependencyGraph(id string, deps []DependencyRequirement) error {
	visiting := map[string]bool{id: true}
	visited := make(map[string]bool)
	var visit func(string) error
	visit = func(current string) error {
		if visiting[current] {
			return errors.New("dependency cycle detected")
		}
		if visited[current] {
			return nil
		}
		visiting[current] = true
		record, ok := m.store.Get(current)
		if ok {
			for _, dep := range record.DependsOn {
				if _, exists := m.store.Get(dep.ID); !exists {
					return fmt.Errorf("dependency %q not found", dep.ID)
				}
				if err := visit(dep.ID); err != nil {
					return err
				}
			}
		}
		delete(visiting, current)
		visited[current] = true
		return nil
	}
	for _, dep := range deps {
		if _, ok := m.store.Get(dep.ID); !ok {
			return fmt.Errorf("dependency %q not found", dep.ID)
		}
		if err := visit(dep.ID); err != nil {
			return err
		}
	}
	return nil
}

// scheduleLocked dispatches queued tasks while capacity allows. The next
// candidate is chosen fairly across groups (round-robin over the groups
// currently waiting, FIFO within a group). If the chosen task does not fit
// in the remaining capacity, the free units are reserved for it and nothing
// else is dispatched, so a steady stream of light tasks cannot starve a
// legitimate heavy task.
func (m *Manager) scheduleLocked() {
	for {
		index := m.pickLocked()
		if index < 0 {
			return
		}
		task := m.queue[index]
		if !m.resolvedDependenciesStillValid(task.resolved) {
			m.queue = append(m.queue[:index], m.queue[index+1:]...)
			m.returnQueuedTaskToBlockedLocked(task)
			continue
		}
		m.queue = append(m.queue[:index], m.queue[index+1:]...)
		m.startLocked(task.id, task.cost, task.resolved)
	}
}

func (m *Manager) returnQueuedTaskToBlockedLocked(task queuedTask) {
	_, _, _ = m.store.Transition(task.id, StatusQueued, StatusBlocked, func(next *Invocation) {
		next.ResolvedDependencies = nil
		next.Error = "waiting for prerequisite results"
		next.UpdatedAt = nowRFC3339()
	})
	m.blocked[task.id] = blockedTask{group: task.group, cost: task.cost}
}

func (m *Manager) pickLocked() int {
	if len(m.queue) == 0 {
		return -1
	}
	groups := make([]string, 0, len(m.queue))
	seen := make(map[string]bool)
	for _, task := range m.queue {
		if !seen[task.group] {
			seen[task.group] = true
			groups = append(groups, task.group)
		}
	}
	start := 0
	for i, group := range groups {
		if group == m.lastGroup {
			start = i + 1
			break
		}
	}
	for offset := 0; offset < len(groups); offset++ {
		group := groups[(start+offset)%len(groups)]
		for index, task := range m.queue {
			if task.group != group {
				continue
			}
			if m.used+task.cost > m.capacity {
				return -1
			}
			m.lastGroup = group
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
	case StatusBlocked:
		updated, transitioned, err := m.store.Transition(id, StatusBlocked, StatusCanceled, func(next *Invocation) {
			next.Error = "canceled while waiting for prerequisites"
			next.FinishedAt = nowRFC3339()
			next.UpdatedAt = next.FinishedAt
		})
		if transitioned {
			m.mu.Lock()
			delete(m.blocked, id)
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

func (m *Manager) SetCapacity(capacity int) error {
	if capacity < 0 {
		return errors.New("capacity must be non-negative")
	}
	m.mu.Lock()
	m.capacity = capacity
	m.scheduleLocked()
	m.promoteReadyBlockedLocked()
	m.scheduleLocked()
	m.mu.Unlock()
	return nil
}

func (m *Manager) Capacity() CapacityResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return CapacityResponse{
		Capacity:            m.capacity,
		Used:                m.used,
		Running:             len(m.running),
		Waiting:             len(m.queue) + m.readyBlockedCountLocked(),
		WaitingDependencies: len(m.blocked) - m.readyBlockedCountLocked(),
		QueueSize:           m.queueSize,
	}
}

func (m *Manager) readyBlockedCountLocked() int {
	count := 0
	for _, entry := range m.blocked {
		if entry.ready {
			count++
		}
	}
	return count
}

func (m *Manager) removeQueuedLocked(id string) {
	for index, task := range m.queue {
		if task.id == id {
			m.queue = append(m.queue[:index], m.queue[index+1:]...)
			return
		}
	}
}

func (m *Manager) waitingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue)
}

func (m *Manager) runningCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.running)
}

func (m *Manager) execute(id string, cost int, resolved []ResolvedDependency) {
	defer func() {
		m.mu.Lock()
		delete(m.running, id)
		delete(m.cancels, id)
		m.used -= cost
		m.scheduleLocked()
		m.promoteReadyBlockedLocked()
		m.scheduleLocked()
		m.mu.Unlock()
		m.wg.Done()
		m.advanceDependents(id)
	}()
	record, ok, err := m.store.Transition(id, StatusQueued, StatusRunning, func(next *Invocation) {
		next.StartedAt = nowRFC3339()
		next.ResolvedDependencies = append([]ResolvedDependency(nil), resolved...)
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

func (m *Manager) ReplaceResult(id string, req ResultReplacementRequest) (Invocation, bool, error) {
	if len(req.Result) == 0 || !json.Valid(req.Result) {
		return Invocation{}, false, errors.New("valid result JSON is required")
	}
	record, ok := m.store.Get(id)
	if !ok {
		return Invocation{}, false, nil
	}
	switch record.Status {
	case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
	default:
		return record, false, errors.New("only failed, canceled, rejected, timed out, or uncertain results can be replaced")
	}
	updated, _, err := m.store.Transition(id, record.Status, StatusSucceeded, func(next *Invocation) {
		next.Result = append(json.RawMessage(nil), req.Result...)
		next.Error = ""
		next.Resolution = req.Basis
		next.UpdatedAt = nowRFC3339()
		if next.FinishedAt == "" {
			next.FinishedAt = next.UpdatedAt
		}
	})
	if err != nil {
		return Invocation{}, false, err
	}
	m.mu.Lock()
	m.scheduleLocked()
	m.promoteReadyBlockedLocked()
	m.scheduleLocked()
	m.mu.Unlock()
	m.advanceDependents(id)
	return updated, true, nil
}

func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		m.wg.Wait()
		return
	}
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
