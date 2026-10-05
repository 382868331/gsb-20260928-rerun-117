package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrQueueFull = errors.New("execution queue is full")

const defaultGroup = "default"

type queuedTask struct {
	id        string
	group     string
	cost      int
	createdAt string
}

// Manager schedules invocations against a unit-based capacity. Each task
// occupies cost units while running; queued tasks wait for enough free
// units. Capacity can be resized at runtime; shrinking never pretends
// running tasks are free, so used may temporarily exceed capacity.
type Manager struct {
	store     *Store
	registry  Registry
	queueSize int

	mu         sync.Mutex
	capacity   int
	used       int
	running    map[string]int
	cancels    map[string]context.CancelFunc
	queue      []queuedTask
	blocked    []string
	lastGroup  string
	stopping   bool
	stopCtx    context.Context
	stopCancel context.CancelFunc
	wg         sync.WaitGroup
}

type dependencyCheck struct {
	ready   bool
	basis   map[string]string
	reasons []DependencyWaitReason
	error   string
}

func (c dependencyCheck) waitingInfo(kind string) *WaitingInfo {
	return &WaitingInfo{
		Kind:      kind,
		Reasons:   append([]DependencyWaitReason(nil), c.reasons...),
		CheckedAt: nowRFC3339(),
	}
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
	stopCtx, stopCancel := context.WithCancel(context.Background())
	manager := &Manager{
		store:      store,
		registry:   registry,
		capacity:   capacity,
		queueSize:  queueSize,
		running:    make(map[string]int),
		cancels:    make(map[string]context.CancelFunc),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	return manager, nil
}

func (m *Manager) Start() {
	m.mu.Lock()
	m.blocked = nil
	m.queue = nil
	m.used = 0
	m.running = make(map[string]int)
	m.cancels = make(map[string]context.CancelFunc)
	records := m.store.List()
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt != records[j].CreatedAt {
			return records[i].CreatedAt < records[j].CreatedAt
		}
		return records[i].ID < records[j].ID
	})
	for _, record := range records {
		if record.Status == StatusBlocked {
			m.blocked = append(m.blocked, record.ID)
		}
	}
	m.mu.Unlock()

	queued := m.store.QueuedRecords()
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].CreatedAt != queued[j].CreatedAt {
			return queued[i].CreatedAt < queued[j].CreatedAt
		}
		return queued[i].ID < queued[j].ID
	})
	for _, record := range queued {
		cost, group := normalizedCostGroup(record.Cost, record.Group)
		m.mu.Lock()
		check := m.checkDependenciesLocked(record)
		admitted := false
		if check.ready {
			admitted = m.admitLocked(record.ID, group, cost, record.CreatedAt)
		} else {
			m.markBlockedLocked(record.ID, check)
		}
		m.mu.Unlock()
		if !check.ready {
			continue
		}
		if !admitted {
			m.finishRejected(record.ID, "execution queue is full after restart")
		}
	}
	m.mu.Lock()
	m.processBlockedLocked()
	m.mu.Unlock()
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
	if err := m.validateDependencies(req); err != nil {
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
	record := Invocation{
		ID:           newID(),
		Function:     req.Function,
		Argument:     argument,
		Cost:         cost,
		Group:        group,
		TimeoutMs:    timeoutMs,
		Dependencies: append([]Dependency(nil), req.Dependencies...),
		Status:       StatusBlocked,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	m.mu.Lock()
	capacity := m.capacity
	check := m.checkDependenciesLocked(record)
	if check.ready {
		record.Status = StatusQueued
		record.DependencyBasis = check.basis
	} else {
		record.Error = check.error
		record.Waiting = check.waitingInfo(WaitingKindDependencies)
	}
	m.mu.Unlock()
	if cost > capacity {
		return Invocation{}, fmt.Errorf("cost %d exceeds current capacity %d", cost, capacity)
	}
	if err := m.store.Create(record); err != nil {
		return Invocation{}, err
	}
	m.mu.Lock()
	admitted := false
	if check.ready {
		admitted = m.admitLocked(record.ID, group, cost, record.CreatedAt)
	} else {
		m.blocked = append(m.blocked, record.ID)
	}
	m.mu.Unlock()
	if !check.ready {
		return record, nil
	}
	if !admitted {
		return m.finishRejected(record.ID, "execution queue is full")
	}
	return record, nil
}

func (m *Manager) validateDependencies(req InvokeRequest) error {
	seen := make(map[string]bool)
	graph := make(map[string][]string)
	ids := make(map[string]bool)
	for _, record := range m.store.List() {
		ids[record.ID] = true
		graph[record.ID] = dependencyIDs(record.Dependencies)
	}
	candidateID := "__submitting__"
	ids[candidateID] = true
	for _, dependency := range req.Dependencies {
		if dependency.InvocationID == "" {
			return errors.New("dependency invocation_id is required")
		}
		if dependency.InvocationID == candidateID {
			return errors.New("task cannot depend on itself")
		}
		if !ids[dependency.InvocationID] {
			return fmt.Errorf("dependency %q does not exist", dependency.InvocationID)
		}
		if seen[dependency.InvocationID] {
			return fmt.Errorf("dependency %q is declared more than once", dependency.InvocationID)
		}
		seen[dependency.InvocationID] = true
	}
	graph[candidateID] = dependencyIDs(req.Dependencies)
	if cycle := findCyclePath(graph, candidateID); len(cycle) > 0 {
		return fmt.Errorf("dependency cycle detected: %s", strings.Join(cycle, " -> "))
	}
	return nil
}

func dependencyIDs(dependencies []Dependency) []string {
	ids := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		ids = append(ids, dependency.InvocationID)
	}
	return ids
}

// findCyclePath returns one concrete cycle path (e.g. a -> b -> a) when the
// dependency graph contains a cycle, or nil otherwise. Returning the path
// makes the rejection understandable to the caller.
func findCyclePath(graph map[string][]string, start string) []string {
	state := make(map[string]int)
	var stack []string
	var visit func(string) []string
	visit = func(id string) []string {
		if state[id] == 2 {
			return nil
		}
		if state[id] == 1 {
			for index, stacked := range stack {
				if stacked == id {
					return append(append([]string(nil), stack[index:]...), id)
				}
			}
			return []string{id, id}
		}
		state[id] = 1
		stack = append(stack, id)
		for _, next := range graph[id] {
			if cycle := visit(next); cycle != nil {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	ids := []string{start}
	for id := range graph {
		if id != start {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		if cycle := visit(id); cycle != nil {
			return cycle
		}
	}
	return nil
}

func (m *Manager) checkDependenciesLocked(record Invocation) dependencyCheck {
	basis := make(map[string]string)
	var reasons []DependencyWaitReason
	for _, dependency := range record.Dependencies {
		prerequisite, ok := m.store.Get(dependency.InvocationID)
		if !ok {
			reasons = append(reasons, DependencyWaitReason{
				InvocationID: dependency.InvocationID,
				Reason:       "dependency does not exist",
			})
			continue
		}
		if prerequisite.Status != StatusSucceeded {
			reasons = append(reasons, DependencyWaitReason{
				InvocationID: prerequisite.ID,
				Status:       prerequisite.Status,
				Reason:       fmt.Sprintf("dependency is %q and requires manual confirmation", prerequisite.Status),
			})
			continue
		}
		digest, missing, err := artifactDigest(prerequisite.Result, dependency.Artifacts)
		if err != nil || len(missing) > 0 {
			reason := fmt.Sprintf("artifacts are unavailable: %v", err)
			if err == nil {
				reason = "requested artifacts are missing from the dependency result"
			}
			reasons = append(reasons, DependencyWaitReason{
				InvocationID:     prerequisite.ID,
				Status:           prerequisite.Status,
				MissingArtifacts: missing,
				Reason:           reason,
			})
			continue
		}
		basis[dependency.InvocationID] = digest
	}
	if len(reasons) > 0 {
		summaries := make([]string, 0, len(reasons))
		for _, reason := range reasons {
			summaries = append(summaries, fmt.Sprintf("dependency %q: %s", reason.InvocationID, reason.Reason))
		}
		return dependencyCheck{reasons: reasons, error: strings.Join(summaries, "; ")}
	}
	return dependencyCheck{ready: true, basis: basis}
}

func artifactDigest(result json.RawMessage, artifacts []string) (string, []string, error) {
	var value any
	if len(result) == 0 {
		return "", nil, errors.New("missing result")
	}
	if err := json.Unmarshal(result, &value); err != nil {
		return "", nil, err
	}
	selected := make(map[string]any)
	var missing []string
	if len(artifacts) == 0 {
		selected["__result__"] = value
	} else {
		values, ok := value.(map[string]any)
		if !ok {
			return "", nil, errors.New("result is not a JSON object")
		}
		for _, artifact := range artifacts {
			artifactValue, ok := values[artifact]
			if !ok {
				missing = append(missing, artifact)
				continue
			}
			selected[artifact] = artifactValue
		}
	}
	if len(missing) > 0 {
		return "", missing, nil
	}
	encoded, err := json.Marshal(selected)
	if err != nil {
		return "", nil, err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil, nil
}

func (m *Manager) processBlockedLocked() {
	if len(m.blocked) == 0 {
		return
	}
	remaining := make([]string, 0, len(m.blocked))
	for _, id := range m.blocked {
		record, ok := m.store.Get(id)
		if !ok || record.Status != StatusBlocked {
			continue
		}
		check := m.checkDependenciesLocked(record)
		if !check.ready {
			m.updateBlockedReasonLocked(id, check, WaitingKindDependencies)
			remaining = append(remaining, id)
			continue
		}
		if !m.canEnterCapacityLocked(record.Cost) {
			m.updateBlockedReasonLocked(id, dependencyCheck{ready: true, error: "dependencies available; waiting for execution capacity"}, WaitingKindCapacity)
			remaining = append(remaining, id)
			continue
		}
		updated, transitioned, err := m.store.Transition(id, StatusBlocked, StatusQueued, func(next *Invocation) {
			next.DependencyBasis = check.basis
			next.Error = ""
			next.Waiting = nil
			next.UpdatedAt = nowRFC3339()
		})
		if err != nil || !transitioned {
			remaining = append(remaining, id)
			continue
		}
		if !m.admitLocked(updated.ID, updated.Group, updated.Cost, updated.CreatedAt) {
			m.finishRejectedLocked(id, "execution queue is full when dependencies became available")
		}
	}
	m.blocked = remaining
}

func (m *Manager) updateBlockedReasonLocked(id string, check dependencyCheck, kind string) {
	record, ok := m.store.Get(id)
	if !ok || record.Status != StatusBlocked {
		return
	}
	if record.Error == check.error && record.Waiting != nil && record.Waiting.Kind == kind && waitingReasonsEqual(record.Waiting.Reasons, check.reasons) {
		return
	}
	_, _, _ = m.store.Transition(id, StatusBlocked, StatusBlocked, func(next *Invocation) {
		next.Error = check.error
		next.Waiting = check.waitingInfo(kind)
		next.UpdatedAt = nowRFC3339()
	})
}

func waitingReasonsEqual(previous, current []DependencyWaitReason) bool {
	if len(previous) != len(current) {
		return false
	}
	for index := range previous {
		if previous[index].InvocationID != current[index].InvocationID ||
			previous[index].Status != current[index].Status ||
			previous[index].Reason != current[index].Reason ||
			!stringSlicesEqual(previous[index].MissingArtifacts, current[index].MissingArtifacts) {
			return false
		}
	}
	return true
}

func stringSlicesEqual(previous, current []string) bool {
	if len(previous) != len(current) {
		return false
	}
	for index := range previous {
		if previous[index] != current[index] {
			return false
		}
	}
	return true
}

func (m *Manager) markBlockedLocked(id string, check dependencyCheck) {
	record, ok := m.store.Get(id)
	if !ok {
		return
	}
	_, _, _ = m.store.Transition(id, record.Status, StatusBlocked, func(next *Invocation) {
		next.Error = check.error
		next.Waiting = check.waitingInfo(WaitingKindDependencies)
		next.DependencyBasis = nil
		next.UpdatedAt = nowRFC3339()
	})
	m.blocked = append(m.blocked, id)
}

func (m *Manager) finishRejectedLocked(id, message string) {
	_, _, _ = m.store.Transition(id, StatusQueued, StatusRejected, func(next *Invocation) {
		next.Error = message
		next.FinishedAt = nowRFC3339()
		next.UpdatedAt = next.FinishedAt
	})
}

// admitLocked either starts the task immediately or appends it to the
// bounded waiting queue. With a zero-size queue only tasks that can be
// scheduled right now are accepted.
func (m *Manager) admitLocked(id, group string, cost int, createdAt string) bool {
	if m.stopping {
		return false
	}
	if len(m.queue) == 0 && m.used+cost <= m.capacity {
		m.startLocked(id, cost)
		return true
	}
	if len(m.queue) >= m.queueSize {
		return false
	}
	m.queue = append(m.queue, queuedTask{id: id, group: group, cost: cost, createdAt: createdAt})
	sort.SliceStable(m.queue, func(i, j int) bool {
		if m.queue[i].createdAt != m.queue[j].createdAt {
			return m.queue[i].createdAt < m.queue[j].createdAt
		}
		return m.queue[i].id < m.queue[j].id
	})
	return true
}

func (m *Manager) canEnterCapacityLocked(cost int) bool {
	return len(m.queue) < m.queueSize && (len(m.queue) > 0 || m.used+cost <= m.capacity)
}

func (m *Manager) startLocked(id string, cost int) {
	record, ok := m.store.Get(id)
	if !ok {
		return
	}
	check := m.checkDependenciesLocked(record)
	if !check.ready {
		m.markBlockedLocked(id, check)
		return
	}
	ctx, cancel := context.WithTimeout(m.stopCtx, time.Duration(record.TimeoutMs)*time.Millisecond)
	m.cancels[id] = cancel
	record, transitioned, err := m.store.Transition(id, StatusQueued, StatusRunning, func(next *Invocation) {
		next.StartedAt = nowRFC3339()
		next.UpdatedAt = next.StartedAt
	})
	if err != nil || !transitioned {
		delete(m.cancels, id)
		cancel()
		return
	}
	m.used += cost
	m.running[id] = cost
	m.wg.Add(1)
	go m.execute(record, cost, ctx, cancel)
}

// scheduleLocked dispatches queued tasks while capacity allows. The next
// candidate is chosen fairly across groups (round-robin over the groups
// currently waiting, FIFO within a group). If the chosen task does not fit
// in the remaining capacity, the free units are reserved for it and nothing
// else is dispatched, so a steady stream of light tasks cannot starve a
// legitimate heavy task.
func (m *Manager) scheduleLocked() {
	for {
		if m.stopping {
			return
		}
		index := m.pickLocked()
		if index < 0 {
			return
		}
		task := m.queue[index]
		m.queue = append(m.queue[:index], m.queue[index+1:]...)
		m.startLocked(task.id, task.cost)
	}
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

// Explain builds a read-only dependency explanation graph for the given
// task, bound to the task record version observed at query time. It never
// mutates records, admits tasks, or confirms external results: it only
// classifies what each direct and indirect prerequisite is still missing.
// Shared prerequisites appear once; cyclic declarations are reported as
// understandable cycle paths instead of being followed forever.
func (m *Manager) Explain(id string) (ExplanationGraph, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	root, ok := m.store.Get(id)
	if !ok {
		return ExplanationGraph{}, false
	}
	checkedAt := nowRFC3339()
	graph := ExplanationGraph{
		Root:        root.ID,
		Version:     root.UpdatedAt,
		GeneratedAt: checkedAt,
	}
	visited := make(map[string]bool)
	edgeSeen := make(map[string]bool)
	onPath := make(map[string]int)
	var path []string
	var walk func(record Invocation)
	walk = func(record Invocation) {
		visited[record.ID] = true
		graph.Nodes = append(graph.Nodes, m.explainNodeLocked(record, checkedAt))
		onPath[record.ID] = len(path)
		path = append(path, record.ID)
		defer func() {
			delete(onPath, record.ID)
			path = path[:len(path)-1]
		}()
		for _, dependency := range record.Dependencies {
			dependencyID := dependency.InvocationID
			edgeKey := record.ID + "\x00" + dependencyID
			if !edgeSeen[edgeKey] {
				edgeSeen[edgeKey] = true
				graph.Edges = append(graph.Edges, m.explainEdgeLocked(record.ID, dependency))
			}
			if cycleStart, isCycle := onPath[dependencyID]; isCycle {
				cycle := append(append([]string(nil), path[cycleStart:]...), dependencyID)
				graph.Cycles = append(graph.Cycles, cycle)
				continue
			}
			if visited[dependencyID] {
				continue
			}
			prerequisite, found := m.store.Get(dependencyID)
			if !found {
				continue
			}
			walk(prerequisite)
		}
	}
	walk(root)
	return graph, true
}

// explainNodeLocked classifies what one task is currently waiting on
// without changing any state.
func (m *Manager) explainNodeLocked(record Invocation, checkedAt string) ExplanationNode {
	node := ExplanationNode{
		InvocationID: record.ID,
		Function:     record.Function,
		Status:       record.Status,
		Wait:         ExplainWaitNone,
	}
	switch record.Status {
	case StatusBlocked:
		check := m.checkDependenciesLocked(record)
		node.CheckedAt = checkedAt
		if check.ready && !m.canEnterCapacityLocked(record.Cost) {
			node.Wait = ExplainWaitCapacity
		} else {
			node.Wait = ExplainWaitDependencies
		}
	case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
		node.Wait = ExplainWaitExternalConfirmation
	case StatusQueued:
		node.Wait = ExplainWaitCapacity
	}
	return node
}

// explainEdgeLocked classifies one declared dependency edge against the
// current prerequisite record, read-only.
func (m *Manager) explainEdgeLocked(from string, dependency Dependency) ExplanationEdge {
	edge := ExplanationEdge{
		From:      from,
		To:        dependency.InvocationID,
		Artifacts: append([]string(nil), dependency.Artifacts...),
	}
	prerequisite, ok := m.store.Get(dependency.InvocationID)
	if !ok {
		edge.Status = EdgeMissingDependency
		edge.Reason = "dependency does not exist"
		return edge
	}
	switch prerequisite.Status {
	case StatusSucceeded:
		_, missing, err := artifactDigest(prerequisite.Result, dependency.Artifacts)
		switch {
		case err != nil:
			edge.Status = EdgeMissingArtifacts
			edge.Reason = fmt.Sprintf("artifacts are unavailable: %v", err)
		case len(missing) > 0:
			edge.Status = EdgeMissingArtifacts
			edge.MissingArtifacts = missing
			edge.Reason = "requested artifacts are missing from the dependency result"
		default:
			edge.Status = EdgeSatisfied
		}
	case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
		edge.Status = EdgeNeedsConfirmation
		edge.Reason = fmt.Sprintf("dependency is %q and requires manual confirmation", prerequisite.Status)
	default:
		edge.Status = EdgeWaiting
		edge.Reason = fmt.Sprintf("dependency is %q and has not finished", prerequisite.Status)
	}
	return edge
}

func (m *Manager) Cancel(id string) (Invocation, bool, error) {
	record, ok := m.store.Get(id)
	if !ok {
		return Invocation{}, false, nil
	}
	switch record.Status {
	case StatusBlocked:
		updated, transitioned, err := m.store.Transition(id, StatusBlocked, StatusCanceled, func(next *Invocation) {
			next.Error = "canceled while waiting for dependencies"
			next.Waiting = nil
			next.FinishedAt = nowRFC3339()
			next.UpdatedAt = next.FinishedAt
		})
		if transitioned {
			m.mu.Lock()
			m.removeBlockedLocked(id)
			m.processBlockedLocked()
			m.mu.Unlock()
		}
		return updated, transitioned, err
	case StatusQueued:
		updated, transitioned, err := m.store.Transition(id, StatusQueued, StatusCanceled, func(next *Invocation) {
			next.Error = "canceled before execution started"
			next.Waiting = nil
			next.FinishedAt = nowRFC3339()
			next.UpdatedAt = next.FinishedAt
		})
		if transitioned {
			m.mu.Lock()
			m.removeQueuedLocked(id)
			m.processBlockedLocked()
			m.scheduleLocked()
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
	m.processBlockedLocked()
	m.scheduleLocked()
	m.mu.Unlock()
	return nil
}

func (m *Manager) Capacity() CapacityResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	waitingDependencies := 0
	waitingCapacityAfterDependencies := 0
	for _, id := range m.blocked {
		record, ok := m.store.Get(id)
		if !ok {
			continue
		}
		check := m.checkDependenciesLocked(record)
		if check.ready && m.canEnterCapacityLocked(record.Cost) {
			continue
		}
		if check.ready {
			waitingCapacityAfterDependencies++
		} else {
			waitingDependencies++
		}
	}
	return CapacityResponse{
		Capacity:            m.capacity,
		Used:                m.used,
		Running:             len(m.running),
		Waiting:             len(m.queue) + len(m.blocked),
		WaitingCapacity:     len(m.queue) + waitingCapacityAfterDependencies,
		WaitingDependencies: waitingDependencies,
		Blocked:             len(m.blocked),
		QueueSize:           m.queueSize,
	}
}

func (m *Manager) ReplaceResult(id string, result json.RawMessage) (Invocation, bool, error) {
	record, ok := m.store.Get(id)
	if !ok {
		return Invocation{}, false, nil
	}
	switch record.Status {
	case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
	default:
		return record, false, fmt.Errorf("result can only replace %q, %q, %q, or %q outcomes", StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain)
	}
	var value any
	if err := json.Unmarshal(result, &value); err != nil {
		return Invocation{}, false, errors.New("result must be valid JSON")
	}
	m.mu.Lock()
	for _, dependent := range m.store.List() {
		if dependent.Status == StatusRunning && dependsOn(dependent.Dependencies, id) {
			m.mu.Unlock()
			return Invocation{}, false, errors.New("cannot replace result while affected downstream task is running")
		}
	}
	updated, transitioned, err := m.store.Transition(id, record.Status, StatusSucceeded, func(next *Invocation) {
		next.Result = append(json.RawMessage(nil), result...)
		next.Error = "result replaced by operator"
		next.UpdatedAt = nowRFC3339()
		if next.FinishedAt == "" {
			next.FinishedAt = next.UpdatedAt
		}
	})
	if err != nil || !transitioned {
		m.mu.Unlock()
		return updated, transitioned, err
	}
	m.processBlockedLocked()
	m.invalidateDependentsLocked(id)
	m.scheduleLocked()
	m.mu.Unlock()
	return updated, true, nil
}

func dependsOn(dependencies []Dependency, prerequisiteID string) bool {
	for _, dependency := range dependencies {
		if dependency.InvocationID == prerequisiteID {
			return true
		}
	}
	return false
}

func (m *Manager) invalidateDependentsLocked(changedID string) {
	for _, task := range m.queue {
		record, ok := m.store.Get(task.id)
		if !ok {
			continue
		}
		if !dependsOn(record.Dependencies, changedID) {
			continue
		}
		check := m.checkDependenciesLocked(record)
		if check.ready && basisMatches(record.DependencyBasis, check.basis) {
			continue
		}
		m.removeQueuedLocked(task.id)
		if check.ready {
			check = dependencyCheck{error: "dependency result replaced; dependency basis must be rechecked"}
		}
		m.markBlockedLocked(task.id, check)
	}
}

func basisMatches(previous, current map[string]string) bool {
	if len(previous) != len(current) {
		return false
	}
	for id, digest := range previous {
		if current[id] != digest {
			return false
		}
	}
	return true
}

func (m *Manager) removeQueuedLocked(id string) {
	for index, task := range m.queue {
		if task.id == id {
			m.queue = append(m.queue[:index], m.queue[index+1:]...)
			return
		}
	}
}

func (m *Manager) removeBlockedLocked(id string) {
	for index, blockedID := range m.blocked {
		if blockedID == id {
			m.blocked = append(m.blocked[:index], m.blocked[index+1:]...)
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

func (m *Manager) execute(record Invocation, cost int, ctx context.Context, cancel context.CancelFunc) {
	id := record.ID
	defer cancel()
	defer func() {
		m.mu.Lock()
		delete(m.running, id)
		delete(m.cancels, id)
		m.used -= cost
		m.processBlockedLocked()
		m.scheduleLocked()
		m.mu.Unlock()
		m.wg.Done()
	}()
	fn, ok := m.registry.Get(record.Function)
	if !ok {
		m.markFailed(id, fmt.Sprintf("unknown function %q", record.Function))
		return
	}
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
	m.stopCancel()
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
