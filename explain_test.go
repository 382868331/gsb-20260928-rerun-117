package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func submitWithDeps(t *testing.T, manager *Manager, function string, dependencies ...Dependency) Invocation {
	t.Helper()
	record, err := manager.Submit(InvokeRequest{
		Function:     function,
		Argument:     json.RawMessage(`{"text":"x"}`),
		Dependencies: dependencies,
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func nodeByID(t *testing.T, graph ExplanationGraph, id string) ExplanationNode {
	t.Helper()
	for _, node := range graph.Nodes {
		if node.InvocationID == id {
			return node
		}
	}
	t.Fatalf("missing node %q", id)
	return ExplanationNode{}
}

func edgeBetween(t *testing.T, graph ExplanationGraph, from, to string) ExplanationEdge {
	t.Helper()
	for _, edge := range graph.Edges {
		if edge.From == from && edge.To == to {
			return edge
		}
	}
	t.Fatalf("missing edge %q -> %q", from, to)
	return ExplanationEdge{}
}

func countNodes(graph ExplanationGraph, id string) int {
	count := 0
	for _, node := range graph.Nodes {
		if node.InvocationID == id {
			count++
		}
	}
	return count
}

func TestExplainDiamondSharedPrerequisiteShownOnce(t *testing.T) {
	store := newTestStore(t)
	manager, err := NewManager(store, DefaultRegistry(), 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	base, err := manager.Submit(InvokeRequest{Function: "add", Argument: json.RawMessage(`{"a":1,"b":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, base.ID, StatusSucceeded)
	left := submitWithDeps(t, manager, "echo", Dependency{InvocationID: base.ID, Artifacts: []string{"sum", "missing"}})
	right := submitWithDeps(t, manager, "echo", Dependency{InvocationID: base.ID, Artifacts: []string{"sum"}})
	join := submitWithDeps(t, manager, "echo",
		Dependency{InvocationID: left.ID},
		Dependency{InvocationID: right.ID},
	)

	graph, ok := manager.Explain(join.ID)
	if !ok {
		t.Fatal("expected explanation graph")
	}
	if graph.Root != join.ID || graph.Version != join.UpdatedAt {
		t.Fatalf("graph bound to root=%q version=%q, want %q/%q", graph.Root, graph.Version, join.ID, join.UpdatedAt)
	}
	if len(graph.Nodes) != 4 {
		t.Fatalf("nodes = %d, want 4 (diamond shared prerequisite once)", len(graph.Nodes))
	}
	if count := countNodes(graph, base.ID); count != 1 {
		t.Fatalf("shared prerequisite appears %d times, want 1", count)
	}
	if len(graph.Edges) != 4 {
		t.Fatalf("edges = %d, want 4", len(graph.Edges))
	}
	if len(graph.Cycles) != 0 {
		t.Fatalf("unexpected cycles: %v", graph.Cycles)
	}
	if got := nodeByID(t, graph, join.ID).Wait; got != ExplainWaitDependencies {
		t.Fatalf("join wait = %q, want dependencies", got)
	}
	missingEdge := edgeBetween(t, graph, left.ID, base.ID)
	if missingEdge.Status != EdgeMissingArtifacts || len(missingEdge.MissingArtifacts) != 1 || missingEdge.MissingArtifacts[0] != "missing" {
		t.Fatalf("left->base edge = %+v, want missing artifact", missingEdge)
	}
	if got := edgeBetween(t, graph, right.ID, base.ID).Status; got != EdgeSatisfied {
		t.Fatalf("right->base edge = %q, want satisfied", got)
	}
	if got := edgeBetween(t, graph, join.ID, left.ID).Status; got != EdgeWaiting {
		t.Fatalf("join->left edge = %q, want waiting", got)
	}
}

func TestExplainMultipleFailuresAndPartialUnblock(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"ok":   func(_ context.Context, _ json.RawMessage) (any, error) { return map[string]any{"done": true}, nil },
		"fail": func(_ context.Context, _ json.RawMessage) (any, error) { return nil, errors.New("boom") },
	}
	manager, err := NewManager(store, registry, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	failA, _ := manager.Submit(InvokeRequest{Function: "fail", Argument: json.RawMessage(`{}`)})
	failB, _ := manager.Submit(InvokeRequest{Function: "fail", Argument: json.RawMessage(`{}`)})
	independent, _ := manager.Submit(InvokeRequest{Function: "ok", Argument: json.RawMessage(`{}`)})
	waitForStatus(t, manager, failA.ID, StatusFailed)
	waitForStatus(t, manager, failB.ID, StatusFailed)
	independentDone := waitForStatus(t, manager, independent.ID, StatusSucceeded)

	waiting := submitWithDeps(t, manager, "ok",
		Dependency{InvocationID: failA.ID},
		Dependency{InvocationID: failB.ID},
	)

	before, ok := manager.Explain(waiting.ID)
	if !ok {
		t.Fatal("expected explanation graph")
	}
	for _, prerequisite := range []string{failA.ID, failB.ID} {
		edge := edgeBetween(t, before, waiting.ID, prerequisite)
		if edge.Status != EdgeNeedsConfirmation {
			t.Fatalf("edge to %q = %q, want needs_confirmation", prerequisite, edge.Status)
		}
		if got := nodeByID(t, before, prerequisite).Wait; got != ExplainWaitExternalConfirmation {
			t.Fatalf("node %q wait = %q, want external_confirmation", prerequisite, got)
		}
	}

	if _, _, err := manager.ReplaceResult(failA.ID, json.RawMessage(`{"done":true}`)); err != nil {
		t.Fatal(err)
	}
	after, ok := manager.Explain(waiting.ID)
	if !ok {
		t.Fatal("expected explanation graph")
	}
	if after.Version == before.Version {
		t.Fatal("graph version must advance with the root record")
	}
	if got := edgeBetween(t, after, waiting.ID, failA.ID).Status; got != EdgeSatisfied {
		t.Fatalf("unblocked edge = %q, want satisfied", got)
	}
	if got := edgeBetween(t, after, waiting.ID, failB.ID).Status; got != EdgeNeedsConfirmation {
		t.Fatalf("unaffected edge = %q, want needs_confirmation", got)
	}
	record, _ := manager.Get(waiting.ID)
	if record.Status != StatusBlocked || record.Waiting == nil || len(record.Waiting.Reasons) != 1 {
		t.Fatalf("waiting record = %+v, want one remaining reason", record)
	}
	// The previously returned graph is a snapshot and stays unchanged.
	if got := edgeBetween(t, before, waiting.ID, failA.ID).Status; got != EdgeNeedsConfirmation {
		t.Fatalf("old graph mutated: edge = %q", got)
	}
	// The independent task finished once and is not rerun.
	current, _ := manager.Get(independent.ID)
	if current.StartedAt != independentDone.StartedAt || current.FinishedAt != independentDone.FinishedAt || current.Status != StatusSucceeded {
		t.Fatalf("independent task changed: %+v", current)
	}
}

func TestExplainCapacityShrinkWaitsOnCapacity(t *testing.T) {
	store := newTestStore(t)
	gate := make(chan struct{})
	registry := Registry{
		"slow": func(ctx context.Context, _ json.RawMessage) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		"gated": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-gate:
				return map[string]any{"done": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	manager, err := NewManager(store, registry, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	occupant, _ := manager.Submit(InvokeRequest{Function: "slow", Argument: json.RawMessage(`{}`), Cost: 1})
	waitForStatus(t, manager, occupant.ID, StatusRunning)
	dep, _ := manager.Submit(InvokeRequest{Function: "gated", Argument: json.RawMessage(`{}`), Cost: 1})
	waitForStatus(t, manager, dep.ID, StatusRunning)
	waiting := submitWithDeps(t, manager, "gated", Dependency{InvocationID: dep.ID})
	waitForWaitingKind(t, manager, waiting.ID, WaitingKindDependencies)

	// Shrink so that no unit is free once the dependency completes.
	if err := manager.SetCapacity(1); err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitForStatus(t, manager, dep.ID, StatusSucceeded)
	waitForWaitingKind(t, manager, waiting.ID, WaitingKindCapacity)

	graph, ok := manager.Explain(waiting.ID)
	if !ok {
		t.Fatal("expected explanation graph")
	}
	if got := nodeByID(t, graph, waiting.ID).Wait; got != ExplainWaitCapacity {
		t.Fatalf("wait = %q, want capacity after shrink", got)
	}
	if got := edgeBetween(t, graph, waiting.ID, dep.ID).Status; got != EdgeSatisfied {
		t.Fatalf("edge = %q, want satisfied", got)
	}
}

func TestExplainCancelAndRestart(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"slow": func(ctx context.Context, _ json.RawMessage) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	manager, err := NewManager(store, registry, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	running, _ := manager.Submit(InvokeRequest{Function: "slow", Argument: json.RawMessage(`{}`)})
	waitForStatus(t, manager, running.ID, StatusRunning)
	canceled, _ := manager.Submit(InvokeRequest{Function: "slow", Argument: json.RawMessage(`{}`)})
	waitForStatus(t, manager, canceled.ID, StatusRunning)
	if _, _, err := manager.Cancel(canceled.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, canceled.ID, StatusCanceled)
	blocked := submitWithDeps(t, manager, "slow",
		Dependency{InvocationID: running.ID},
		Dependency{InvocationID: canceled.ID},
	)

	// Reopen against the same state file without stopping the old manager,
	// simulating a crash: running becomes uncertain, blocked keeps waiting.
	reopened, err := OpenStore(store.path)
	if err != nil {
		t.Fatal(err)
	}
	manager2, err := NewManager(reopened, registry, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager2.Start()
	t.Cleanup(manager2.Stop)

	record, _ := manager2.Get(blocked.ID)
	if record.Status != StatusBlocked {
		t.Fatalf("blocked status after restart = %q", record.Status)
	}
	graph, ok := manager2.Explain(blocked.ID)
	if !ok {
		t.Fatal("expected explanation graph")
	}
	if got := nodeByID(t, graph, running.ID).Status; got != StatusUncertain {
		t.Fatalf("running node status = %q, want uncertain", got)
	}
	for _, prerequisite := range []string{running.ID, canceled.ID} {
		if got := edgeBetween(t, graph, blocked.ID, prerequisite).Status; got != EdgeNeedsConfirmation {
			t.Fatalf("edge to %q = %q, want needs_confirmation", prerequisite, got)
		}
	}
	// Explaining must not confirm anything or trigger execution.
	if got := manager2.runningCount(); got != 0 {
		t.Fatalf("explanation triggered execution: running = %d", got)
	}
	uncertainRecord, _ := manager2.Get(running.ID)
	if uncertainRecord.Status != StatusUncertain {
		t.Fatalf("explanation confirmed external result: %q", uncertainRecord.Status)
	}
}

func TestExplainCyclePathReported(t *testing.T) {
	store := newTestStore(t)
	manager, err := NewManager(store, DefaultRegistry(), 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	// Craft a cyclic declaration directly in the store to verify the graph
	// walker reports an understandable cycle path instead of looping.
	cyclicA := Invocation{
		ID:           "cyclic-a",
		Function:     "echo",
		Status:       StatusBlocked,
		CreatedAt:    nowRFC3339(),
		UpdatedAt:    nowRFC3339(),
		Dependencies: []Dependency{{InvocationID: "cyclic-b"}},
	}
	cyclicB := Invocation{
		ID:           "cyclic-b",
		Function:     "echo",
		Status:       StatusBlocked,
		CreatedAt:    nowRFC3339(),
		UpdatedAt:    nowRFC3339(),
		Dependencies: []Dependency{{InvocationID: "cyclic-a"}},
	}
	if err := store.Create(cyclicA); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(cyclicB); err != nil {
		t.Fatal(err)
	}
	graph, ok := manager.Explain("cyclic-a")
	if !ok {
		t.Fatal("expected explanation graph")
	}
	if len(graph.Cycles) != 1 {
		t.Fatalf("cycles = %v, want one", graph.Cycles)
	}
	cycle := strings.Join(graph.Cycles[0], " -> ")
	if cycle != "cyclic-a -> cyclic-b -> cyclic-a" {
		t.Fatalf("cycle path = %q", cycle)
	}
	if len(graph.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(graph.Nodes))
	}

	// Submit-time validation rejects cycles with the concrete path.
	_, err = manager.Submit(InvokeRequest{
		Function:     "echo",
		Argument:     json.RawMessage(`{}`),
		Dependencies: []Dependency{{InvocationID: "cyclic-b"}},
	})
	if err == nil || !strings.Contains(err.Error(), "cyclic-b -> cyclic-a -> cyclic-b") {
		t.Fatalf("cycle error = %v, want path", err)
	}
}

func TestExplainIsReadOnly(t *testing.T) {
	store := newTestStore(t)
	manager, err := NewManager(store, DefaultRegistry(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	slow, err := manager.Submit(InvokeRequest{Function: "wait", Argument: json.RawMessage(`{"ms":30000}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, slow.ID, StatusRunning)
	blocked := submitWithDeps(t, manager, "echo", Dependency{InvocationID: slow.ID})
	before, _ := manager.Get(blocked.ID)
	capacityBefore := manager.Capacity()

	if _, ok := manager.Explain(blocked.ID); !ok {
		t.Fatal("expected explanation graph")
	}
	after, _ := manager.Get(blocked.ID)
	if after.UpdatedAt != before.UpdatedAt || after.Status != before.Status {
		t.Fatalf("explanation mutated record: before=%+v after=%+v", before, after)
	}
	capacityAfter := manager.Capacity()
	if capacityAfter != capacityBefore {
		t.Fatalf("explanation changed capacity view: %+v -> %+v", capacityBefore, capacityAfter)
	}
	if _, ok := manager.Explain("missing-id"); ok {
		t.Fatal("expected no graph for unknown id")
	}
}
