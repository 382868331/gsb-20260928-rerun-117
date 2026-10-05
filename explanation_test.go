package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func blockRegistry(release <-chan struct{}) Registry {
	return Registry{
		"echo": echoFunction,
		"fail": func(context.Context, json.RawMessage) (any, error) { return nil, errors.New("boom") },
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
}

func explanationNode(t *testing.T, explanation DependencyExplanation, id string) ExplanationNode {
	t.Helper()
	for _, node := range explanation.Nodes {
		if node.InvocationID == id {
			return node
		}
	}
	t.Fatalf("explanation missing node %q: %+v", id, explanation.Nodes)
	return ExplanationNode{}
}

func TestExplanationDiamondSharesPrerequisiteOnce(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	manager, err := NewManager(store, blockRegistry(release), 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	base, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, base.ID, StatusRunning)
	left, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"l"}`), Dependencies: []Dependency{{InvocationID: base.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"r"}`), Dependencies: []Dependency{{InvocationID: base.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	join, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"j"}`), Dependencies: []Dependency{
		{InvocationID: left.ID, Artifacts: []string{"text"}},
		{InvocationID: right.ID, Artifacts: []string{"text"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, join.ID, StatusBlocked)

	joinRecord, _ := manager.Get(join.ID)
	explanation, ok := manager.Explain(join.ID)
	if !ok {
		t.Fatal("explanation not found")
	}
	if explanation.TaskVersion != joinRecord.UpdatedAt {
		t.Fatalf("task version = %q, want %q", explanation.TaskVersion, joinRecord.UpdatedAt)
	}
	if len(explanation.Nodes) != 4 {
		t.Fatalf("nodes = %d, want 4 (shared prerequisite listed once): %+v", len(explanation.Nodes), explanation.Nodes)
	}
	baseCount := 0
	for _, node := range explanation.Nodes {
		if node.InvocationID == base.ID {
			baseCount++
		}
	}
	if baseCount != 1 {
		t.Fatalf("shared prerequisite appears %d times, want 1", baseCount)
	}
	root := explanationNode(t, explanation, join.ID)
	if root.Kind != WaitingKindDependencies || root.CheckedAt == "" {
		t.Fatalf("root node = %+v, want dependency wait with check time", root)
	}
	if len(root.Edges) != 2 || root.Edges[0].Satisfied || root.Edges[1].Satisfied {
		t.Fatalf("root edges = %+v, want two unsatisfied edges", root.Edges)
	}
	leftNode := explanationNode(t, explanation, left.ID)
	if len(leftNode.Edges) != 1 || leftNode.Edges[0].InvocationID != base.ID {
		t.Fatalf("left edges = %+v, want edge to shared base", leftNode.Edges)
	}

	snapshot := func() map[string]Invocation {
		records := make(map[string]Invocation)
		for _, record := range store.List() {
			records[record.ID] = record
		}
		return records
	}
	before := snapshot()
	if _, ok := manager.Explain(join.ID); !ok {
		t.Fatal("second explanation not found")
	}
	after := snapshot()
	if len(before) != len(after) {
		t.Fatalf("read-only query changed record count: %d -> %d", len(before), len(after))
	}
	for id, previous := range before {
		current := after[id]
		if previous.Status != current.Status || previous.UpdatedAt != current.UpdatedAt {
			t.Fatalf("read-only query mutated record %+v -> %+v", previous, current)
		}
	}

	close(release)
	waitForStatus(t, manager, join.ID, StatusSucceeded)
}

func TestExplanationMultipleFailuresAndTargetedUpdate(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	close(release)
	manager, err := NewManager(store, blockRegistry(release), 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	independent, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"free"}`), TimeoutMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	independentDone := waitForStatus(t, manager, independent.ID, StatusSucceeded)
	failedOne, err := manager.Submit(InvokeRequest{Function: "fail", TimeoutMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	failedTwo, err := manager.Submit(InvokeRequest{Function: "fail", TimeoutMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, failedOne.ID, StatusFailed)
	waitForStatus(t, manager, failedTwo.ID, StatusFailed)
	dependent, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"dep"}`), TimeoutMs: 1000, Dependencies: []Dependency{
		{InvocationID: failedOne.ID, Artifacts: []string{"text"}},
		{InvocationID: failedTwo.ID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, dependent.ID, StatusBlocked)

	first, ok := manager.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found")
	}
	root := explanationNode(t, first, dependent.ID)
	if root.Kind != WaitingKindDependencies || len(root.Edges) != 2 {
		t.Fatalf("root = %+v, want dependency wait with two edges", root)
	}
	for _, id := range []string{failedOne.ID, failedTwo.ID} {
		node := explanationNode(t, first, id)
		if node.Kind != ExplanationKindExternalConfirmation {
			t.Fatalf("node %q kind = %q, want external confirmation", id, node.Kind)
		}
	}
	frozen, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := manager.ReplaceResult(failedOne.ID, json.RawMessage(`{"text":"fixed"}`)); err != nil || !ok {
		t.Fatalf("replace first result ok=%v err=%v", ok, err)
	}
	second, ok := manager.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found after replace")
	}
	root = explanationNode(t, second, dependent.ID)
	var fixedEdge, pendingEdge *ExplanationEdge
	for index := range root.Edges {
		switch root.Edges[index].InvocationID {
		case failedOne.ID:
			fixedEdge = &root.Edges[index]
		case failedTwo.ID:
			pendingEdge = &root.Edges[index]
		}
	}
	if fixedEdge == nil || !fixedEdge.Satisfied || fixedEdge.Basis == "" {
		t.Fatalf("fixed edge = %+v, want satisfied with basis", fixedEdge)
	}
	if pendingEdge == nil || pendingEdge.Satisfied {
		t.Fatalf("pending edge = %+v, want still blocked", pendingEdge)
	}
	if !strings.Contains(string(frozen), "requires manual confirmation") {
		t.Fatalf("old explanation snapshot changed: %s", frozen)
	}
	independentAfter, _ := manager.Get(independent.ID)
	if independentAfter.StartedAt != independentDone.StartedAt || independentAfter.FinishedAt != independentDone.FinishedAt || independentAfter.Status != StatusSucceeded {
		t.Fatalf("independent task reran: before %+v after %+v", independentDone, independentAfter)
	}

	if _, ok, err := manager.ReplaceResult(failedTwo.ID, json.RawMessage(`{"text":"fixed2"}`)); err != nil || !ok {
		t.Fatalf("replace second result ok=%v err=%v", ok, err)
	}
	waitForStatus(t, manager, dependent.ID, StatusSucceeded)
}

func TestExplanationCapacityWaitAndShrink(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	manager, err := NewManager(store, blockRegistry(release), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	prerequisite, err := manager.Submit(InvokeRequest{Function: "fail", TimeoutMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, prerequisite.ID, StatusFailed)
	occupant, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, occupant.ID, StatusRunning)
	dependent, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"dep"}`), TimeoutMs: 1000, Dependencies: []Dependency{{InvocationID: prerequisite.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, dependent.ID, StatusBlocked)
	if _, ok, err := manager.ReplaceResult(prerequisite.ID, json.RawMessage(`{"text":"base"}`)); err != nil || !ok {
		t.Fatalf("replace result ok=%v err=%v", ok, err)
	}
	waitForWaitingKind(t, manager, dependent.ID, WaitingKindCapacity)

	explanation, ok := manager.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found")
	}
	root := explanationNode(t, explanation, dependent.ID)
	if root.Kind != WaitingKindCapacity {
		t.Fatalf("root kind = %q, want capacity wait: %+v", root.Kind, root)
	}
	base := explanationNode(t, explanation, prerequisite.ID)
	if base.Kind != "" || base.Status != StatusSucceeded {
		t.Fatalf("satisfied prerequisite = %+v, want no wait kind", base)
	}

	if err := manager.SetCapacity(0); err != nil {
		t.Fatal(err)
	}
	shrunk, ok := manager.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found after shrink")
	}
	if got := explanationNode(t, shrunk, dependent.ID); got.Kind != WaitingKindCapacity {
		t.Fatalf("after shrink kind = %q, want capacity wait", got.Kind)
	}
	if got := manager.Capacity(); got.Used != 1 {
		t.Fatalf("shrunk capacity used = %d, want running task still occupied", got.Used)
	}
	close(release)
	if err := manager.SetCapacity(1); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, dependent.ID, StatusSucceeded)
}

func TestExplanationSurvivesCancelAndRestart(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"wait": waitFunction,
		"echo": echoFunction,
	}
	manager, err := NewManager(store, registry, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()

	prerequisite, err := manager.Submit(InvokeRequest{Function: "wait", Argument: json.RawMessage(`{"ms":10000}`), TimeoutMs: 20000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, prerequisite.ID, StatusRunning)
	dependent, err := manager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"dep"}`), TimeoutMs: 1000, Dependencies: []Dependency{{InvocationID: prerequisite.ID, Artifacts: []string{"waited_ms"}}}})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, dependent.ID, StatusBlocked)
	if _, _, err := manager.Cancel(prerequisite.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, prerequisite.ID, StatusCanceled)

	explanation, ok := manager.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found")
	}
	node := explanationNode(t, explanation, prerequisite.ID)
	if node.Kind != ExplanationKindExternalConfirmation {
		t.Fatalf("canceled prerequisite kind = %q, want external confirmation", node.Kind)
	}
	manager.Stop()

	restarted, err := NewManager(store, registry, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Start()
	t.Cleanup(restarted.Stop)
	waitForStatus(t, restarted, dependent.ID, StatusBlocked)
	afterRestart, ok := restarted.Explain(dependent.ID)
	if !ok {
		t.Fatal("explanation not found after restart")
	}
	node = explanationNode(t, afterRestart, prerequisite.ID)
	if node.Kind != ExplanationKindExternalConfirmation {
		t.Fatalf("after restart kind = %q, want external confirmation", node.Kind)
	}
	dependentRecord, _ := restarted.Get(dependent.ID)
	if afterRestart.TaskVersion != dependentRecord.UpdatedAt {
		t.Fatalf("task version = %q, want %q", afterRestart.TaskVersion, dependentRecord.UpdatedAt)
	}
	if _, ok, err := restarted.ReplaceResult(prerequisite.ID, json.RawMessage(`{"waited_ms":10000}`)); err != nil || !ok {
		t.Fatalf("replace after restart ok=%v err=%v", ok, err)
	}
	waitForStatus(t, restarted, dependent.ID, StatusSucceeded)
}

func TestExplanationReportsCyclePath(t *testing.T) {
	store := newTestStore(t)
	now := nowRFC3339()
	first := Invocation{ID: "cycle-a", Function: "echo", Cost: 1, Group: "default", Status: StatusBlocked, CreatedAt: now, UpdatedAt: now, Dependencies: []Dependency{{InvocationID: "cycle-b"}}}
	second := Invocation{ID: "cycle-b", Function: "echo", Cost: 1, Group: "default", Status: StatusBlocked, CreatedAt: now, UpdatedAt: now, Dependencies: []Dependency{{InvocationID: "cycle-a"}}}
	if err := store.Create(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(second); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, Registry{"echo": echoFunction}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	explanation, ok := manager.Explain("cycle-a")
	if !ok {
		t.Fatal("explanation not found")
	}
	node := explanationNode(t, explanation, "cycle-b")
	if len(node.Edges) != 1 || len(node.Edges[0].Cycle) == 0 {
		t.Fatalf("expected cycle path on edge, got %+v", node.Edges)
	}
	cycle := node.Edges[0].Cycle
	if cycle[0] != "cycle-a" || cycle[len(cycle)-1] != "cycle-a" {
		t.Fatalf("cycle path = %v, want readable loop back to cycle-a", cycle)
	}
	if len(explanation.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2 despite cycle", len(explanation.Nodes))
	}
}

func TestHTTPExplanationEndpoint(t *testing.T) {
	store := newTestStore(t)
	manager, err := NewManager(store, Registry{"fail": func(context.Context, json.RawMessage) (any, error) { return nil, errors.New("boom") }}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)
	handler := NewRouter(manager)

	request := httptest.NewRequest(http.MethodPost, "/v1/invocations", jsonBody(`{"function":"fail","timeout_ms":1000}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var created InvokeResponse
	if err := json.NewDecoder(response.Result().Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, created.ID, StatusFailed)

	dependent, err := manager.Submit(InvokeRequest{Function: "fail", TimeoutMs: 1000, Dependencies: []Dependency{{InvocationID: created.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, dependent.ID, StatusBlocked)

	explainRequest := httptest.NewRequest(http.MethodGet, "/v1/invocations/"+dependent.ID+"/explanation", nil)
	explainResponse := httptest.NewRecorder()
	handler.ServeHTTP(explainResponse, explainRequest)
	if explainResponse.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", explainResponse.Code)
	}
	var explanation DependencyExplanation
	if err := json.NewDecoder(explainResponse.Result().Body).Decode(&explanation); err != nil {
		t.Fatal(err)
	}
	if explanation.Root != dependent.ID || len(explanation.Nodes) != 2 {
		t.Fatalf("explanation = %+v", explanation)
	}
	if explanationNode(t, explanation, created.ID).Kind != ExplanationKindExternalConfirmation {
		t.Fatalf("failed prerequisite not marked for external confirmation: %+v", explanation.Nodes)
	}

	missingRequest := httptest.NewRequest(http.MethodGet, "/v1/invocations/missing/explanation", nil)
	missingResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingResponse, missingRequest)
	if missingResponse.Code != http.StatusNotFound {
		t.Fatalf("missing explanation status = %d, want 404", missingResponse.Code)
	}
}
