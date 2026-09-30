package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitForStatus(t *testing.T, manager *Manager, id, want string) Invocation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, ok := manager.Get(id)
		if !ok {
			t.Fatal("missing invocation")
		}
		if record.Status == want {
			return record
		}
		time.Sleep(5 * time.Millisecond)
	}
	record, _ := manager.Get(id)
	t.Fatalf("status = %q, want %q", record.Status, want)
	return Invocation{}
}

func TestQueueFullRejectsWithoutUnboundedTasks(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{}
	started := make(chan string, 3)
	release := make(chan struct{})
	registry["block"] = func(ctx context.Context, argument json.RawMessage) (any, error) {
		var args map[string]string
		if err := json.Unmarshal(argument, &args); err != nil {
			return nil, err
		}
		started <- args["id"]
		select {
		case <-release:
			return map[string]any{"ok": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	manager, err := NewManager(store, registry, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	arg := func(id string) json.RawMessage {
		return json.RawMessage(`{"id":"` + id + `"}`)
	}
	first, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("first")})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("second")})
	if err != nil {
		t.Fatal(err)
	}
	third, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("third")})
	if err != ErrQueueFull {
		t.Fatalf("error = %v, want ErrQueueFull", err)
	}
	if third.Status != StatusRejected {
		t.Fatalf("third status = %q, want rejected", third.Status)
	}
	if got := manager.waitingCount(); got != 1 {
		t.Fatalf("waiting jobs = %d, want 1", got)
	}

	close(release)
	waitForStatus(t, manager, first.ID, StatusSucceeded)
	waitForStatus(t, manager, second.ID, StatusSucceeded)
	waitForStatus(t, manager, third.ID, StatusRejected)
}

func TestTimeoutIgnoresLateSuccess(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"late": func(ctx context.Context, _ json.RawMessage) (any, error) {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			return map[string]any{"unexpected": "success"}, nil
		},
	}
	manager, err := NewManager(store, registry, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	invocation, err := manager.Submit(InvokeRequest{Function: "late", TimeoutMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	record := waitForStatus(t, manager, invocation.ID, StatusTimedOut)
	time.Sleep(50 * time.Millisecond)
	record, _ = manager.Get(invocation.ID)
	if record.Status != StatusTimedOut || len(record.Result) != 0 {
		t.Fatalf("record = %+v, want timed out without result", record)
	}
}

func TestSlotsRecycleFastAndSlowCalls(t *testing.T) {
	store := newTestStore(t)
	var active int
	var maxActive int
	var mu sync.Mutex
	registry := Registry{
		"mixed": func(ctx context.Context, argument json.RawMessage) (any, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			defer func() {
				mu.Lock()
				active--
				mu.Unlock()
			}()
			var args struct {
				DelayMs int `json:"delay_ms"`
			}
			if err := json.Unmarshal(argument, &args); err != nil {
				return nil, err
			}
			timer := time.NewTimer(time.Duration(args.DelayMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	manager, err := NewManager(store, registry, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	var ids []string
	for _, delay := range []int{150, 1, 1, 120, 1, 100, 1} {
		invocation, err := manager.Submit(InvokeRequest{
			Function:  "mixed",
			Argument:  json.RawMessage([]byte(`{"delay_ms":` + itoa(delay) + `}`)),
			TimeoutMs: 1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, invocation.ID)
	}
	for _, id := range ids {
		waitForStatus(t, manager, id, StatusSucceeded)
	}
	if maxActive > 2 {
		t.Fatalf("max concurrent calls = %d, want at most 2", maxActive)
	}
}

func TestRestartDistinguishesCompletedAndUncertain(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"echo": echoFunction,
	}
	now := nowRFC3339()
	completed := Invocation{ID: "completed", Function: "echo", Status: StatusSucceeded, Result: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now, FinishedAt: now}
	running := Invocation{ID: "running", Function: "echo", Status: StatusRunning, CreatedAt: now, UpdatedAt: now, StartedAt: now}
	if err := store.Create(completed); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(running); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManager(store, registry, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)

	completedRecord, _ := manager.Get("completed")
	if completedRecord.Status != StatusSucceeded {
		t.Fatalf("completed = %q, want succeeded", completedRecord.Status)
	}
	uncertainRecord := waitForStatus(t, manager, "running", StatusUncertain)
	if uncertainRecord.Error == "" {
		t.Fatal("uncertain record lacks an explanation")
	}
}

func TestHTTPCancelRunningInvocation(t *testing.T) {
	store := newTestStore(t)
	registry := Registry{
		"wait": waitFunction,
	}
	manager, err := NewManager(store, registry, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(manager.Stop)
	handler := NewRouter(manager)

	request := httptest.NewRequest(http.MethodPost, "/v1/invocations", jsonBody(`{"function":"wait","argument":{"ms":1000},"timeout_ms":2000}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var invocation Invocation
	if err := json.NewDecoder(response.Result().Body).Decode(&invocation); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, invocation.ID, StatusRunning)
	cancelRequest := httptest.NewRequest(http.MethodPost, "/v1/invocations/"+invocation.ID+"/cancel", nil)
	cancelResponse := httptest.NewRecorder()
	handler.ServeHTTP(cancelResponse, cancelRequest)
	if cancelResponse.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, want %d", cancelResponse.Code, http.StatusOK)
	}
	waitForStatus(t, manager, invocation.ID, StatusCanceled)
}

func TestZeroQueueAcceptsFirstRequestImmediately(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	registry := Registry{
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	manager, err := NewManager(store, registry, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()

	first, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		manager.Stop()
		t.Fatalf("first request was rejected with idle slot: %v", err)
	}
	second, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != ErrQueueFull {
		manager.Stop()
		t.Fatalf("second error = %v, want ErrQueueFull", err)
	}
	if second.Status != StatusRejected {
		manager.Stop()
		t.Fatalf("second status = %q, want rejected", second.Status)
	}
	if got := manager.waitingCount(); got != 0 {
		t.Fatalf("waiting = %d, want 0 for zero-size queue", got)
	}
	close(release)
	waitForStatus(t, manager, first.ID, StatusSucceeded)
	manager.Stop()

	if got := manager.waitingCount(); got != 0 {
		t.Fatalf("waiting after stop = %d, want 0", got)
	}
	if got := manager.runningCount(); got != 0 {
		t.Fatalf("running after stop = %d, want 0", got)
	}
}

func TestConcurrentSubmissionsRespectZeroQueue(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	registry := Registry{
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	manager, err := NewManager(store, registry, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(func() {
		close(release)
		manager.Stop()
	})

	const total = 16
	type submitResult struct {
		id     string
		err    error
		status string
	}
	results := make(chan submitResult, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			invocation, submitErr := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
			results <- submitResult{id: invocation.ID, err: submitErr, status: invocation.Status}
		}()
	}
	wg.Wait()
	close(results)

	accepted, rejected := 0, 0
	for result := range results {
		if result.err == nil {
			accepted++
			if result.status != StatusQueued {
				t.Fatalf("accepted request status = %q, want queued", result.status)
			}
			continue
		}
		if result.err != ErrQueueFull || result.status != StatusRejected {
			t.Fatalf("result = %+v, want rejected ErrQueueFull", result)
		}
		rejected++
	}
	if accepted != 2 || rejected != total-2 {
		t.Fatalf("accepted = %d, rejected = %d, want 2 and %d", accepted, rejected, total-2)
	}
	if got := manager.waitingCount(); got != 0 {
		t.Fatalf("waiting = %d, want 0", got)
	}
}

func TestImmediateCancelReleasesWaitingCapacity(t *testing.T) {
	store := newTestStore(t)
	release := make(chan struct{})
	registry := Registry{
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	manager, err := NewManager(store, registry, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(func() {
		close(release)
		manager.Stop()
	})

	running, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, running.ID, StatusRunning)
	waiting, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		t.Fatal(err)
	}
	overflow, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != ErrQueueFull {
		t.Fatalf("overflow error = %v, want ErrQueueFull", err)
	}
	waitForStatus(t, manager, overflow.ID, StatusRejected)

	canceled, transitioned, err := manager.Cancel(waiting.ID)
	if err != nil || !transitioned {
		t.Fatalf("cancel = %+v %v, want transition", canceled, err)
	}
	if got := manager.waitingCount(); got != 0 {
		t.Fatalf("waiting after cancel = %d, want 0", got)
	}
	replacement, err := manager.Submit(InvokeRequest{Function: "block", TimeoutMs: 5000})
	if err != nil {
		t.Fatalf("replacement rejected: %v", err)
	}
	if got := manager.waitingCount(); got != 1 {
		t.Fatalf("waiting = %d, want 1", got)
	}
	if _, _, err := manager.Cancel(replacement.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, manager, canceled.ID, StatusCanceled)
	waitForStatus(t, manager, replacement.ID, StatusCanceled)
}

func TestStopAndRestartReprocessesWaitingQueue(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "state.json")
	registry := Registry{
		"echo": echoFunction,
		"wait": waitFunction,
	}

	firstStore, err := OpenStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	firstManager, err := NewManager(firstStore, registry, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	firstManager.Start()
	running, err := firstManager.Submit(InvokeRequest{Function: "wait", Argument: json.RawMessage(`{"ms":10000}`), TimeoutMs: 30000})
	if err != nil {
		t.Fatal(err)
	}
	waitingOne, err := firstManager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"one"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitingTwo, err := firstManager.Submit(InvokeRequest{Function: "echo", Argument: json.RawMessage(`{"text":"two"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, firstManager, running.ID, StatusRunning)
	firstManager.Stop()

	secondStore, err := OpenStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	secondManager, err := NewManager(secondStore, registry, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	secondManager.Start()
	t.Cleanup(secondManager.Stop)

	waitForStatus(t, secondManager, running.ID, StatusCanceled)
	waitForStatus(t, secondManager, waitingOne.ID, StatusSucceeded)
	waitForStatus(t, secondManager, waitingTwo.ID, StatusSucceeded)
	if got := secondManager.waitingCount(); got != 0 {
		t.Fatalf("waiting after restart drained = %d, want 0", got)
	}
	if got := secondManager.runningCount(); got != 0 {
		t.Fatalf("running after restart drained = %d, want 0", got)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
