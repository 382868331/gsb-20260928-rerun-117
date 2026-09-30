package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
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
	active, waiting := manager.capacitySnapshot()
	if active != 1 || waiting != 1 {
		t.Fatalf("capacity = %d active / %d waiting, want 1 / 1", active, waiting)
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

func TestZeroQueueAcceptsFirstSubmitBeforeWorkerScheduled(t *testing.T) {
	// The original failure was a scheduler-dependent select/default: with one
	// free slot and a zero-length wait queue, the very first Submit right after
	// Start could be rejected before any worker consumed a job. Repeat it to
	// also exercise concurrent submissions.
	for attempt := 0; attempt < 50; attempt++ {
		store := newTestStore(t)
		registry := Registry{
			"echo": echoFunction,
		}
		manager, err := NewManager(store, registry, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		manager.Start()
		// Submit immediately after Start, before exercising any wait: the free
		// slot must accept it instead of reporting "queue full".
		first, submitErr := manager.Submit(InvokeRequest{
			Function: "echo",
			Argument: json.RawMessage(`{"text":"immediate"}`),
		})
		if submitErr != nil {
			manager.Stop()
			t.Fatalf("attempt %d: first submit rejected: %v", attempt, submitErr)
		}
		waitForStatus(t, manager, first.ID, StatusSucceeded)
		active, waiting := manager.capacitySnapshot()
		if active != 0 || waiting != 0 {
			manager.Stop()
			t.Fatalf("attempt %d: capacity = %d / %d, want 0 / 0", attempt, active, waiting)
		}
		manager.Stop()
	}
}

func TestZeroQueueImmediateConcurrentSubmitsTerminalsAndAccounting(t *testing.T) {
	store := newTestStore(t)
	started := make(chan struct{})
	var startOnce sync.Once
	release := make(chan struct{})
	var releaseOnce sync.Once
	registry := Registry{
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			startOnce.Do(func() { close(started) })
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
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		manager.Stop()
	})

	first, err := manager.Submit(InvokeRequest{Function: "block"})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	// With the sole slot busy and a zero wait queue, further submits must be
	// rejected with terminal records and must not consume any capacity.
	const extra = 16
	var wg sync.WaitGroup
	for i := 0; i < extra; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, submitErr := manager.Submit(InvokeRequest{Function: "block"})
			if submitErr != ErrQueueFull {
				t.Errorf("submit error = %v, want ErrQueueFull", submitErr)
			}
		}()
	}
	wg.Wait()
	active, waiting := manager.capacitySnapshot()
	if active != 1 || waiting != 0 {
		t.Fatalf("capacity = %d active / %d waiting, want 1 / 0", active, waiting)
	}

	releaseOnce.Do(func() { close(release) })
	waitForStatus(t, manager, first.ID, StatusSucceeded)

	// Slot must be reusable after the only running call finished.
	second, err := manager.Submit(InvokeRequest{Function: "block"})
	if err != nil {
		t.Fatalf("submit after slot freed: %v", err)
	}
	waitForStatus(t, manager, second.ID, StatusSucceeded)
}

func TestQueuedCancelRefundsExactlyOneWaitSlot(t *testing.T) {
	store := newTestStore(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	registry := Registry{
		"block": func(ctx context.Context, argument json.RawMessage) (any, error) {
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
		},
	}
	manager, err := NewManager(store, registry, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager.Start()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		manager.Stop()
	})

	arg := func(id string) json.RawMessage {
		return json.RawMessage(`{"id":"` + id + `"}`)
	}
	first, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("first")})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("queued")})
	if err != nil {
		t.Fatal(err)
	}

	// Immediate cancel of the waiting request must refund its wait slot, so a
	// subsequent submit can occupy the wait queue instead of being rejected.
	canceled, transitioned, err := manager.Cancel(queued.ID)
	if err != nil || !transitioned {
		t.Fatalf("cancel = %+v %v %v", canceled, transitioned, err)
	}
	waitForStatus(t, manager, queued.ID, StatusCanceled)
	active, waiting := manager.capacitySnapshot()
	if active != 1 || waiting != 0 {
		t.Fatalf("capacity after queued cancel = %d / %d, want 1 / 0", active, waiting)
	}
	replacement, err := manager.Submit(InvokeRequest{Function: "block", Argument: arg("replacement")})
	if err != nil {
		t.Fatalf("resubmit after cancel: %v", err)
	}
	_, overErr := manager.Submit(InvokeRequest{Function: "block", Argument: arg("overflow")})
	if overErr != ErrQueueFull {
		t.Fatalf("overflow error = %v, want ErrQueueFull", overErr)
	}

	releaseOnce.Do(func() { close(release) })
	waitForStatus(t, manager, first.ID, StatusSucceeded)
	waitForStatus(t, manager, replacement.ID, StatusSucceeded)
	active, waiting = manager.capacitySnapshot()
	if active != 0 || waiting != 0 {
		t.Fatalf("capacity after drain = %d / %d, want 0 / 0", active, waiting)
	}
}

func TestRestartReopensQueueWithCorrectAccounts(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "state.json")
	store, err := OpenStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	reopened := make(chan struct{})
	var reopenOnce sync.Once
	registry := Registry{
		"block": func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-reopened:
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
	running, err := manager.Submit(InvokeRequest{Function: "block"})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := manager.Submit(InvokeRequest{Function: "block"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	manager.Stop()

	// Reopen with the same durable state: the previously running request is
	// uncertain, while the queued request must re-enter the bounded wait queue.
	store2, err := OpenStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	manager2, err := NewManager(store2, registry, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager2.Start()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		reopenOnce.Do(func() { close(reopened) })
		manager2.Stop()
	})
	// Stop canceled the in-flight invocation durably; an unclean restart would
	// instead have marked it uncertain (covered separately).
	waitForStatus(t, manager2, running.ID, StatusCanceled)
	// The re-queued request is promoted immediately and needs a signal to finish.
	waitForStatus(t, manager2, queued.ID, StatusRunning)
	reopenOnce.Do(func() { close(reopened) })
	waitForStatus(t, manager2, queued.ID, StatusSucceeded)
	active, waiting := manager2.capacitySnapshot()
	if active != 0 || waiting != 0 {
		t.Fatalf("capacity after reopened drain = %d / %d, want 0 / 0", active, waiting)
	}
}

func TestTimeoutLateSuccessNeverOverturnsTerminal(t *testing.T) {
	// Run repeatedly because the original bug was a select race: when both
	// ctx.Done and the buffered success result were ready, the late success
	// could be chosen and overwrite the timeout with a succeeded result.
	if !testing.Short() {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	for attempt := 0; attempt < 100; attempt++ {
		store := newTestStore(t)
		registry := Registry{
			"late": func(ctx context.Context, _ json.RawMessage) (any, error) {
				<-ctx.Done()
				return map[string]any{"unexpected": "success"}, nil
			},
		}
		manager, err := NewManager(store, registry, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		manager.Start()
		invocation, err := manager.Submit(InvokeRequest{Function: "late", TimeoutMs: 1})
		if err != nil {
			manager.Stop()
			t.Fatal(err)
		}
		waitForStatus(t, manager, invocation.ID, StatusTimedOut)
		time.Sleep(5 * time.Millisecond)
		record, _ := manager.Get(invocation.ID)
		manager.Stop()
		if record.Status != StatusTimedOut || len(record.Result) != 0 {
			t.Fatalf("attempt %d: record = %+v, want timed out without result", attempt, record)
		}
		active, waiting := manager.capacitySnapshot()
		if active != 0 || waiting != 0 {
			t.Fatalf("attempt %d: capacity = %d / %d, want 0 / 0", attempt, active, waiting)
		}
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
