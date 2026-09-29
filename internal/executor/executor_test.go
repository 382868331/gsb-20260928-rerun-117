package executor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitStatus(t *testing.T, st *Store, id string, want Status) *Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := st.Get(id); ok && rec.Status == want {
			return rec
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec, _ := st.Get(id)
	t.Fatalf("record %s did not reach %s (got %+v)", id, want, rec)
	return nil
}

func newTestExecutor(t *testing.T, cfg Config, reg *Registry) (*Executor, *Store) {
	t.Helper()
	st, err := OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	if reg == nil {
		reg = DefaultRegistry()
	}
	ex := NewExecutor(cfg, reg, st)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ex.Shutdown(ctx)
	})
	return ex, st
}

func TestConcurrencyLimit(t *testing.T) {
	var cur, max atomic.Int64
	reg := NewRegistry()
	reg.Register("probe", func(ctx context.Context, args json.RawMessage) (any, error) {
		n := cur.Add(1)
		for {
			if m := max.Load(); n <= m || max.CompareAndSwap(m, n) {
				break
			}
		}
		defer cur.Add(-1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(80 * time.Millisecond):
			return "done", nil
		}
	})
	ex, st := newTestExecutor(t, Config{MaxSlots: 2, MaxQueue: 8, ExecTimeout: time.Second}, reg)

	ids := make([]string, 6)
	for i := range ids {
		id, err := ex.Submit("probe", nil)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		ids[i] = id
	}
	for _, id := range ids {
		waitStatus(t, st, id, StatusSucceeded)
	}
	if got := max.Load(); got > 2 {
		t.Fatalf("concurrency exceeded limit: saw %d, want <= 2", got)
	}
	if got := max.Load(); got == 0 {
		t.Fatal("function never ran")
	}
}

func TestQueueFullRejected(t *testing.T) {
	ex, st := newTestExecutor(t, Config{MaxSlots: 1, MaxQueue: 1, ExecTimeout: 5 * time.Second}, nil)
	// First fills the slot, second fills the queue, third must be rejected.
	first, err := ex.Submit("sleep", json.RawMessage(`{"ms":500}`))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, first, StatusRunning)
	if _, err := ex.Submit("sleep", json.RawMessage(`{"ms":500}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.Submit("sleep", json.RawMessage(`{"ms":500}`)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestTimeoutLateResultDiscarded(t *testing.T) {
	release := make(chan struct{})
	reg := NewRegistry()
	reg.Register("stubborn", func(ctx context.Context, args json.RawMessage) (any, error) {
		// Ignores cancellation entirely; returns long after the timeout.
		<-release
		return "late-result", nil
	})
	ex, st := newTestExecutor(t, Config{MaxSlots: 1, MaxQueue: 4, ExecTimeout: 100 * time.Millisecond}, reg)

	id, err := ex.Submit("stubborn", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := waitStatus(t, st, id, StatusTimeout)
	if rec.Result != nil {
		t.Fatalf("timeout record must not carry a result: %+v", rec)
	}

	// Let the function finish late; the record must stay timeout.
	close(release)
	time.Sleep(200 * time.Millisecond)
	rec, _ = st.Get(id)
	if rec.Status != StatusTimeout {
		t.Fatalf("late result changed status to %s", rec.Status)
	}
	if rec.Result != nil {
		t.Fatalf("late result was recorded: %+v", rec.Result)
	}
}

func TestCancel(t *testing.T) {
	ex, st := newTestExecutor(t, Config{MaxSlots: 1, MaxQueue: 4, ExecTimeout: 5 * time.Second}, nil)
	id, err := ex.Submit("sleep", json.RawMessage(`{"ms":3000}`))
	if err != nil {
		t.Fatal(err)
	}
	// Wait until running, then cancel.
	waitStatus(t, st, id, StatusRunning)
	if !ex.Cancel(id) {
		t.Fatal("cancel returned false for running task")
	}
	rec := waitStatus(t, st, id, StatusCancelled)
	if rec.Result != nil {
		t.Fatalf("cancelled record has result: %+v", rec)
	}
}

func TestMixedFastSlowSlotRecycling(t *testing.T) {
	ex, st := newTestExecutor(t, Config{MaxSlots: 2, MaxQueue: 64, ExecTimeout: 2 * time.Second}, nil)

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	// 10 slow (200ms) + 30 fast (echo) interleaved.
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var id string
			var err error
			if i%4 == 0 {
				id, err = ex.Submit("sleep", json.RawMessage(`{"ms":200}`))
			} else {
				id, err = ex.Submit("echo", json.RawMessage(`{"i":1}`))
			}
			if err != nil {
				errs <- err
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if rec, ok := st.Get(id); ok && rec.Status.Terminal() {
					if rec.Status != StatusSucceeded {
						errs <- errors.New("unexpected status: " + string(rec.Status))
					}
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
			errs <- errors.New("timed out waiting for " + id)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// 10*200ms of slow work on 2 slots => >= ~1s if slots were NOT
	// recycled for fast tasks we'd see much worse; sanity bound only.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took too long, slots may not be recycled: %s", elapsed)
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.jsonl")

	st1, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	st1.Put(&Record{ID: "done-1", Function: "echo", Status: StatusSucceeded, Result: "ok", CreatedAt: now, FinishedAt: &now})
	st1.Put(&Record{ID: "run-1", Function: "sleep", Status: StatusRunning, CreatedAt: now})
	st1.Put(&Record{ID: "q-1", Function: "sleep", Status: StatusQueued, CreatedAt: now})
	st1.Close()

	// Simulate restart.
	st2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	done, _ := st2.Get("done-1")
	if done.Status != StatusSucceeded || done.Result != "ok" {
		t.Fatalf("completed record corrupted after restart: %+v", done)
	}
	run, _ := st2.Get("run-1")
	if run.Status != StatusUnknown {
		t.Fatalf("running record after restart = %s, want unknown", run.Status)
	}
	q, _ := st2.Get("q-1")
	if q.Status != StatusUnknown {
		t.Fatalf("queued record after restart = %s, want unknown", q.Status)
	}
}

func TestUnknownFunctionRejected(t *testing.T) {
	ex, _ := newTestExecutor(t, Config{MaxSlots: 1, MaxQueue: 1, ExecTimeout: time.Second}, nil)
	if _, err := ex.Submit("rm-rf", nil); !errors.Is(err, ErrUnknownFunction) {
		t.Fatalf("expected ErrUnknownFunction, got %v", err)
	}
}
