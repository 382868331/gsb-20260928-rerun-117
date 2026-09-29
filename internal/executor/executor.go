package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrQueueFull is returned when every slot is busy and the wait
	// queue is at capacity; callers should surface 429.
	ErrQueueFull = errors.New("execution queue is full")
	// ErrShuttingDown rejects new work while the executor drains.
	ErrShuttingDown = errors.New("executor is shutting down")
)

// Config tunes the executor. All limits are hard caps: the executor
// never spawns unbounded goroutines per request beyond maxSlots workers.
type Config struct {
	MaxSlots     int           // concurrent executions
	MaxQueue     int           // pending requests waiting for a slot
	ExecTimeout  time.Duration // per-invocation timeout
	ResultRetain time.Duration // reserved for future GC of old records
}

type task struct {
	id   string
	fn   Func
	args json.RawMessage
	ctx  context.Context
}

// Executor runs whitelisted functions with bounded concurrency, bounded
// queueing, per-run timeout and cancellation, and durable result
// recording.
type Executor struct {
	cfg  Config
	reg  *Registry
	st   *Store

	slots  chan struct{}
	queue  chan task
	stopCh chan struct{}

	mu       sync.Mutex
	cancels  map[string]context.CancelFunc
	stopping bool
	wg       sync.WaitGroup
}

func NewExecutor(cfg Config, reg *Registry, st *Store) *Executor {
	if cfg.MaxSlots <= 0 {
		cfg.MaxSlots = 4
	}
	if cfg.MaxQueue < 0 {
		cfg.MaxQueue = 0
	}
	if cfg.ExecTimeout <= 0 {
		cfg.ExecTimeout = 30 * time.Second
	}
	e := &Executor{
		cfg:     cfg,
		reg:     reg,
		st:      st,
		slots:   make(chan struct{}, cfg.MaxSlots),
		queue:   make(chan task, cfg.MaxQueue),
		stopCh:  make(chan struct{}),
		cancels: map[string]context.CancelFunc{},
	}
	// Fixed dispatcher pool: one goroutine per slot, so concurrency is
	// structurally capped regardless of submission rate.
	for i := 0; i < cfg.MaxSlots; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	return e
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Submit enqueues a request or rejects it immediately when full.
// It never blocks the caller waiting for a slot.
func (e *Executor) Submit(fnName string, args json.RawMessage) (string, error) {
	fn, ok := e.reg.Get(fnName)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnknownFunction, fnName)
	}
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return "", ErrShuttingDown
	}
	e.mu.Unlock()

	id := newID()
	rec := &Record{
		ID:        id,
		Function:  fnName,
		Args:      args,
		Status:    StatusQueued,
		CreatedAt: time.Now(),
	}
	e.st.Put(rec)

	select {
	case e.queue <- task{id: id, fn: fn, args: args}:
		return id, nil
	default:
		// Queue full: record the rejection so the request is auditable,
		// and tell the caller explicitly instead of growing unboundedly.
		e.st.CASStatus(id, []Status{StatusQueued}, StatusCancelled, func(r *Record) {
			r.Error = ErrQueueFull.Error()
			now := time.Now()
			r.FinishedAt = &now
		})
		return "", ErrQueueFull
	}
}

// Cancel requests cancellation of a queued or running invocation.
func (e *Executor) Cancel(id string) bool {
	e.mu.Lock()
	cancel, ok := e.cancels[id]
	e.mu.Unlock()
	if ok {
		cancel()
		return true
	}
	// Queued (not yet started): mark cancelled directly.
	_, changed := e.st.CASStatus(id, []Status{StatusQueued}, StatusCancelled, func(r *Record) {
		r.Error = "cancelled before start"
		now := time.Now()
		r.FinishedAt = &now
	})
	return changed
}

func (e *Executor) worker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case t := <-e.queue:
			e.run(t)
		}
	}
}

func (e *Executor) run(t task) {
	// If it was cancelled while queued, skip execution.
	rec, ok := e.st.Get(t.id)
	if !ok || rec.Status != StatusQueued {
		return
	}
	now := time.Now()
	if _, ok := e.st.CASStatus(t.id, []Status{StatusQueued}, StatusRunning, func(r *Record) {
		r.StartedAt = &now
	}); !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.ExecTimeout)
	e.mu.Lock()
	e.cancels[t.id] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.cancels, t.id)
		e.mu.Unlock()
		cancel()
	}()

	type outcome struct {
		val any
		err error
	}
	resCh := make(chan outcome, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				resCh <- outcome{err: fmt.Errorf("panic: %v", p)}
			}
		}()
		v, err := t.fn(ctx, t.args)
		resCh <- outcome{val: v, err: err}
	}()

	select {
	case out := <-resCh:
		// Normal completion. CAS ensures a concurrent cancel/timeout
		// transition wins if it landed first.
		if out.err != nil {
			if errors.Is(out.err, context.Canceled) {
				e.finishCancelled(t.id, "cancelled")
				return
			}
			if errors.Is(out.err, context.DeadlineExceeded) {
				e.finishTimeout(t.id)
				return
			}
			e.st.CASStatus(t.id, []Status{StatusRunning}, StatusFailed, func(r *Record) {
				r.Error = out.err.Error()
				fin := time.Now()
				r.FinishedAt = &fin
			})
			return
		}
		e.st.CASStatus(t.id, []Status{StatusRunning}, StatusSucceeded, func(r *Record) {
			r.Result = out.val
			fin := time.Now()
			r.FinishedAt = &fin
		})
	case <-ctx.Done():
		// Timeout or cancel fired first. Transition to the terminal
		// state NOW; when the function eventually returns, its late
		// result hits the CAS above (or the drain below) and is dropped
		// because the record is no longer StatusRunning.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			e.finishTimeout(t.id)
		} else {
			e.finishCancelled(t.id, "cancelled")
		}
		// Drain the late result so the function goroutine can exit;
		// its outcome is deliberately discarded.
		go func() { <-resCh }()
	}
}

func (e *Executor) finishTimeout(id string) {
	e.st.CASStatus(id, []Status{StatusRunning}, StatusTimeout, func(r *Record) {
		r.Error = "execution timeout"
		fin := time.Now()
		r.FinishedAt = &fin
	})
}

func (e *Executor) finishCancelled(id, msg string) {
	e.st.CASStatus(id, []Status{StatusRunning}, StatusCancelled, func(r *Record) {
		r.Error = msg
		fin := time.Now()
		r.FinishedAt = &fin
	})
}

// Get returns the durable record for a request.
func (e *Executor) Get(id string) (*Record, bool) { return e.st.Get(id) }

// Shutdown stops accepting work and waits for in-flight runs to finish
// (bounded by their own timeouts). Queued-but-unstarted requests are
// left for recovery to classify as unknown on next start.
func (e *Executor) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return nil
	}
	e.stopping = true
	close(e.stopCh)
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
