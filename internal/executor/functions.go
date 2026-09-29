package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Func is a whitelisted, locally implemented function. It must honor
// ctx cancellation to be eligible for timeout/cancel semantics.
type Func func(ctx context.Context, args json.RawMessage) (any, error)

// Registry holds the fixed set of functions the platform is allowed to
// run. There is intentionally no way to register code at runtime: only
// functions compiled into this binary can execute.
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]Func
}

func NewRegistry() *Registry {
	return &Registry{funcs: map[string]Func{}}
}

// Register adds a function at startup. Panics on duplicate names so
// misconfiguration fails fast.
func (r *Registry) Register(name string, fn Func) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.funcs[name]; ok {
		panic("duplicate function: " + name)
	}
	r.funcs[name] = fn
}

func (r *Registry) Get(name string) (Func, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.funcs[name]
	return fn, ok
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.funcs))
	for n := range r.funcs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

var ErrUnknownFunction = errors.New("unknown function")

// DefaultRegistry builds the built-in demo function set.
func DefaultRegistry() *Registry {
	r := NewRegistry()

	r.Register("echo", func(ctx context.Context, args json.RawMessage) (any, error) {
		var v any
		if len(args) > 0 {
			if err := json.Unmarshal(args, &v); err != nil {
				return nil, fmt.Errorf("echo: bad args: %w", err)
			}
		}
		return v, nil
	})

	r.Register("add", func(ctx context.Context, args json.RawMessage) (any, error) {
		var p struct {
			A float64 `json:"a"`
			B float64 `json:"b"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, fmt.Errorf("add: bad args: %w", err)
		}
		return p.A + p.B, nil
	})

	// sleep blocks for ms milliseconds, honoring cancellation.
	r.Register("sleep", func(ctx context.Context, args json.RawMessage) (any, error) {
		var p struct {
			Ms int64 `json:"ms"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, fmt.Errorf("sleep: bad args: %w", err)
		}
		if p.Ms < 0 {
			return nil, errors.New("sleep: ms must be >= 0")
		}
		t := time.NewTimer(time.Duration(p.Ms) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			return map[string]any{"slept_ms": p.Ms}, nil
		}
	})

	// fib computes fib(n) iteratively, checking cancellation.
	r.Register("fib", func(ctx context.Context, args json.RawMessage) (any, error) {
		var p struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, fmt.Errorf("fib: bad args: %w", err)
		}
		if p.N < 0 || p.N > 92 {
			return nil, errors.New("fib: n must be in [0,92]")
		}
		a, b := int64(0), int64(1)
		for i := 0; i < p.N; i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a, b = b, a+b
		}
		return a, nil
	})

	return r
}
