package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Function func(ctx context.Context, argument json.RawMessage) (any, error)

type Registry map[string]Function

func DefaultRegistry() Registry {
	return Registry{
		"echo": echoFunction,
		"add":  addFunction,
		"wait": waitFunction,
	}
}

func (r Registry) Get(name string) (Function, bool) {
	fn, ok := r[name]
	return fn, ok
}

func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}

	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

type echoArgs struct {
	Text string `json:"text"`
}

func echoFunction(_ context.Context, raw json.RawMessage) (any, error) {
	var args echoArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid echo argument: %w", err)
	}
	return map[string]any{"text": args.Text}, nil
}

type addArgs struct {
	A float64 `json:"a"`
	B float64 `json:"b"`
}

func addFunction(_ context.Context, raw json.RawMessage) (any, error) {
	var args addArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid add argument: %w", err)
	}
	return map[string]any{"sum": args.A + args.B}, nil
}

type waitArgs struct {
	Ms int `json:"ms"`
}

func waitFunction(ctx context.Context, raw json.RawMessage) (any, error) {
	var args waitArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid wait argument: %w", err)
	}
	if args.Ms < 0 {
		return nil, errors.New("ms must be non-negative")
	}
	if args.Ms > 30000 {
		return nil, errors.New("ms must be at most 30000")
	}
	timer := time.NewTimer(time.Duration(args.Ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return map[string]any{"waited_ms": args.Ms}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
