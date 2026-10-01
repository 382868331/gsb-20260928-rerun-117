package main

import "encoding/json"

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusTimedOut  = "timed_out"
	StatusCanceled  = "canceled"
	StatusRejected  = "rejected"
	StatusUncertain = "uncertain"
)

type Invocation struct {
	ID         string          `json:"id"`
	Function   string          `json:"function"`
	Argument   json.RawMessage `json:"argument,omitempty"`
	Weight     int             `json:"weight"`
	Group      string          `json:"group"`
	TimeoutMs  int             `json:"timeout_ms"`
	Status     string          `json:"status"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
	StartedAt  string          `json:"started_at,omitempty"`
	FinishedAt string          `json:"finished_at,omitempty"`
}

type InvokeRequest struct {
	Function  string          `json:"function"`
	Argument  json.RawMessage `json:"argument"`
	Weight    int             `json:"weight"`
	Group     string          `json:"group"`
	TimeoutMs int             `json:"timeout_ms"`
}

type InvokeResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func (r *Invocation) Clone() Invocation {
	copyValue := *r
	if r.Argument != nil {
		copyValue.Argument = append(json.RawMessage(nil), r.Argument...)
	}
	if r.Result != nil {
		copyValue.Result = append(json.RawMessage(nil), r.Result...)
	}
	return copyValue
}
