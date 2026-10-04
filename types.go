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
	StatusBlocked   = "blocked"
)

type Dependency struct {
	InvocationID string   `json:"invocation_id"`
	Artifacts    []string `json:"artifacts,omitempty"`
}

type Invocation struct {
	ID              string            `json:"id"`
	Function        string            `json:"function"`
	Argument        json.RawMessage   `json:"argument,omitempty"`
	Cost            int               `json:"cost"`
	Group           string            `json:"group"`
	TimeoutMs       int               `json:"timeout_ms"`
	Dependencies    []Dependency      `json:"dependencies,omitempty"`
	DependencyBasis map[string]string `json:"dependency_basis,omitempty"`
	Status          string            `json:"status"`
	Result          json.RawMessage   `json:"result,omitempty"`
	Error           string            `json:"error,omitempty"`
	CreatedAt       string            `json:"created_at"`
	UpdatedAt       string            `json:"updated_at"`
	StartedAt       string            `json:"started_at,omitempty"`
	FinishedAt      string            `json:"finished_at,omitempty"`
}

type InvokeRequest struct {
	Function     string          `json:"function"`
	Argument     json.RawMessage `json:"argument"`
	Cost         int             `json:"cost,omitempty"`
	Group        string          `json:"group,omitempty"`
	TimeoutMs    int             `json:"timeout_ms"`
	Dependencies []Dependency    `json:"dependencies,omitempty"`
}

type ReplaceResultRequest struct {
	Result json.RawMessage `json:"result"`
}

type CapacityRequest struct {
	Capacity int `json:"capacity"`
}

type CapacityResponse struct {
	Capacity            int `json:"capacity"`
	Used                int `json:"used"`
	Running             int `json:"running"`
	Waiting             int `json:"waiting"`
	WaitingCapacity     int `json:"waiting_capacity"`
	WaitingDependencies int `json:"waiting_dependencies"`
	Blocked             int `json:"blocked"`
	QueueSize           int `json:"queue_size"`
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
