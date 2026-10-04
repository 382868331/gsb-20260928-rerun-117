package main

import "encoding/json"

const (
	StatusQueued    = "queued"
	StatusBlocked   = "blocked"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusTimedOut  = "timed_out"
	StatusCanceled  = "canceled"
	StatusRejected  = "rejected"
	StatusUncertain = "uncertain"
)

type Invocation struct {
	ID                   string                  `json:"id"`
	Function             string                  `json:"function"`
	Argument             json.RawMessage         `json:"argument,omitempty"`
	Cost                 int                     `json:"cost"`
	Group                string                  `json:"group"`
	DependsOn            []DependencyRequirement `json:"depends_on,omitempty"`
	ResolvedDependencies []ResolvedDependency    `json:"resolved_dependencies,omitempty"`
	TimeoutMs            int                     `json:"timeout_ms"`
	Status               string                  `json:"status"`
	Result               json.RawMessage         `json:"result,omitempty"`
	Error                string                  `json:"error,omitempty"`
	Resolution           string                  `json:"resolution,omitempty"`
	CreatedAt            string                  `json:"created_at"`
	UpdatedAt            string                  `json:"updated_at"`
	StartedAt            string                  `json:"started_at,omitempty"`
	FinishedAt           string                  `json:"finished_at,omitempty"`
}

type DependencyRequirement struct {
	ID       string `json:"id"`
	Artifact string `json:"artifact"`
}

type ResolvedDependency struct {
	ID              string `json:"id"`
	Artifact        string `json:"artifact"`
	SourceUpdatedAt string `json:"source_updated_at"`
}

type InvokeRequest struct {
	Function  string                  `json:"function"`
	Argument  json.RawMessage         `json:"argument"`
	Cost      int                     `json:"cost,omitempty"`
	Group     string                  `json:"group,omitempty"`
	DependsOn []DependencyRequirement `json:"depends_on,omitempty"`
	TimeoutMs int                     `json:"timeout_ms"`
}

type ResultReplacementRequest struct {
	Result json.RawMessage `json:"result"`
	Basis  string          `json:"basis,omitempty"`
}

type CapacityRequest struct {
	Capacity int `json:"capacity"`
}

type CapacityResponse struct {
	Capacity            int `json:"capacity"`
	Used                int `json:"used"`
	Running             int `json:"running"`
	Waiting             int `json:"waiting_capacity"`
	WaitingDependencies int `json:"waiting_dependencies"`
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
	if r.DependsOn != nil {
		copyValue.DependsOn = append([]DependencyRequirement(nil), r.DependsOn...)
	}
	if r.ResolvedDependencies != nil {
		copyValue.ResolvedDependencies = append([]ResolvedDependency(nil), r.ResolvedDependencies...)
	}
	return copyValue
}
