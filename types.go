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

type DependencyWait struct {
	InvocationID          string   `json:"invocation_id"`
	Artifacts             []string `json:"artifacts,omitempty"`
	Ready                 bool     `json:"ready"`
	Reason                string   `json:"reason,omitempty"`
	PrerequisiteStatus    string   `json:"prerequisite_status,omitempty"`
	PrerequisiteUpdatedAt string   `json:"prerequisite_updated_at,omitempty"`
	Basis                 string   `json:"basis,omitempty"`
	LastCheckedAt         string   `json:"last_checked_at"`
}

type CapacityWait struct {
	Waiting       bool   `json:"waiting"`
	Reason        string `json:"reason,omitempty"`
	LastCheckedAt string `json:"last_checked_at,omitempty"`
}

type WaitingState struct {
	Kind          string           `json:"kind,omitempty"`
	Dependencies  []DependencyWait `json:"dependencies,omitempty"`
	Capacity      *CapacityWait    `json:"capacity,omitempty"`
	LastCheckedAt string           `json:"last_checked_at,omitempty"`
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
	Waiting         *WaitingState     `json:"waiting,omitempty"`
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
	if r.Dependencies != nil {
		copyValue.Dependencies = append([]Dependency(nil), r.Dependencies...)
	}
	if r.DependencyBasis != nil {
		copyValue.DependencyBasis = make(map[string]string, len(r.DependencyBasis))
		for key, value := range r.DependencyBasis {
			copyValue.DependencyBasis[key] = value
		}
	}
	if r.Waiting != nil {
		copyValue.Waiting = cloneWaitingState(r.Waiting)
	}
	if r.Argument != nil {
		copyValue.Argument = append(json.RawMessage(nil), r.Argument...)
	}
	if r.Result != nil {
		copyValue.Result = append(json.RawMessage(nil), r.Result...)
	}
	return copyValue
}

func cloneWaitingState(waiting *WaitingState) *WaitingState {
	copyValue := *waiting
	if waiting.Dependencies != nil {
		copyValue.Dependencies = make([]DependencyWait, len(waiting.Dependencies))
		for index, dependency := range waiting.Dependencies {
			copyValue.Dependencies[index] = dependency
			if dependency.Artifacts != nil {
				copyValue.Dependencies[index].Artifacts = append([]string(nil), dependency.Artifacts...)
			}
		}
	}
	if waiting.Capacity != nil {
		capacity := *waiting.Capacity
		copyValue.Capacity = &capacity
	}
	return &copyValue
}
