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

const (
	WaitingKindDependencies = "dependencies"
	WaitingKindCapacity     = "capacity"
)

// ExplanationKindExternalConfirmation marks explanation nodes whose
// prerequisite outcome (failed, timed_out, canceled, uncertain) needs an
// operator to confirm or replace the external result before dependents move.
const ExplanationKindExternalConfirmation = "external_confirmation"

// DependencyWaitReason explains why one prerequisite still blocks the task:
// it is missing, not yet succeeded, needs manual confirmation, or requested
// artifacts are absent from its result.
type DependencyWaitReason struct {
	InvocationID     string   `json:"invocation_id"`
	Status           string   `json:"status,omitempty"`
	MissingArtifacts []string `json:"missing_artifacts,omitempty"`
	Reason           string   `json:"reason"`
}

// WaitingInfo describes the current wait of a blocked task. Kind separates
// waiting on dependencies from waiting on execution capacity; CheckedAt is
// the time of the most recent dependency re-check behind the reasons.
type WaitingInfo struct {
	Kind      string                 `json:"kind"`
	Reasons   []DependencyWaitReason `json:"reasons,omitempty"`
	CheckedAt string                 `json:"checked_at"`
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
	Waiting         *WaitingInfo      `json:"waiting,omitempty"`
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

// DependencyExplanation is a read-only snapshot explaining why a task is
// waiting. It is bound to the task version observed at query time
// (TaskVersion is the task's updated_at); later result replacements produce
// new explanations while previously returned ones stay unchanged. Shared
// prerequisites appear exactly once in Nodes; edges reference node IDs.
type DependencyExplanation struct {
	InvocationID string            `json:"invocation_id"`
	TaskVersion  string            `json:"task_version"`
	GeneratedAt  string            `json:"generated_at"`
	Root         string            `json:"root"`
	Nodes        []ExplanationNode `json:"nodes"`
}

// ExplanationNode describes one invocation reachable from the queried task.
// Kind distinguishes waiting on external confirmation, waiting on
// dependencies, and waiting for capacity; it is empty for satisfied nodes.
type ExplanationNode struct {
	InvocationID string            `json:"invocation_id"`
	Function     string            `json:"function"`
	Status       string            `json:"status"`
	Kind         string            `json:"kind,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	CheckedAt    string            `json:"checked_at,omitempty"`
	UpdatedAt    string            `json:"updated_at"`
	Edges        []ExplanationEdge `json:"edges,omitempty"`
}

// ExplanationEdge links a node to one declared prerequisite. Cycle holds a
// readable cycle path (e.g. [a,b,a]) when following the edge would revisit
// a node already on the current path.
type ExplanationEdge struct {
	InvocationID     string   `json:"invocation_id"`
	Status           string   `json:"status,omitempty"`
	Satisfied        bool     `json:"satisfied"`
	MissingArtifacts []string `json:"missing_artifacts,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	Basis            string   `json:"basis,omitempty"`
	Cycle            []string `json:"cycle,omitempty"`
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
	if r.Waiting != nil {
		waiting := *r.Waiting
		waiting.Reasons = append([]DependencyWaitReason(nil), r.Waiting.Reasons...)
		for index := range waiting.Reasons {
			waiting.Reasons[index].MissingArtifacts = append([]string(nil), r.Waiting.Reasons[index].MissingArtifacts...)
		}
		copyValue.Waiting = &waiting
	}
	return copyValue
}
