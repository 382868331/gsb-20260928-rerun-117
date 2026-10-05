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

// Wait kinds used by the explanation graph. WaitExternalConfirmation means
// the task or prerequisite ended in a state (failed, timed_out, canceled,
// uncertain) that only an operator can resolve by replacing the result.
const (
	ExplainWaitNone                 = "none"
	ExplainWaitDependencies         = "dependencies"
	ExplainWaitCapacity             = "capacity"
	ExplainWaitExternalConfirmation = "external_confirmation"
)

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

// ExplanationNode is one task in the read-only dependency explanation
// graph. Wait classifies what the task is currently waiting on.
type ExplanationNode struct {
	InvocationID string `json:"invocation_id"`
	Function     string `json:"function"`
	Status       string `json:"status"`
	Wait         string `json:"wait"`
	CheckedAt    string `json:"checked_at,omitempty"`
}

// ExplanationEdge is one declared dependency from a task (From) to its
// prerequisite (To), with the artifacts required and the ones still missing.
const (
	EdgeSatisfied         = "satisfied"
	EdgeWaiting           = "waiting"
	EdgeNeedsConfirmation = "needs_confirmation"
	EdgeMissingArtifacts  = "missing_artifacts"
	EdgeMissingDependency = "missing_dependency"
)

type ExplanationEdge struct {
	From             string   `json:"from"`
	To               string   `json:"to"`
	Artifacts        []string `json:"artifacts,omitempty"`
	MissingArtifacts []string `json:"missing_artifacts,omitempty"`
	Status           string   `json:"status"`
	Reason           string   `json:"reason,omitempty"`
}

// ExplanationGraph is a read-only snapshot of everything a waiting task is
// blocked on, direct and indirect. Shared prerequisites appear once in
// Nodes; Cycles lists any cyclic declaration paths found while walking.
// Version binds the graph to the root task record version (its UpdatedAt)
// observed at query time: later result replacements produce a new graph
// while previously returned graphs stay unchanged.
type ExplanationGraph struct {
	Root        string            `json:"root"`
	Version     string            `json:"version"`
	GeneratedAt string            `json:"generated_at"`
	Nodes       []ExplanationNode `json:"nodes"`
	Edges       []ExplanationEdge `json:"edges"`
	Cycles      [][]string        `json:"cycles,omitempty"`
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
