package main

import (
	"fmt"
)

// Explain builds a read-only dependency explanation graph for the given
// invocation. It never mutates records, never admits work, and never
// confirms external results; it only reads the store snapshot taken at
// query time. The graph is bound to the task version observed now.
func (m *Manager) Explain(id string) (DependencyExplanation, bool) {
	root, ok := m.store.Get(id)
	if !ok {
		return DependencyExplanation{}, false
	}
	records := make(map[string]Invocation)
	for _, record := range m.store.List() {
		records[record.ID] = record
	}
	builder := &explanationBuilder{
		records: records,
		nodes:   make(map[string]*ExplanationNode),
	}
	builder.walk(root, nil)
	explanation := DependencyExplanation{
		InvocationID: id,
		TaskVersion:  root.UpdatedAt,
		GeneratedAt:  nowRFC3339(),
		Root:         id,
		Nodes:        make([]ExplanationNode, 0, len(builder.order)),
	}
	for _, nodeID := range builder.order {
		explanation.Nodes = append(explanation.Nodes, *builder.nodes[nodeID])
	}
	return explanation, true
}

type explanationBuilder struct {
	records map[string]Invocation
	nodes   map[string]*ExplanationNode
	order   []string
}

func (b *explanationBuilder) walk(record Invocation, stack []string) {
	if _, seen := b.nodes[record.ID]; seen {
		return
	}
	node := &ExplanationNode{
		InvocationID: record.ID,
		Function:     record.Function,
		Status:       record.Status,
		UpdatedAt:    record.UpdatedAt,
	}
	switch record.Status {
	case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
		node.Kind = ExplanationKindExternalConfirmation
		node.Reason = fmt.Sprintf("outcome is %q and requires manual confirmation", record.Status)
	case StatusBlocked:
		if record.Waiting != nil {
			node.Kind = record.Waiting.Kind
			node.Reason = record.Error
			node.CheckedAt = record.Waiting.CheckedAt
		} else {
			node.Kind = WaitingKindDependencies
			node.Reason = record.Error
		}
	}
	b.nodes[record.ID] = node
	b.order = append(b.order, record.ID)
	stack = append(stack, record.ID)
	for _, dependency := range record.Dependencies {
		edge := ExplanationEdge{InvocationID: dependency.InvocationID}
		if cycleStart := indexOf(stack, dependency.InvocationID); cycleStart >= 0 {
			edge.Reason = "dependency declaration forms a cycle"
			edge.Cycle = append(append([]string(nil), stack[cycleStart:]...), dependency.InvocationID)
			node.Edges = append(node.Edges, edge)
			continue
		}
		prerequisite, ok := b.records[dependency.InvocationID]
		if !ok {
			edge.Reason = "dependency does not exist"
			node.Edges = append(node.Edges, edge)
			continue
		}
		edge.Status = prerequisite.Status
		if prerequisite.Status != StatusSucceeded {
			switch prerequisite.Status {
			case StatusFailed, StatusTimedOut, StatusCanceled, StatusUncertain:
				edge.Reason = fmt.Sprintf("dependency is %q and requires manual confirmation", prerequisite.Status)
			default:
				edge.Reason = fmt.Sprintf("dependency is %q and has not succeeded yet", prerequisite.Status)
			}
		} else {
			digest, missing, err := artifactDigest(prerequisite.Result, dependency.Artifacts)
			switch {
			case err != nil:
				edge.Reason = fmt.Sprintf("artifacts are unavailable: %v", err)
			case len(missing) > 0:
				edge.Reason = "requested artifacts are missing from the dependency result"
				edge.MissingArtifacts = missing
			default:
				edge.Satisfied = true
				edge.Basis = digest
			}
		}
		node.Edges = append(node.Edges, edge)
		b.walk(prerequisite, stack)
	}
}

func indexOf(stack []string, id string) int {
	for index, entry := range stack {
		if entry == id {
			return index
		}
	}
	return -1
}
