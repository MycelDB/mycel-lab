package oracle

import (
	"fmt"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type OperationType string

const (
	OperationCreateNode OperationType = "createNode"
	OperationUpdateNode OperationType = "updateNode"
	OperationCreateEdge OperationType = "createEdge"
)

type Operation struct {
	Type       OperationType  `json:"type"`
	NodeID     string         `json:"nodeId,omitempty"`
	FromID     string         `json:"fromId,omitempty"`
	ToID       string         `json:"toId,omitempty"`
	Label      string         `json:"label,omitempty"`
	EdgeType   string         `json:"edgeType,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

type Transaction struct {
	ActorID      string      `json:"actorId"`
	Sequence     int64       `json:"sequence"`
	Operations   []Operation `json:"operations"`
	Acknowledged bool        `json:"acknowledged"`
	ErrorClass   string      `json:"errorClass,omitempty"`
}

type Counts struct {
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
}

type Observation struct {
	ActorID  string `json:"actorId"`
	Endpoint string `json:"endpoint"`
	Counts   Counts `json:"counts"`
}

type ConvergenceFailure struct {
	ActorID       string `json:"actorId"`
	Endpoint      string `json:"endpoint"`
	ExpectedNodes int    `json:"expectedNodes"`
	ObservedNodes int    `json:"observedNodes"`
	ExpectedEdges int    `json:"expectedEdges"`
	ObservedEdges int    `json:"observedEdges"`
	Phase         string `json:"phase,omitempty"`
}

func ExpectedCounts(transactions []Transaction) Counts {
	var counts Counts
	for _, tx := range transactions {
		if !tx.Acknowledged {
			continue
		}
		for _, op := range tx.Operations {
			switch op.Type {
			case OperationCreateNode:
				counts.Nodes++
			case OperationCreateEdge:
				counts.Edges++
			}
		}
	}
	return counts
}

func CheckConvergence(expected map[string]Counts, observations []Observation, phase string) []ConvergenceFailure {
	var failures []ConvergenceFailure
	for _, obs := range observations {
		want, ok := expected[obs.ActorID]
		if !ok {
			continue
		}
		if want != obs.Counts {
			failures = append(failures, ConvergenceFailure{ActorID: obs.ActorID, Endpoint: obs.Endpoint, ExpectedNodes: want.Nodes, ObservedNodes: obs.Counts.Nodes, ExpectedEdges: want.Edges, ObservedEdges: obs.Counts.Edges, Phase: phase})
		}
	}
	return failures
}

type ReadCheckResult struct {
	ActorID             string `json:"actorId"`
	Phase               string `json:"phase"`
	OK                  bool   `json:"ok"`
	FailureClass        string `json:"failureClass,omitempty"`
	ExpectedDegradation bool   `json:"expectedDegradation"`
}

func ClassifyReadCheck(actorID string, phase spec.PhaseSpec, ok bool, failureClass string) ReadCheckResult {
	availability := phase.Outcome["availability"]
	return ReadCheckResult{ActorID: actorID, Phase: phase.Name, OK: ok, FailureClass: failureClass, ExpectedDegradation: !ok && availability == "expected-degradation" && (failureClass == "transient" || failureClass == "unavailable")}
}

func AssertNoAcknowledgedLoss(expected, observed Counts) error {
	if expected == observed {
		return nil
	}
	return fmt.Errorf("acknowledged graph transaction loss: expected nodes=%d edges=%d observed nodes=%d edges=%d", expected.Nodes, expected.Edges, observed.Nodes, observed.Edges)
}
