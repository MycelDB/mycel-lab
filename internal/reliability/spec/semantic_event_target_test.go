package spec

import (
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateSemanticEventTargetAcceptsTypedContracts(t *testing.T) {
	tests := []struct {
		eventType string
		target    map[string]any
	}{
		{
			eventType: "semantic-search-assert",
			target: map[string]any{
				"query": "axis-0", "limit": 3, "minimumScore": 0.9,
				"expectedNodeIds": []any{"node-1"}, "expectedScores": []any{1.0},
				"scoreTolerance": 1e-9, "requireProvenance": true,
			},
		},
		{
			eventType: "semantic-rebuild",
			target: map[string]any{
				"backfillLimit": 2, "maintenanceBatchLimit": 64, "maxCatchupIterations": 64,
				"concurrentMutation": map[string]any{
					"eventType": "semantic-node-create", "nodeId": "node-1", "labels": []any{"Document"}, "text": "axis-1",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.eventType, func(t *testing.T) {
			if err := ValidateSemanticEventTarget(test.eventType, test.target); err != nil {
				t.Fatalf("ValidateSemanticEventTarget() error=%v", err)
			}
		})
	}
}

func TestValidateSemanticEventTargetRejectsUntypedOrUnknownAssertions(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		target    map[string]any
		contains  string
	}{
		{name: "unknown typo", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "minimimScore": 0.9}, contains: "not supported"},
		{name: "wrong numeric type", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "minimumScore": "0.9"}, contains: "finite number"},
		{name: "wrong score collection", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "expectedScores": "1.0"}, contains: "numeric list"},
		{name: "mixed score collection", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "expectedScores": []any{1.0, "zero"}}, contains: "finite numbers"},
		{name: "non-finite score", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "minimumScore": math.NaN()}, contains: "finite number"},
		{name: "non-finite tolerance", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "scoreTolerance": math.Inf(1)}, contains: "finite number"},
		{name: "fractional limit", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "limit": 1.5}, contains: "integer"},
		{name: "limit out of range", eventType: "semantic-search-assert", target: map[string]any{"query": "axis-0", "limit": 1001}, contains: "between 1 and 1000"},
		{name: "string boolean", eventType: "semantic-rule-enable", target: map[string]any{"enabled": "true"}, contains: "boolean"},
		{name: "unknown nested field", eventType: "semantic-rebuild", target: map[string]any{"concurrentMutation": map[string]any{"nodeId": "node-1", "txet": "axis-1"}}, contains: "not supported"},
		{name: "wrong nested event type", eventType: "semantic-rebuild", target: map[string]any{"concurrentMutation": map[string]any{"eventType": true, "nodeId": "node-1"}}, contains: "nonempty string"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSemanticEventTarget(test.eventType, test.target)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("ValidateSemanticEventTarget() error=%v, want substring %q", err, test.contains)
			}
		})
	}
}

func TestCanonicalSemanticScenarioPassesStrictTargetValidation(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-contract", "compose-semantic-graph-contract.yaml")
	if _, err := LoadFile(path); err != nil {
		t.Fatalf("LoadFile(canonical semantic scenario) error=%v", err)
	}
}
