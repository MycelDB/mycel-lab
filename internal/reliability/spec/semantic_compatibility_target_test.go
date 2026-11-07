package spec

import "testing"

func TestSemanticCompatibilityTargetsAreStrictAndScoped(t *testing.T) {
	for kind, target := range map[string]map[string]any{
		"semantic-domain-peer-create":      {"name": "secondary"},
		"semantic-route-create":            {"name": "model-v2", "dimensions": 8, "vectorSpaceKey": "space-a"},
		"semantic-route-bind":              {"route": "model-v2"},
		"semantic-model-config-update":     {"route": "model-v2", "dimensions": 16},
		"semantic-domain-isolation-assert": {"peer": "secondary", "nodeId": "00000000-0000-4000-8000-000000004670", "primaryQuery": "axis-0", "peerQuery": "axis-2", "expectPrimaryRevisionFrom": "a", "expectPeerRevisionFrom": "b"},
		"semantic-search-assert":           {"scope": "secondary", "query": "axis-2", "broad": true, "requireConfiguredRoute": true, "expectedDimensions": 8, "expectedVectorSpaceKey": "space-a", "expectedPayloadText": "axis-2"},
		"semantic-reliability-assert":      {"requireGenerationAfter": "old-generation", "expectRebuildRequired": true},
	} {
		if err := ValidateSemanticEventTarget(kind, target); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		target["ignoreIsolationFailure"] = true
		if ValidateSemanticEventTarget(kind, target) == nil {
			t.Fatal("unknown field silently weakened compatibility assertion")
		}
	}
	for _, target := range []map[string]any{{"route": "model-v2"}, {"route": "model-v2", "dimensions": 16, "vectorSpaceKey": "space-b"}, {"route": "model-v2", "dimensions": 0}} {
		if ValidateSemanticEventTarget("semantic-model-config-update", target) == nil {
			t.Fatal("ambiguous configured-model mutation accepted")
		}
	}
	if ValidateSemanticEventTarget("semantic-search-assert", map[string]any{"query": "axis-0", "configuredRoute": "model-v2"}) == nil {
		t.Fatal("configured route ignored without its provenance assertion")
	}
	if ValidateSemanticEventTarget("semantic-ordering-assert", map[string]any{"nodeId": "00000000-0000-4000-8000-000000004670", "gate": "older", "text": "axis-2", "expectRevisionFrom": "old", "captureRevisionAs": "new", "captureRecordAs": "record", "scope": "ignored"}) == nil {
		t.Fatal("scope accepted by an event that does not select domain scope")
	}
}
