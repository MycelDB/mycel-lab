package spec

import (
	"gopkg.in/yaml.v3"
	"testing"
)

func TestSemanticOrderingAndFaultTargetsFailClosed(t *testing.T) {
	ordering := map[string]any{"nodeId": "00000000-0000-4000-8000-000000004660", "gate": "older-response", "text": "axis-2", "expectRevisionFrom": "old", "captureRevisionAs": "new", "captureRecordAs": "record", "timeout": "30s", "pollInterval": "100ms"}
	if err := ValidateSemanticEventTarget("semantic-ordering-assert", ordering); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{"gate": "../escape", "nodeId": "opaque", "captureRecordAs": "bad:name", "allowStale": true, "timeout": "0s"} {
		copy := map[string]any{}
		for k, v := range ordering {
			copy[k] = v
		}
		copy[key] = value
		if ValidateSemanticEventTarget("semantic-ordering-assert", copy) == nil {
			t.Fatalf("invalid ordering field %s passed", key)
		}
	}
	for _, mode := range []string{"retryable", "permanent", "deadline", "cancel"} {
		target := map[string]any{"nodeId": ordering["nodeId"], "expectRevisionFrom": "commit", "disposition": mode}
		if mode == "cancel" {
			target["gate"] = "cancel-response"
		}
		if mode == "deadline" {
			target["processTimeout"] = "200ms"
		}
		if err := ValidateSemanticEventTarget("semantic-fault-assert", target); err != nil {
			t.Fatal(err)
		}
		target["repairIfMissing"] = true
		if ValidateSemanticEventTarget("semantic-fault-assert", target) == nil {
			t.Fatal("fault control silently accepted repair")
		}
	}
	for _, target := range []map[string]any{
		{"nodeId": ordering["nodeId"], "expectRevisionFrom": "commit", "disposition": "cancel"},
		{"nodeId": ordering["nodeId"], "expectRevisionFrom": "commit", "disposition": "deadline"},
		{"nodeId": ordering["nodeId"], "expectRevisionFrom": "commit", "disposition": "permanent", "gate": "ignored"},
		{"nodeId": ordering["nodeId"], "expectRevisionFrom": "commit", "disposition": "permanent", "minimumBackoff": "1s"},
	} {
		if ValidateSemanticEventTarget("semantic-fault-assert", target) == nil {
			t.Fatal("ambiguous fault mode accepted")
		}
	}
}
func TestSemanticCompositionTypoCannotBecomeEmbeddingOnly(t *testing.T) {
	raw := []byte("targetActorGroup: controller\nproviderStagez: []\nrule:\n  key: sample\n  targetLabels: [Document]\n")
	var semantic SemanticScenarioSpec
	if err := yaml.Unmarshal(raw, &semantic); err == nil {
		t.Fatal("misspelled composition silently ignored")
	}
}
