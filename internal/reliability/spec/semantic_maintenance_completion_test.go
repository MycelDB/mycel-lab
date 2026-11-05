package spec

import "testing"

func TestSemanticMaintenanceCompletionRequiresExplicitDisabledContract(t *testing.T) {
	paused := func() map[string]any {
		return map[string]any{"completion": "disabled-quiescent", "expectGenerationFrom": "baseline-generation", "expectFreshRevisionFrom": "baseline-fresh", "expectRevisionFrom": "disabled-commit"}
	}
	for _, target := range []map[string]any{nil, {"completion": "indexed"}, paused()} {
		if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err != nil {
			t.Fatalf("valid completion rejected: %v", err)
		}
	}
	for _, key := range []string{"expectGenerationFrom", "expectFreshRevisionFrom", "expectRevisionFrom"} {
		target := paused()
		delete(target, key)
		if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err == nil {
			t.Fatalf("disabled mode accepted missing %s", key)
		}
		target = map[string]any{key: "baseline"}
		if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err == nil {
			t.Fatalf("indexed mode silently ignored %s", key)
		}
	}
	for _, mode := range []any{"", true, "quiescent", "disabled", "disabled-quiescent ", map[string]any{"ignoreFreshness": true}} {
		target := paused()
		target["completion"] = mode
		if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err == nil {
			t.Fatalf("unsupported completion accepted: %T", mode)
		}
	}
	target := paused()
	target["expectGenerationFrom"] = ""
	if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err == nil {
		t.Fatal("empty capture accepted")
	}
	target = paused()
	target["requireFreshAtLatest"] = false
	if err := ValidateSemanticEventTarget("semantic-maintenance-await", target); err == nil {
		t.Fatal("generic freshness override accepted")
	}
}
