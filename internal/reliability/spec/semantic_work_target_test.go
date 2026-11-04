package spec

import (
	"strings"
	"testing"
)

func TestSemanticWorkTargetIsStrictAndPreservationIsReadOnly(t *testing.T) {
	valid := map[string]any{"nodeId": "00000000-0000-4000-8000-000000004614", "expectedStatus": "pending", "expectRevisionFrom": "pending-commit", "captureWorkAs": "pending-before", "analyze": true, "expectedAttemptCount": 0, "expectBackgroundMaintenanceEnabled": false, "timeout": "30s", "pollInterval": "100ms"}
	if err := ValidateSemanticEventTarget("semantic-work-assert", valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"unknown field":               func(m map[string]any) { m["rebuildIfMissing"] = true },
		"missing node":                func(m map[string]any) { delete(m, "nodeId") },
		"invalid node":                func(m map[string]any) { m["nodeId"] = "opaque" },
		"uppercase node":              func(m map[string]any) { m["nodeId"] = "00000000-0000-4000-8000-00000000AAAA" },
		"zero node":                   func(m map[string]any) { m["nodeId"] = "00000000-0000-0000-0000-000000000000" },
		"missing committed revision":  func(m map[string]any) { delete(m, "expectRevisionFrom") },
		"unbounded capture":           func(m map[string]any) { m["captureWorkAs"] = strings.Repeat("x", 129) },
		"capture alias":               func(m map[string]any) { m["expectWorkFrom"] = "semantic-work-v1:other" },
		"unsupported status":          func(m map[string]any) { m["expectedStatus"] = "completed" },
		"preservation reconstruction": func(m map[string]any) { m["expectWorkFrom"] = "before" },
		"completion analysis":         func(m map[string]any) { m["expectedStatus"] = "complete" },
		"negative attempts":           func(m map[string]any) { m["expectedAttemptCount"] = -1 },
		"fractional attempts":         func(m map[string]any) { m["expectedAttemptCount"] = 1.5 },
		"string boolean":              func(m map[string]any) { m["expectBackgroundMaintenanceEnabled"] = "false" },
		"zero timeout":                func(m map[string]any) { m["timeout"] = "0s" },
	} {
		t.Run(name, func(t *testing.T) {
			target := map[string]any{}
			for key, value := range valid {
				target[key] = value
			}
			mutate(target)
			if err := ValidateSemanticEventTarget("semantic-work-assert", target); err == nil {
				t.Fatal("invalid work contract passed")
			}
		})
	}
	delete(valid, "analyze")
	delete(valid, "captureWorkAs")
	valid["expectWorkFrom"], valid["expectedStatus"], valid["expectedAttemptCount"] = "pending-before", "complete", 1
	if err := ValidateSemanticEventTarget("semantic-work-assert", valid); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalSemanticUUIDRejectsAliases(t *testing.T) {
	if !IsCanonicalSemanticUUID("00000000-0000-4000-8000-000000004614") {
		t.Fatal("canonical UUID rejected")
	}
	for _, value := range []string{"", "00000000-0000-0000-0000-000000000000", "00000000000040008000000000004614", "{00000000-0000-4000-8000-000000004614}", "urn:uuid:00000000-0000-4000-8000-000000004614", "00000000-0000-4000-8000-00000000ABCD", " 00000000-0000-4000-8000-000000004614", "00000000_0000-4000-8000-000000004614"} {
		if IsCanonicalSemanticUUID(value) {
			t.Fatalf("UUID alias accepted: %q", value)
		}
	}
}
