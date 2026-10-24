package spec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProcedureInferenceScenarioContractsValidate(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios")
	paths := []string{
		filepath.Join(root, "compose-procedure-inference-control-emulator", "compose-procedure-inference-control-emulator.yaml"),
		filepath.Join(root, "k3d-procedure-inference-recovery", "k3d-procedure-inference-recovery.yaml"),
		filepath.Join(root, "k3d-procedure-inference-backup-restore", "k3d-procedure-inference-backup-restore.yaml"),
	}
	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			loaded, err := LoadFile(path)
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}
			scenario, ok := loaded.(Scenario)
			if !ok || scenario.ProcedureInference == nil {
				t.Fatalf("loaded=%T procedureInference=%+v", loaded, scenario.ProcedureInference)
			}
			if scenario.ProviderProfileRef != "openai-compatible-procedure-emulator" || scenario.ProcedureInference.TargetActorGroup != "graph-writers" || scenario.ProcedureInference.AuditActorGroup != "inference-auditors" {
				t.Fatalf("unexpected procedure inference contract: %+v", scenario.ProcedureInference)
			}
			if scenario.Environment.Driver == "compose" {
				files, ok := scenario.Environment.Options["composeFiles"].([]any)
				if !ok || len(files) == 0 {
					t.Fatal("compose scenario has no composeFiles")
				}
				for _, file := range files {
					name, ok := file.(string)
					if !ok {
						t.Fatalf("compose file is %T", file)
					}
					if _, err := os.Stat(filepath.Join("..", "..", "..", name)); err != nil {
						t.Fatalf("compose file %q: %v", name, err)
					}
				}
			}
		})
	}
}

func TestProcedureInferenceEventTargetsFailClosed(t *testing.T) {
	tests := []struct {
		name, event string
		target      map[string]any
		contains    string
	}{
		{name: "unknown field", event: "procedure-inference-setup", target: map[string]any{"timeout": "1s", "prompt": "unsafe"}, contains: "unsupported field"},
		{name: "unbounded text", event: "procedure-inference-node-create", target: map[string]any{"nodeId": "n", "text": strings.Repeat("x", 4097)}, contains: "1-4096"},
		{name: "invalid stable duration", event: "procedure-inference-await-terminal", target: map[string]any{"stableFor": "0s"}, contains: "positive duration"},
		{name: "unbounded attempts", event: "procedure-inference-await-terminal", target: map[string]any{"minimumAttempts": 1, "maximumAttempts": 257}, contains: "256"},
		{name: "unknown control", event: "procedure-inference-control", target: map[string]any{"action": "revoke-all"}, contains: "enable or disable"},
		{name: "emulator string boolean", event: "procedure-inference-emulator-assert", target: validEmulatorTarget(map[string]any{"requireSingleItem": "maybe"}), contains: "must be boolean"},
		{name: "emulator invalid outcome count", event: "procedure-inference-emulator-assert", target: validEmulatorTarget(map[string]any{"minimumOutcomes": map[string]any{"success": "nope"}}), contains: "bounded nonnegative integers"},
		{name: "emulator blank model", event: "procedure-inference-emulator-assert", target: validEmulatorTarget(map[string]any{"expectedModels": []any{""}}), contains: "nonempty string list"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateProcedureInferenceEventTarget(test.event, test.target)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error=%v, want %q", err, test.contains)
			}
		})
	}
}

func TestProcedureInferenceEmulatorTargetAllowsExactZeroAfterNamespaceReset(t *testing.T) {
	target := map[string]any{"minimumRequests": 0, "maximumRequests": 0, "maximumBatchItems": 1, "maximumAttemptsPerInput": 1, "authorization": "absent"}
	if err := ValidateProcedureInferenceEventTarget("procedure-inference-emulator-assert", target); err != nil {
		t.Fatalf("zero-request emulator assertion error=%v", err)
	}
}

func validEmulatorTarget(overrides map[string]any) map[string]any {
	target := map[string]any{
		"minimumRequests": 1, "maximumRequests": 2, "requireSingleItem": true,
		"maximumBatchItems": 1, "maximumAttemptsPerInput": 2, "authorization": "absent",
		"expectedModels": []any{"model"}, "minimumOutcomes": map[string]any{"success": 1}, "minimumStatuses": map[string]any{"200": 1},
	}
	for key, value := range overrides {
		target[key] = value
	}
	return target
}
