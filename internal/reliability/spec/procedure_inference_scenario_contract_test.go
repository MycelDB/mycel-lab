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

func TestProcedureInferenceBackupRestoreScenarioRecoveryFences(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "k3d-procedure-inference-backup-restore", "k3d-procedure-inference-backup-restore.yaml")
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	scenario := loaded.(Scenario)
	phases := make(map[string]PhaseSpec, len(scenario.Phases))
	for _, phase := range scenario.Phases {
		phases[phase.Name] = phase
	}
	initial := phases["establish-backed-up-attempt"].Events[0]
	if initial.Type != "cluster-recovery-wait" || initial.Target["timeout"] != "2m" || initial.Target["minimumStablePolls"] != 3 || initial.Target["requireZeroApplyLag"] != true || initial.Target["requireConsistentProgress"] != true {
		t.Fatalf("initial recovery fence=%+v", initial)
	}
	if _, legacy := initial.Target["reuseEndpointSessions"]; legacy {
		t.Fatal("initial recovery fence retains removed endpoint-session option")
	}
	negativeControl := phases["create-post-backup-negative-control"]
	if stable := negativeControl.Events[2].Target["stableFor"]; stable != "3s" {
		t.Fatalf("post-backup terminal stability=%v, want 3s", stable)
	}
	restore := phases["restore-fresh-pvcs"]
	if len(restore.Events) != 3 || restore.Events[0].Type != "cluster-restore-apply" || restore.Events[1].Type != "cluster-recovery-wait" || restore.Events[2].Type != "procedure-inference-emulator-assert" {
		t.Fatalf("restore events=%+v, want apply, stable recovery, zero-call assertion", restore.Events)
	}
	recovery := restore.Events[1].Target
	if recovery["timeout"] != "2m" || recovery["minimumStablePolls"] != 3 || recovery["requireZeroApplyLag"] != true || recovery["requireConsistentProgress"] != true {
		t.Fatalf("post-restore recovery fence=%+v", recovery)
	}
	expectedSequences := map[string]string{
		"capture-system-backup":                 "cluster-backup-create,cluster-backup-validate,procedure-inference-emulator-assert",
		"create-post-backup-negative-control":   "procedure-inference-control,procedure-inference-node-create,procedure-inference-await-terminal,procedure-inference-emulator-assert",
		"restore-fresh-pvcs":                    "cluster-restore-apply,cluster-recovery-wait,procedure-inference-emulator-assert",
		"verify-restored-ledger-and-watermarks": "cluster-restore-verify,procedure-inference-await-terminal,procedure-inference-control,procedure-inference-node-create,procedure-inference-await-terminal,procedure-inference-control,procedure-inference-node-create,procedure-inference-await-terminal,procedure-inference-emulator-assert,procedure-inference-capacity-diagnostics-assert,cluster-backup-cleanup",
	}
	for phaseName, expected := range expectedSequences {
		events := phases[phaseName].Events
		types := make([]string, 0, len(events))
		for _, event := range events {
			types = append(types, event.Type)
		}
		if observed := strings.Join(types, ","); observed != expected {
			t.Fatalf("phase %s events=%s, want %s", phaseName, observed, expected)
		}
	}
	establish := phases["establish-backed-up-attempt"]
	if establish.Events[6].Target["checkpoint"] != "disabled-barrier" {
		t.Fatalf("disable barrier checkpoint=%v", establish.Events[6].Target)
	}
	final := phases["verify-restored-ledger-and-watermarks"]
	if final.Events[0].Target["expectedNodes"] != 3 || final.Events[0].Target["expectedEdges"] != 1 || final.Events[1].Target["compareCheckpoint"] != "backed-up-ledger" || final.Events[2].Target["compareCheckpoint"] != "disabled-barrier" {
		t.Fatalf("restored identity checkpoints are incomplete: %+v", final.Events[:3])
	}
	if final.Events[8].Target["minimumRequests"] != 1 || final.Events[8].Target["maximumRequests"] != 1 || final.Events[10].Type != "cluster-backup-cleanup" {
		t.Fatalf("final provider and cleanup bounds are incomplete: %+v", final.Events[8:])
	}
	finalAssertions, _ := scenario.Assertions["final"].(map[string]any)
	if finalAssertions["requireConvergedCounts"] != true || finalAssertions["requireNoAcknowledgedGraphTransactionLoss"] == true {
		t.Fatalf("final assertions=%+v, want registered fixture convergence without generic transaction-loss claim", finalAssertions)
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
		{name: "unsafe control checkpoint", event: "procedure-inference-control", target: map[string]any{"action": "disable", "checkpoint": "../unsafe"}, contains: "safe checkpoint"},
		{name: "conflicting control checkpoints", event: "procedure-inference-control", target: map[string]any{"action": "disable", "checkpoint": "before", "compareCheckpoint": "after"}, contains: "cannot set"},
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
