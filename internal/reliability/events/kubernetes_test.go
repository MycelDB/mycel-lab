package events

import (
	"context"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestKubernetesRuntimePodStopDeletesPodAndWaits(t *testing.T) {
	runner := &fakeRunner{}
	runtime := NewKubernetesRuntime("k3d-test", "mycel-lab", runner)
	phase := spec.PhaseSpec{Name: "outage", Events: []spec.EventSpec{{Type: "pod-stop", Target: map[string]any{"pod": "myceld-2"}}}}
	recorder := &recordingEventRecorder{}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"kubectl --context k3d-test -n mycel-lab delete pod myceld-2", "kubectl --context k3d-test -n mycel-lab wait pods -l app=myceld"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
	if len(recorder.records) != 2 || recorder.records[0].Type != "event-started" || recorder.records[1].Type != "event-completed" {
		t.Fatalf("records=%+v", recorder.records)
	}
}

func TestKubernetesRuntimeRollingRestart(t *testing.T) {
	runner := &fakeRunner{}
	runtime := NewKubernetesRuntime("k3d-test", "mycel-lab", runner)
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "rolling-restart", Target: map[string]any{"batchSize": 1}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"rollout restart statefulset/myceld", "rollout status statefulset/myceld", "wait pods -l app=myceld"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
}

func TestKubernetesRuntimeDryRunRecordsWithoutKubectl(t *testing.T) {
	runner := &fakeRunner{}
	runtime := NewKubernetesRuntime("k3d-test", "mycel-lab", runner)
	phase := spec.PhaseSpec{Name: "outage", Events: []spec.EventSpec{{Type: "pod-restart", Target: map[string]any{"pod": "myceld-1"}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, true, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents(dry-run) error=%v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("dry-run executed commands: %+v", runner.commands)
	}
}

type fakeRunner struct{ commands []string }

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	return executil.Result{Command: cmd, Stdout: "ok\n"}, nil
}
