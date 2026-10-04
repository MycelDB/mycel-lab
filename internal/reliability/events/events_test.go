package events

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestEventScheduleStartsAtPhaseBoundary(t *testing.T) {
	phase := spec.PhaseSpec{Name: "outage", Events: []spec.EventSpec{{Type: "pod-stop", Target: map[string]any{"pod": "myceld-1"}}}}
	recorder := &recordingEventRecorder{}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, recorder); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	if len(recorder.records) != 2 {
		t.Fatalf("records=%d, want start+complete", len(recorder.records))
	}
	if recorder.records[0].Type != "event-started" || recorder.records[0].PhaseName != "outage" || recorder.records[0].EventType != "pod-stop" {
		t.Fatalf("first record not event-started at phase boundary: %+v", recorder.records[0])
	}
}

func TestExpectedDegradationDoesNotMaskCorrectnessFailure(t *testing.T) {
	phase := spec.PhaseSpec{Name: "outage", Outcome: map[string]string{"availability": "expected-degradation"}}
	if !FailureIsMaskedByExpectedDegradation(phase, FailureUnavailable) {
		t.Fatal("unavailable failure should be expected degradation")
	}
	if FailureIsMaskedByExpectedDegradation(phase, FailureCorrectness) {
		t.Fatal("correctness failure must not be masked")
	}
	if !CorrectnessFailureFailsScenario(phase) {
		t.Fatal("correctness failure should fail scenario")
	}
}

func TestPodNameTargetingIsDeterministic(t *testing.T) {
	target := map[string]any{"pod": "myceld-2"}
	if PodName(target) != "myceld-2" || PodName(target) != "myceld-2" {
		t.Fatalf("pod targeting is not deterministic")
	}
}

func TestNodeRestartAcceptsOrdinalTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinal": 1}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
}

func TestHostCommandRequiresCommandTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "host", Events: []spec.EventSpec{{Type: "host-command", Target: map[string]any{}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want missing command target")
	}
}

func TestDriverRuntimeRepeatsNodeRestartWithOrdinalSequence(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntime(driver, env.Environment{Name: "test", Driver: "test"})
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinalSequence": []any{0, 1, 2}}, Repeat: 5}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	want := []int{0, 1, 2, 0, 1}
	if len(driver.restarted) != len(want) {
		t.Fatalf("restarted=%v, want %v", driver.restarted, want)
	}
	for i := range want {
		if driver.restarted[i] != want[i] {
			t.Fatalf("restarted=%v, want %v", driver.restarted, want)
		}
	}
}

func TestExecuteHostCommandCapturesOutput(t *testing.T) {
	command := []any{"sh", "-c", "printf hello"}
	if runtime.GOOS == "windows" {
		command = []any{"cmd", "/c", "echo hello"}
	}
	result, err := ExecuteHostCommand(context.Background(), map[string]any{"command": command})
	if err != nil {
		t.Fatalf("ExecuteHostCommand() error=%v", err)
	}
	if !strings.Contains(result.Stdout, "hello") {
		t.Fatalf("stdout=%q, want hello", result.Stdout)
	}
}

func TestPodStopRequiresPodTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "bad", Events: []spec.EventSpec{{Type: "pod-stop", Target: map[string]any{}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want missing pod target")
	}
}

type recordingEventRecorder struct{ records []Record }

func (r *recordingEventRecorder) RecordRuntimeEvent(_ context.Context, record Record) error {
	r.records = append(r.records, record)
	return nil
}

type recordingDriver struct{ restarted []int }

func (d *recordingDriver) Name() string { return "test" }
func (d *recordingDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeRestart)
}
func (d *recordingDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *recordingDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *recordingDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{}, nil
}
func (d *recordingDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return nil, nil
}
func (d *recordingDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *recordingDriver) Exec(context.Context, env.Environment, env.NodeRef, env.ExecRequest) (env.ExecResult, error) {
	return env.ExecResult{}, nil
}
func (d *recordingDriver) RestartNode(_ context.Context, _ env.Environment, node env.NodeRef) error {
	d.restarted = append(d.restarted, node.Ordinal)
	return nil
}
func (d *recordingDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *recordingDriver) Delete(context.Context, env.Environment) error { return nil }
