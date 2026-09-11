package events

import (
	"context"
	"testing"

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
