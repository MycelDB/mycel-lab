package events

import (
	"context"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type FailureClass string

const (
	FailureTransient   FailureClass = "transient"
	FailureUnavailable FailureClass = "unavailable"
	FailurePermanent   FailureClass = "permanent"
	FailureCorrectness FailureClass = "correctness"
)

type Record struct {
	Time      time.Time      `json:"time"`
	Type      string         `json:"type"`
	PhaseName string         `json:"phaseName"`
	EventType string         `json:"eventType"`
	Target    map[string]any `json:"target,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type Recorder interface {
	RecordRuntimeEvent(ctx context.Context, record Record) error
}

type Runtime interface {
	ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error
}

type LocalRuntime struct{}

func (LocalRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error {
	for _, event := range phase.Events {
		if err := executeOne(ctx, phase, event, dryRun, recorder); err != nil {
			return err
		}
	}
	return nil
}

func executeOne(ctx context.Context, phase spec.PhaseSpec, event spec.EventSpec, dryRun bool, recorder Recorder) error {
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"dryRun": dryRun}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		if recorder != nil {
			_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"error": err.Error()}})
		}
		return err
	}
	if !dryRun && event.Duration.Duration > 0 {
		timer := time.NewTimer(event.Duration.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if recorder != nil {
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target})
	}
	return nil
}

func validateEvent(event spec.EventSpec) error {
	switch event.Type {
	case "pod-stop", "pod-restart":
		if PodName(event.Target) == "" {
			return fmt.Errorf("%s requires target.pod", event.Type)
		}
	case "rolling-restart":
		return nil
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}
	return nil
}

func PodName(target map[string]any) string {
	pod, _ := target["pod"].(string)
	return pod
}

func IsExpectedDegradation(phase spec.PhaseSpec) bool {
	return phase.Outcome["availability"] == "expected-degradation"
}

func FailureIsMaskedByExpectedDegradation(phase spec.PhaseSpec, class FailureClass) bool {
	if !IsExpectedDegradation(phase) {
		return false
	}
	return class == FailureTransient || class == FailureUnavailable
}

func CorrectnessFailureFailsScenario(phase spec.PhaseSpec) bool {
	return !FailureIsMaskedByExpectedDegradation(phase, FailureCorrectness)
}

func ArtifactEvent(record Record) artifacts.Event {
	return artifacts.Event{Time: record.Time, Type: record.Type, PhaseName: record.PhaseName, Payload: map[string]any{"eventType": record.EventType, "target": record.Target, "payload": record.Payload}}
}
