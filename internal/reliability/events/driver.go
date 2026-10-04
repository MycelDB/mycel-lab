package events

import (
	"context"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type DriverRuntime struct {
	Driver      env.EnvironmentDriver
	Environment env.Environment
}

func NewDriverRuntime(driver env.EnvironmentDriver, environment env.Environment) DriverRuntime {
	return DriverRuntime{Driver: driver, Environment: environment}
}

func (r DriverRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error {
	for _, event := range phase.Events {
		if err := r.executeOne(ctx, phase, event, dryRun, recorder); err != nil {
			return err
		}
	}
	return nil
}

func (r DriverRuntime) executeOne(ctx context.Context, phase spec.PhaseSpec, event spec.EventSpec, dryRun bool, recorder Recorder) error {
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"dryRun": dryRun, "runtime": r.Environment.Driver}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		return r.recordFailure(ctx, recorder, phase, event, err)
	}
	if !dryRun {
		if err := r.applyEvent(ctx, event); err != nil {
			return r.recordFailure(ctx, recorder, phase, event, err)
		}
	}
	if event.Duration.Duration > 0 {
		timer := time.NewTimer(event.Duration.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if recorder != nil {
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"runtime": r.Environment.Driver}})
	}
	return nil
}

func (r DriverRuntime) applyEvent(ctx context.Context, event spec.EventSpec) error {
	switch event.Type {
	case "pod-stop", "pod-restart", "pod-delete", "node-stop", "node-restart":
		return r.Driver.RestartNode(ctx, r.Environment, env.NodeRef{Name: nodeName(event.Target), Ordinal: nodeOrdinal(event.Target)})
	case "rolling-restart":
		return r.Driver.RollingRestart(ctx, r.Environment)
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}
}

func nodeName(target map[string]any) string {
	if pod := PodName(target); pod != "" {
		return pod
	}
	if node, _ := target["node"].(string); node != "" {
		return node
	}
	if service, _ := target["service"].(string); service != "" {
		return service
	}
	return ""
}

func nodeOrdinal(target map[string]any) int {
	for _, key := range []string{"ordinal", "nodeOrdinal"} {
		switch v := target[key].(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return -1
}

func (r DriverRuntime) recordFailure(ctx context.Context, recorder Recorder, phase spec.PhaseSpec, event spec.EventSpec, err error) error {
	if recorder != nil {
		_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"error": err.Error(), "runtime": r.Environment.Driver}})
	}
	return err
}
