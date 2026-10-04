package events

import (
	"context"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type DriverRuntime struct {
	Driver      env.EnvironmentDriver
	Environment env.Environment
	Resources   *provision.ScenarioResources
	userBackups map[string]UserBackupOperationResult
}

func NewDriverRuntime(driver env.EnvironmentDriver, environment env.Environment) *DriverRuntime {
	return &DriverRuntime{Driver: driver, Environment: environment, userBackups: map[string]UserBackupOperationResult{}}
}

func NewDriverRuntimeWithResources(driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources) *DriverRuntime {
	return &DriverRuntime{Driver: driver, Environment: environment, Resources: resources, userBackups: map[string]UserBackupOperationResult{}}
}

func (r *DriverRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error {
	for _, event := range phase.Events {
		if err := r.executeOne(ctx, phase, event, dryRun, recorder); err != nil {
			return err
		}
	}
	return nil
}

func (r *DriverRuntime) executeOne(ctx context.Context, phase spec.PhaseSpec, event spec.EventSpec, dryRun bool, recorder Recorder) error {
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"dryRun": dryRun, "runtime": r.Environment.Driver}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		return r.recordFailure(ctx, recorder, phase, event, err)
	}
	completionPayload := map[string]any{"runtime": r.Environment.Driver}
	repeat := event.Repeat
	if repeat <= 0 {
		repeat = 1
	}
	completionPayload["repeat"] = repeat
	if !dryRun {
		for i := 0; i < repeat; i++ {
			payload, err := r.applyEvent(ctx, eventForIteration(event, i))
			if err != nil {
				return r.recordFailure(ctx, recorder, phase, event, err)
			}
			for key, value := range payload {
				completionPayload[key] = value
			}
			if i+1 < repeat && event.Interval.Duration > 0 {
				if err := sleepContext(ctx, event.Interval.Duration); err != nil {
					return err
				}
			}
		}
	}
	if event.Duration.Duration > 0 {
		if err := sleepContext(ctx, event.Duration.Duration); err != nil {
			return err
		}
	}
	if recorder != nil {
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: completionPayload})
	}
	return nil
}

func (r *DriverRuntime) applyEvent(ctx context.Context, event spec.EventSpec) (map[string]any, error) {
	switch event.Type {
	case "pod-stop", "pod-restart", "pod-delete", "node-stop", "node-restart":
		return nil, r.Driver.RestartNode(ctx, r.Environment, env.NodeRef{Name: nodeName(event.Target), Ordinal: nodeOrdinal(event.Target)})
	case "rolling-restart":
		return nil, r.Driver.RollingRestart(ctx, r.Environment)
	case "host-command":
		result, err := ExecuteHostCommand(ctx, event.Target)
		return map[string]any{"hostCommand": result}, err
	case "environment-reset":
		resetter, ok := r.Driver.(env.EnvironmentResetter)
		if !ok {
			return nil, fmt.Errorf("driver %q does not support environment-reset", r.Driver.Name())
		}
		return nil, resetter.Reset(ctx, r.Environment)
	case "user-backup-export", "user-backup-validate", "user-backup-import", "user-backup-fixture", "user-backup-verify-restored", "user-backup-assert-safety":
		result, err := executeUserBackupEvent(ctx, r.Driver, r.Environment, r.Resources, event.Type, event.Target, r.userBackups)
		if event.Type == "user-backup-export" || event.Type == "user-backup-import" {
			r.userBackups[userBackupKey(event.Target)] = result
		}
		return map[string]any{"userBackup": result}, err
	default:
		return nil, fmt.Errorf("unsupported event type %q", event.Type)
	}
}

func eventForIteration(event spec.EventSpec, iteration int) spec.EventSpec {
	sequence := nodeTargetOrdinalSequence(event.Target)
	if len(sequence) == 0 {
		return event
	}
	out := event
	out.Target = map[string]any{}
	for key, value := range event.Target {
		out.Target[key] = value
	}
	out.Target["ordinal"] = sequence[iteration%len(sequence)]
	return out
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
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
	return nodeTargetOrdinal(target)
}

func (r *DriverRuntime) recordFailure(ctx context.Context, recorder Recorder, phase spec.PhaseSpec, event spec.EventSpec, err error) error {
	if recorder != nil {
		_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"error": err.Error(), "runtime": r.Environment.Driver}})
	}
	return err
}
