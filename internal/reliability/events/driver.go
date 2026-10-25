package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type DriverRuntime struct {
	Driver                        env.EnvironmentDriver
	Environment                   env.Environment
	Scenario                      spec.ResolvedScenario
	Resources                     *provision.ScenarioResources
	EndpointRecovery              env.EndpointRecovery
	endpointProbe                 func(context.Context, *provision.ScenarioResources, string) (int, error)
	userBackups                   map[string]UserBackupOperationResult
	systemBackups                 map[string]SystemBackupOperationResult
	procedureInferenceCheckpoints map[string]procedureInferenceCheckpoint
}

func NewDriverRuntime(driver env.EnvironmentDriver, environment env.Environment) *DriverRuntime {
	return &DriverRuntime{Driver: driver, Environment: environment, userBackups: map[string]UserBackupOperationResult{}, systemBackups: map[string]SystemBackupOperationResult{}, procedureInferenceCheckpoints: map[string]procedureInferenceCheckpoint{}}
}

func NewDriverRuntimeWithResources(driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources) *DriverRuntime {
	return &DriverRuntime{Driver: driver, Environment: environment, Resources: resources, userBackups: map[string]UserBackupOperationResult{}, systemBackups: map[string]SystemBackupOperationResult{}, procedureInferenceCheckpoints: map[string]procedureInferenceCheckpoint{}}
}

func NewDriverRuntimeWithScenario(driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, resources *provision.ScenarioResources) *DriverRuntime {
	return &DriverRuntime{Driver: driver, Environment: environment, Scenario: scenario, Resources: resources, userBackups: map[string]UserBackupOperationResult{}, systemBackups: map[string]SystemBackupOperationResult{}, procedureInferenceCheckpoints: map[string]procedureInferenceCheckpoint{}}
}

func (r *DriverRuntime) SetEndpointRecovery(recovery env.EndpointRecovery) {
	r.EndpointRecovery = recovery
}

func (r *DriverRuntime) CleanupSensitiveHostArtifacts() error {
	var failures []error
	for key, backup := range r.systemBackups {
		if backup.HostDir != "" {
			if !safeSystemBackupHostDir(backup.HostDir) {
				failures = append(failures, errors.New("sensitive backup cleanup path is outside the managed temporary root"))
			} else if err := os.RemoveAll(backup.HostDir); err != nil {
				failures = append(failures, errors.New("remove sensitive backup host directory"))
			} else if _, err := os.Stat(backup.HostDir); !os.IsNotExist(err) {
				failures = append(failures, errors.New("sensitive backup host directory still exists after cleanup"))
			}
		}
		delete(r.systemBackups, key)
	}
	return errors.Join(failures...)
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
	operationCtx, release, err := reserveRuntimeOperation(ctx, recorder, phase.Name, event.Type)
	if err != nil {
		return err
	}
	defer release()
	ctx = operationCtx
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: evidenceSafeEventTarget(event), Payload: map[string]any{"dryRun": dryRun, "runtime": r.Environment.Driver}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		return r.recordFailure(ctx, recorder, phase, event, nil, err)
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
				return r.recordFailure(ctx, recorder, phase, event, payload, err)
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
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: evidenceSafeEventTarget(event), Payload: completionPayload})
	}
	return nil
}

func (r *DriverRuntime) applyEvent(ctx context.Context, event spec.EventSpec) (map[string]any, error) {
	switch event.Type {
	case "pod-stop", "pod-restart", "pod-delete", "node-stop", "node-restart":
		return r.restartNodeAndRecoverEndpoint(ctx, event.Target)
	case "rolling-restart":
		return r.rollingRestartAndRecoverEndpoints(ctx)
	case "statefulset-image-upgrade":
		snapshots := r.managedEndpointSnapshots()
		result, err := executeStatefulSetImageUpgradeEvent(ctx, r.Driver, r.Environment, event.Target)
		payload := map[string]any{"statefulSetImageUpgrade": result}
		if err != nil || len(snapshots) == 0 {
			return payload, err
		}
		recoveries, recoveryErr := r.recoverEndpoints(ctx, snapshots)
		payload["endpointRecoveries"] = recoveries
		return payload, recoveryErr
	case "procedure-inference-setup", "procedure-inference-node-create", "procedure-inference-await-terminal", "procedure-inference-control", "procedure-inference-capacity-diagnostics-assert":
		result, err := executeProcedureInferenceEvent(ctx, r.Driver, r.Environment, r.Resources, r.procedureInferenceCheckpoints, event.Type, event.Target)
		return map[string]any{"procedureInference": result}, err
	case "procedure-inference-emulator-assert":
		result, err := executeSemanticProviderEmulatorAssert(ctx, r.Driver, r.Environment, event.Target)
		return map[string]any{"procedureInferenceEmulator": result}, err
	case "commonfolio-automation-seed", "commonfolio-automation-diagnostics":
		result, err := executeCommonFolioAutomationEvent(ctx, r.Driver, r.Environment, r.Resources, event.Type, event.Target)
		return map[string]any{"commonfolioAutomation": result}, err
	case "commonfolio-automation-await-terminal":
		result, err := executeCommonFolioAwaitTerminalEvent(ctx, r.Driver, r.Environment, r.Resources, event.Target)
		return map[string]any{"commonfolioTerminalFixture": result}, err
	case "semantic-node-create", "semantic-node-update", "semantic-node-delete", "semantic-transaction-negative", "semantic-rule-create", "semantic-backfill", "semantic-maintenance-await", "semantic-search-assert", "semantic-unauthorized-search", "semantic-rule-enable", "semantic-rule-projection-update", "semantic-reliability-assert", "semantic-rebuild":
		result, err := executeSemanticEvent(ctx, r.Resources, event.Type, event.Target)
		return map[string]any{"semantic": result}, err
	case "cluster-recovery-wait":
		result, err := executeClusterRecoveryWaitEvent(ctx, r.Driver, r.Environment, r.Scenario, event.Target)
		return map[string]any{"clusterRecovery": result}, err
	case "log-assert-absent":
		result, err := executeLogAssertAbsentEvent(ctx, r.Environment, event.Target)
		return map[string]any{"logAssertAbsent": result}, err
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
	case "system-backup-fixture", "cluster-backup-create", "cluster-backup-validate", "cluster-restore-apply", "cluster-restore-verify", "cluster-backup-cleanup":
		var snapshots []env.EndpointForwardSnapshot
		if event.Type == "cluster-restore-apply" {
			snapshots = r.managedEndpointSnapshots()
		}
		result, err := executeSystemBackupEvent(ctx, r.Driver, r.Environment, r.Scenario, r.Resources, event.Type, event.Target, r.systemBackups)
		if event.Type == "cluster-backup-create" || event.Type == "cluster-restore-apply" {
			r.systemBackups[systemBackupKey(event.Target)] = result
		}
		payload := map[string]any{"systemBackup": result}
		if err != nil || len(snapshots) == 0 {
			return payload, err
		}
		recoveries, recoveryErr := r.recoverEndpoints(ctx, snapshots)
		payload["endpointRecoveries"] = recoveries
		return payload, recoveryErr
	case "raft-snapshot-create", "raft-pvc-replace-node", "raft-snapshot-verify-rejoined":
		var snapshots []env.EndpointForwardSnapshot
		if event.Type == "raft-pvc-replace-node" {
			snapshots = r.managedEndpointSnapshots()
		}
		result, err := executeRaftRejoinEvent(ctx, r.Driver, r.Environment, r.Scenario, event.Type, event.Target)
		payload := map[string]any{"raftSnapshot": result}
		if err != nil || len(snapshots) == 0 || event.Type != "raft-pvc-replace-node" {
			return payload, err
		}
		recovery, managed, recoveryErr := r.recoverNodeFromSnapshots(ctx, snapshots, fmt.Sprintf("myceld-%d", result.Ordinal))
		if managed {
			payload["endpointRecovery"] = recovery
		}
		return payload, recoveryErr
	case "identity-duplicate-principal-replay":
		snapshots := r.managedEndpointSnapshots()
		result, err := executeIdentityReplayEvent(ctx, r.Driver, r.Environment, r.Scenario, event.Target)
		payload := map[string]any{"identityReplay": result}
		if err != nil || len(snapshots) == 0 {
			return payload, err
		}
		recovery, managed, recoveryErr := r.recoverNodeFromSnapshots(ctx, snapshots, fmt.Sprintf("myceld-%d", result.RestartOrdinal))
		if managed {
			payload["endpointRecovery"] = recovery
		}
		return payload, recoveryErr
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

func (r *DriverRuntime) recordFailure(ctx context.Context, recorder Recorder, phase spec.PhaseSpec, event spec.EventSpec, eventPayload map[string]any, err error) error {
	if recorder != nil {
		payload := map[string]any{"error": err.Error(), "runtime": r.Environment.Driver}
		for key, value := range eventPayload {
			payload[key] = value
		}
		_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: evidenceSafeEventTarget(event), Payload: payload})
	}
	return err
}
