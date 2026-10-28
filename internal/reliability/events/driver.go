package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DriverRuntime struct {
	Driver                        env.EnvironmentDriver
	Environment                   env.Environment
	Scenario                      spec.ResolvedScenario
	Resources                     *provision.ScenarioResources
	EndpointRecovery              env.EndpointRecovery
	endpointProbe                 func(context.Context, *provision.ScenarioResources, string) (endpointProbeObservation, error)
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

func (r *DriverRuntime) liveSemanticProvider() bool {
	return r.Scenario.ProviderProfile != nil && r.Scenario.ProviderProfile.Profile.Mode == "live" && r.Scenario.Semantic != nil
}

func (r *DriverRuntime) evidenceSafeEventTarget(event spec.EventSpec) map[string]any {
	if !r.liveSemanticProvider() || !strings.HasPrefix(event.Type, "semantic-") {
		return evidenceSafeEventTarget(event)
	}
	return evidenceSafeLiveSemanticEventTarget(event)
}

func evidenceSafeLiveSemanticEventTarget(event spec.EventSpec) map[string]any {
	allowed := map[string]struct{}{}
	switch event.Type {
	case "semantic-backfill":
		allowed = map[string]struct{}{"force": {}, "limit": {}, "expectedSelected": {}, "minimumGenerated": {}}
	case "semantic-maintenance-await":
		allowed = map[string]struct{}{"timeout": {}, "pollInterval": {}}
	case "semantic-search-assert":
		allowed = map[string]struct{}{"limit": {}, "minimumScore": {}, "expectedResultCount": {}, "requireProvenance": {}, "requireActualRoute": {}, "allPages": {}, "allowWarnings": {}}
	case "semantic-reliability-assert":
		allowed = map[string]struct{}{"expectRebuildRequired": {}, "maximumUnresolvedGapCount": {}, "requireFreshAtLatest": {}, "minimumSemanticGeneration": {}, "requireDeltaAfterPinned": {}}
	case "semantic-provider-budget-reconcile":
		allowed = map[string]struct{}{"minimumAttempts": {}, "requireTerminal": {}}
	case "semantic-rule-enable":
		allowed = map[string]struct{}{"enabled": {}}
	}
	out := map[string]any{"sensitiveSemanticInputsRedacted": true}
	for key, value := range event.Target {
		if _, ok := allowed[key]; ok {
			out[key] = value
		}
	}
	return out
}

func sanitizeLiveSemanticResult(eventType string, result any) any {
	switch eventType {
	case "semantic-maintenance-await":
		if value, ok := result.(SemanticMaintenanceResult); ok {
			return map[string]any{"iterations": len(value.Observations), "converged": value.Converged}
		}
		return map[string]any{"completed": result != nil}
	case "semantic-reliability-assert":
		return map[string]any{"asserted": result != nil, "sensitiveSemanticOutputsRedacted": true}
	case "semantic-unauthorized-search":
		if value, ok := result.(SemanticAuthorizationProbeResult); ok {
			return map[string]any{
				"rejected": value.Rejected, "rejectedCode": value.RejectedCode,
				"providerBudgetObserved": value.ProviderBudgetObserved, "providerBudgetUnchanged": value.ProviderBudgetUnchanged,
				"requests": value.Requests, "attempts": value.Attempts, "retryAttempts": value.RetryAttempts,
				"batchItems": value.BatchItems, "inputTokens": value.InputTokens, "estimatedCostMicros": value.EstimatedCostMicros,
				"activeProviderCalls": value.ActiveProviderCalls, "ambiguousAttempts": value.AmbiguousAttempts,
				"sensitiveSemanticIdentifiersRedacted": true,
			}
		}
		return map[string]any{"rejected": false, "providerBudgetObserved": false, "sensitiveSemanticIdentifiersRedacted": true}
	case "semantic-rebuild":
		return map[string]any{"completed": result != nil, "sensitiveSemanticOutputsRedacted": true}
	case "semantic-provider-budget-reconcile":
		if value, ok := result.(SemanticProviderBudgetReconciliation); ok {
			return map[string]any{"requests": value.Requests, "attempts": value.Attempts, "retryAttempts": value.RetryAttempts, "batchItems": value.BatchItems, "completedAttempts": value.CompletedAttempts, "ambiguousAttempts": value.AmbiguousAttempts, "reservedEvents": value.ReservedEvents, "terminalEvents": value.TerminalEvents, "historyComplete": value.HistoryComplete, "attemptPairsValid": value.AttemptPairsValid, "usageWithinLimits": value.UsageWithinLimits, "retainedScopeDiscovered": value.RetainedScopeDiscovered, "sensitiveSemanticIdentifiersRedacted": true}
		}
		return map[string]any{"reconciled": result != nil}
	case "semantic-rule-enable", "semantic-rule-projection-update":
		return map[string]any{"completed": result != nil}
	}
	switch value := result.(type) {
	case SemanticGraphMutationResult:
		return map[string]any{"action": value.Action, "committed": value.CommittedRevision > 0, "sensitiveSemanticInputsRedacted": true}
	case SemanticSearchEvidence:
		return map[string]any{"resultCount": len(value.Results), "warningCount": len(value.Warnings), "rejectedCode": value.RejectedCode, "rankingInvariantChecked": true, "sensitiveSemanticOutputsRedacted": true}
	case SemanticBackfillResult:
		return map[string]any{"selected": value.Selected, "generated": value.Generated, "skipped": value.Skipped, "failed": value.Failed}
	}
	if eventType == "semantic-rule-create" {
		return map[string]any{"created": true}
	}
	return map[string]any{"completed": result != nil, "sensitiveSemanticOutputsRedacted": true}
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
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: r.evidenceSafeEventTarget(event), Payload: map[string]any{"dryRun": dryRun, "runtime": r.Environment.Driver}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		return r.recordFailure(ctx, recorder, phase, event, nil, err)
	}
	completionPayload := map[string]any{"runtime": r.Environment.Driver, "dryRun": dryRun}
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
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: r.evidenceSafeEventTarget(event), Payload: completionPayload})
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
	case "semantic-node-create", "semantic-node-update", "semantic-node-delete", "semantic-transaction-negative", "semantic-rule-create", "semantic-backfill", "semantic-maintenance-await", "semantic-search-assert", "semantic-unauthorized-search", "semantic-rule-enable", "semantic-rule-projection-update", "semantic-reliability-assert", "semantic-rebuild", "semantic-provider-budget-reconcile":
		result, err := executeSemanticEvent(ctx, r.Resources, event.Type, event.Target)
		if r.liveSemanticProvider() {
			result = sanitizeLiveSemanticResult(event.Type, result)
		}
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
		if event.Type == "system-backup-fixture" || event.Type == "cluster-backup-create" || event.Type == "cluster-restore-apply" {
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

func liveSemanticFailureCategory(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	switch status.Code(err) {
	case codes.Unauthenticated:
		return "unauthenticated"
	case codes.PermissionDenied:
		return "permission_denied"
	case codes.NotFound:
		return "not_found"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Unavailable:
		return "unavailable"
	case codes.ResourceExhausted:
		return "resource_exhausted"
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.Aborted:
		return "aborted"
	case codes.Internal:
		return "internal"
	default:
		return "operation_failed"
	}
}

func (r *DriverRuntime) recordFailure(ctx context.Context, recorder Recorder, phase spec.PhaseSpec, event spec.EventSpec, eventPayload map[string]any, err error) error {
	returnedError := err
	payload := map[string]any{"error": err.Error(), "runtime": r.Environment.Driver}
	if r.liveSemanticProvider() && strings.HasPrefix(event.Type, "semantic-") {
		category := liveSemanticFailureCategory(err)
		payload = map[string]any{"errorCategory": category, "runtime": r.Environment.Driver, "sensitiveSemanticFailureRedacted": true}
		returnedError = fmt.Errorf("live semantic event %s failed (%s)", event.Type, category)
	}
	if recorder != nil {
		for key, value := range eventPayload {
			payload[key] = value
		}
		_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: r.evidenceSafeEventTarget(event), Payload: payload})
	}
	return returnedError
}
