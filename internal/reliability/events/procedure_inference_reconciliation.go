package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

type procedureInferenceReconciliationOperation struct {
	OperationID       string `json:"operation_id"`
	Status            string `json:"status"`
	ScannedCount      int64  `json:"scanned_count"`
	ScheduledCount    int64  `json:"scheduled_count"`
	CompletedCount    int64  `json:"completed_count"`
	FreshSkipped      int64  `json:"fresh_skipped_count"`
	FailedCount       int64  `json:"failed_count"`
	FailureCode       string `json:"failure_code"`
	DiscoveryComplete bool   `json:"discovery_complete"`
}

func startProcedureInferenceReconciliation(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, checkpoints map[string]procedureInferenceCheckpoint, target map[string]any) (ProcedureInferenceResult, error) {
	configured, auditor, err := procedureInferenceReconciliationContext(resources)
	if err != nil {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-start"}, err
	}
	checkpoint := firstNonEmpty(stringTarget(target, "checkpoint"), "reconciliation")
	idempotencyKey := firstNonEmpty(stringTarget(target, "idempotencyKey"), "mycel-lab-reconciliation-v1")
	batchSize := intTarget(target, "batchSize", 1)
	maxConcurrency := intTarget(target, "maxConcurrency", 1)
	cmd := []string{
		"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", auditor.Username, "--password", auditor.Password, "--output", "json",
		"automation", "reconciliation", "start", "--space-id", configured.SpaceID, "--domain-id", configured.DomainID,
		"--binding-id", configured.BindingID, "--idempotency-key", idempotencyKey,
		"--batch-size", strconv.Itoa(batchSize), "--max-concurrency", strconv.Itoa(maxConcurrency),
	}
	operationCtx, cancel := procedureInferenceTimeout(ctx, target, 30*time.Second)
	defer cancel()
	res, execErr := driver.Exec(operationCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: cmd})
	if execErr != nil {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-start"}, errors.New("start reconciliation command failed")
	}
	var operation procedureInferenceReconciliationOperation
	if err := json.Unmarshal([]byte(res.Stdout), &operation); err != nil || strings.TrimSpace(operation.OperationID) == "" {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-start"}, errors.New("start reconciliation response was incomplete")
	}
	if expectExisting, ok := target["expectExisting"].(bool); ok && expectExisting {
		previous := checkpoints[checkpoint].ReconciliationOperationID
		if previous == "" || previous != operation.OperationID {
			return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-start", ReconciliationOperationID: operation.OperationID}, errors.New("reconciliation idempotency did not return the original operation")
		}
	}
	checkpointState := checkpoints[checkpoint]
	checkpointState.ReconciliationOperationID = operation.OperationID
	checkpoints[checkpoint] = checkpointState
	return procedureInferenceReconciliationResult("procedure-inference-reconciliation-start", operation), nil
}

func awaitProcedureInferenceReconciliation(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, checkpoints map[string]procedureInferenceCheckpoint, target map[string]any) (ProcedureInferenceResult, error) {
	configured, auditor, err := procedureInferenceReconciliationContext(resources)
	if err != nil {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-await-terminal"}, err
	}
	checkpoint := firstNonEmpty(stringTarget(target, "checkpoint"), "reconciliation")
	operationID := checkpoints[checkpoint].ReconciliationOperationID
	if operationID == "" {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-await-terminal"}, errors.New("reconciliation checkpoint is unavailable")
	}
	timeout, err := targetDuration(target, "timeout")
	if err != nil {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-await-terminal", ReconciliationOperationID: operationID}, err
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	interval, err := targetDuration(target, "interval")
	if err != nil {
		return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-await-terminal", ReconciliationOperationID: operationID}, err
	}
	if interval <= 0 {
		interval = time.Second
	}
	requireCompleted := true
	if raw, ok := target["requireCompleted"]; ok {
		value, ok := raw.(bool)
		if !ok {
			return ProcedureInferenceResult{Operation: "procedure-inference-reconciliation-await-terminal", ReconciliationOperationID: operationID}, errors.New("requireCompleted must be boolean")
		}
		requireCompleted = value
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last procedureInferenceReconciliationOperation
	for {
		cmd := []string{
			"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", auditor.Username, "--password", auditor.Password, "--output", "json",
			"automation", "reconciliation", "get", "--space-id", configured.SpaceID, operationID,
		}
		res, execErr := driver.Exec(waitCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: cmd})
		if execErr == nil {
			if err := json.Unmarshal([]byte(res.Stdout), &last); err == nil {
				if isTerminalReconciliationStatus(last.Status) {
					result := procedureInferenceReconciliationResult("procedure-inference-reconciliation-await-terminal", last)
					if err := validateReconciliationProgress(last, target, requireCompleted); err != nil {
						return result, err
					}
					return result, nil
				}
			}
		}
		if waitCtx.Err() != nil {
			return procedureInferenceReconciliationResult("procedure-inference-reconciliation-await-terminal", last), errors.New("reconciliation did not reach a valid terminal state within the bounded timeout")
		}
		if err := sleepContext(waitCtx, interval); err != nil {
			return procedureInferenceReconciliationResult("procedure-inference-reconciliation-await-terminal", last), err
		}
	}
}

func procedureInferenceReconciliationContext(resources *provision.ScenarioResources) (*provision.ProcedureInferenceResources, provision.ActorAssignment, error) {
	if resources == nil || resources.ProcedureInference == nil {
		return nil, provision.ActorAssignment{}, errors.New("procedure inference resources are not provisioned")
	}
	configured := resources.ProcedureInference
	auditor, ok := procedureInferenceAssignment(resources, configured.AuditActorID)
	if !ok || strings.TrimSpace(auditor.Username) == "" || strings.TrimSpace(auditor.Password) == "" {
		return nil, provision.ActorAssignment{}, errors.New("procedure inference audit assignment is incomplete")
	}
	return configured, auditor, nil
}

func procedureInferenceReconciliationResult(operation string, value procedureInferenceReconciliationOperation) ProcedureInferenceResult {
	return ProcedureInferenceResult{
		Operation: operation, ReconciliationOperationID: value.OperationID, ReconciliationStatus: value.Status,
		ReconciliationScanned: value.ScannedCount, ReconciliationScheduled: value.ScheduledCount,
		ReconciliationCompleted: value.CompletedCount, ReconciliationFreshSkipped: value.FreshSkipped,
		ReconciliationFailed: value.FailedCount,
	}
}

func validateReconciliationProgress(operation procedureInferenceReconciliationOperation, target map[string]any, requireCompleted bool) error {
	if requireCompleted && operation.Status != "completed" && operation.Status != "completed_with_failures" {
		return fmt.Errorf("reconciliation reached unexpected terminal status")
	}
	if operation.Status == "failed" || operation.Status == "canceled" {
		return fmt.Errorf("reconciliation reached unexpected terminal status")
	}
	checks := []struct {
		name string
		got  int64
		want int
	}{
		{"scanned", operation.ScannedCount, intTarget(target, "minimumScanned", 0)},
		{"scheduled", operation.ScheduledCount, intTarget(target, "minimumScheduled", 0)},
		{"completed", operation.CompletedCount, intTarget(target, "minimumCompleted", 0)},
		{"fresh skipped", operation.FreshSkipped, intTarget(target, "minimumFreshSkipped", 0)},
	}
	for _, check := range checks {
		if check.got < int64(check.want) {
			return fmt.Errorf("reconciliation %s count below required bound", check.name)
		}
	}
	if operation.FailedCount > int64(intTarget(target, "maximumFailed", 0)) {
		return fmt.Errorf("reconciliation failure count exceeded bound")
	}
	return nil
}

func isTerminalReconciliationStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "completed_with_failures", "canceled", "cancelled", "failed":
		return true
	default:
		return false
	}
}
