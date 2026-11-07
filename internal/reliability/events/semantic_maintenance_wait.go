package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Indexed completion requires both clear background health and current scoped
// freshness. Explicit disabled quiescence instead preserves captured pre-disable
// frontiers, brackets status with disabled-rule reads, and never invokes work.
func awaitScopedSemanticMaintenance(ctx context.Context, admin *mycel.AdminClient, resources *provision.SemanticResources, poll time.Duration, disabled *disabledSemanticQuiescence) (SemanticMaintenanceResult, error) {
	result := SemanticMaintenanceResult{Completion: "indexed"}
	if disabled != nil {
		result.Completion = "disabled-quiescent"
	}
	var previous *SemanticWorkReliabilityEvidenceV1
	readOnly := disabled != nil
	for sequence := 1; sequence <= maxSemanticMaintenanceObservations; sequence++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		observation := SemanticMaintenanceObservation{Sequence: sequence, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), ReadOnly: readOnly}
		if disabled != nil {
			var err error
			observation.DisabledRuleBefore, err = readDisabledSemanticRule(ctx, admin, resources)
			if err != nil {
				result.Observations = append(result.Observations, observation)
				return result, err
			}
		}
		var analyzed *adminv1.AnalyzeSemanticDirtyWorkResponse
		var processed *adminv1.ProcessSemanticDirtyWorkResponse
		if !readOnly {
			callCtx, cancel := admin.AuthCallContext(ctx)
			var err error
			analyzed, err = admin.SemanticMaintenance.AnalyzeSemanticDirtyWork(callCtx, &adminv1.AnalyzeSemanticDirtyWorkRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, Limit: int32(resources.MaintenanceAnalyzeLimit)})
			cancel()
			if err != nil || analyzed == nil {
				return result, maintenanceRPCFailure(ctx, "analyze semantic maintenance", err)
			}
			callCtx, cancel = admin.AuthCallContext(ctx)
			processed, err = admin.SemanticMaintenance.ProcessSemanticDirtyWork(callCtx, &adminv1.ProcessSemanticDirtyWorkRequest{SpaceId: resources.SpaceID, Limit: int32(resources.MaintenanceProcessLimit)})
			cancel()
			if err != nil || processed == nil {
				return result, maintenanceRPCFailure(ctx, "process semantic maintenance", err)
			}
		}
		observation.ProcessedEvents, observation.EnqueuedItems = analyzed.GetProcessedEvents(), analyzed.GetEnqueuedItems()
		observation.ProcessedItems, observation.CompletedItems, observation.FailedItems = processed.GetProcessedItems(), processed.GetCompletedItems(), processed.GetFailedItems()
		callCtx, cancel := admin.AuthCallContext(ctx)
		response, err := admin.SemanticMaintenance.GetSemanticMaintenanceStatus(callCtx, &adminv1.GetSemanticMaintenanceStatusRequest{SpaceId: resources.SpaceID})
		cancel()
		if err != nil {
			response = nil
		}
		observation.NodeHealth = captureMaintenanceHealth(response)
		if err != nil {
			observation.NodeHealth.RPCCode = status.Code(err).String()
		}
		observation.QueueDepthPending, observation.QueueDepthRunning = response.GetQueueDepthPending(), response.GetQueueDepthRunning()
		observation.QueueDepthRetryable, observation.QueueDepthPermanent = response.GetQueueDepthFailedRetryable(), response.GetQueueDepthFailedPermanent()
		observation.Degraded = observation.NodeHealth.Degraded
		if observation.Degraded {
			observation.DegradedReason = observation.NodeHealth.CauseClass
		}
		result.Observations = append(result.Observations, observation)
		current := &result.Observations[len(result.Observations)-1]
		if err != nil || response == nil {
			return result, maintenanceRPCFailure(ctx, "get semantic maintenance status", err)
		}
		callCtx, cancel = admin.AuthCallContext(ctx)
		reliability, rpcErr := admin.SemanticMaintenance.GetSemanticReliabilityStatus(callCtx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey})
		cancel()
		if rpcErr != nil {
			current.Reliability = &SemanticWorkReliabilityEvidenceV1{SchemaVersion: 1, RPCCode: status.Code(rpcErr).String(), RejectionReason: "rpc-failure"}
			return result, maintenanceRPCFailure(ctx, "get semantic maintenance reliability", rpcErr)
		}
		proof := captureSemanticWorkReliability(reliability, resources)
		current.Reliability = proof
		if err := validateSemanticWorkReliability(proof, false); err != nil {
			return result, err
		}
		if previous != nil && (proof.SemanticGeneration != previous.SemanticGeneration || proof.LatestObservedGraphRevision < previous.LatestObservedGraphRevision || proof.AnalyzedThroughGraphRevision < previous.AnalyzedThroughGraphRevision || proof.FreshThroughGraphRevision < previous.FreshThroughGraphRevision) {
			return result, errors.New("semantic maintenance generation or certified frontiers changed/regressed")
		}
		previous = proof
		for _, count := range []int32{current.ProcessedEvents, current.EnqueuedItems, current.ProcessedItems, current.CompletedItems, current.FailedItems, current.QueueDepthPending, current.QueueDepthRunning, current.QueueDepthRetryable, current.QueueDepthPermanent} {
			if count < 0 {
				return result, errors.New("semantic maintenance counters cannot be negative")
			}
		}
		if current.QueueDepthPermanent > 0 {
			return result, errors.New("semantic maintenance has permanently failed work")
		}
		if err := validateMaintenanceHealth(current.NodeHealth); err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		queuesEmpty := current.QueueDepthPending == 0 && current.QueueDepthRunning == 0 && current.QueueDepthRetryable == 0
		scopeComplete := proof.UnresolvedGapCount == 0 && !proof.RebuildRequired && proof.FreshThroughGraphRevision == proof.LatestObservedGraphRevision
		if disabled != nil {
			// Quiescence only; never a freshness claim.
			scopeComplete, err = disabled.validate(proof)
			if err != nil {
				return result, err
			}
			current.DisabledRuleAfter, err = readDisabledSemanticRule(ctx, admin, resources)
			if err != nil {
				return result, err
			}
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if queuesEmpty && scopeComplete && !current.NodeHealth.Degraded {
			result.Converged = true
			result.FreshnessProved = disabled == nil
			return result, nil
		}
		// Once current work is certified, waiting for a periodic background health
		// refresh is read-only. New work/frontier lag re-enables normal maintenance.
		readOnly = disabled != nil || (queuesEmpty && scopeComplete)
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, errors.New("semantic maintenance observation limit reached")
}

func maintenanceRPCFailure(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err == nil {
		return errors.New(operation + " RPC returned no response")
	}
	// gRPC's deadline timer can win just before the parent context timer.
	// Preserve deadline identity even in that ordering, without retrying the RPC.
	if status.Code(err) == codes.DeadlineExceeded {
		return fmt.Errorf("%s RPC deadline exceeded: %w", operation, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s RPC rejected: %s", operation, status.Code(err))
}
