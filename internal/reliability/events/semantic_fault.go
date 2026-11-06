package events

import (
	"context"
	"errors"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SemanticFaultEvidence struct {
	SchemaVersion           int                                `json:"schemaVersion"`
	Disposition             string                             `json:"disposition"`
	ProcessCode             string                             `json:"processCode"`
	Before                  SemanticWorkCheckpointV1           `json:"before"`
	Failed                  SemanticWorkCheckpointV1           `json:"failed"`
	Recovered               SemanticWorkCheckpointV1           `json:"recovered"`
	Discovery               *SemanticWorkEvidenceV1            `json:"discovery,omitempty"`
	BeforeReliability       *SemanticWorkReliabilityEvidenceV1 `json:"beforeReliability,omitempty"`
	FailedReliability       *SemanticWorkReliabilityEvidenceV1 `json:"failedReliability,omitempty"`
	RecoveredReliability    *SemanticWorkReliabilityEvidenceV1 `json:"recoveredReliability,omitempty"`
	FailedFreshThrough      uint64                             `json:"failedFreshThrough"`
	FailedGapCount          uint64                             `json:"failedGapCount"`
	FailureCategory         string                             `json:"failureCategory,omitempty"`
	BackoffHonored          bool                               `json:"backoffHonored"`
	OperatorRetry           bool                               `json:"operatorRetry"`
	OperatorRetryGeneration uint64                             `json:"operatorRetryGeneration,omitempty"`
}

type publicWorkSnapshot struct {
	work        SemanticWorkCheckpointV1
	raw         *adminv1.SemanticMaintenanceWorkItem
	reliability *adminv1.GetSemanticReliabilityStatusResponse
	evidence    *SemanticWorkReliabilityEvidenceV1
}

func readPublicWork(ctx context.Context, client adminv1.AdminSemanticMaintenanceServiceClient, resources *provision.SemanticResources, node string, revision uint64) (publicWorkSnapshot, error) {
	result := publicWorkSnapshot{}
	listing, err := client.ListSemanticMaintenanceWork(ctx, &adminv1.ListSemanticMaintenanceWorkRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, Limit: maxSemanticWorkItems + 1})
	if err != nil || listing == nil || len(listing.GetItems()) > maxSemanticWorkItems {
		return result, errors.New("fault work listing is unavailable or incomplete")
	}
	for _, item := range listing.GetItems() {
		if item == nil || item.GetSpaceId() != resources.SpaceID || item.GetDomainId() != resources.DomainID || item.GetSemanticRuleId() != resources.SemanticRuleID || item.GetEmbeddingBindingKey() != resources.EmbeddingBindingKey {
			return result, errors.New("fault work listing is out of scope")
		}
		if item.GetTargetNodeId() != node {
			continue
		}
		if result.raw != nil {
			return result, errors.New("fault work identity is ambiguous")
		}
		result.raw = item
	}
	if result.raw == nil {
		return result, errSemanticWorkNotReady
	}
	r, err := client.GetSemanticReliabilityStatus(ctx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey})
	if err != nil {
		result.evidence = &SemanticWorkReliabilityEvidenceV1{SchemaVersion: 1, RPCCode: status.Code(err).String(), RejectionReason: "rpc-failure"}
		return result, errors.New("fault reliability RPC rejected: " + status.Code(err).String())
	}
	result.reliability = r
	result.evidence = captureSemanticWorkReliability(r, resources)
	// Expected provider failures may degrade the rule, but never authorize an
	// actual rebuild, unsafe capacity, false freshness, or inconsistent gap flag.
	if err := validateSemanticWorkReliability(result.evidence, true); err != nil {
		return result, err
	}
	if r.GetLatestObservedGraphRevision() < revision || r.GetAnalyzedThroughGraphRevision() < revision {
		return result, errSemanticWorkNotReady
	}
	item := result.raw
	result.work = faultWorkCheckpoint(item, r.GetSemanticGeneration())
	if err := validateSemanticWorkCheckpoint(result.work); err != nil {
		return result, err
	}
	if result.work.LastGraphRevision < revision {
		return result, errSemanticWorkNotReady
	}
	if result.work.LastGraphRevision != revision {
		return result, errors.New("fault work does not bind the acknowledged commit")
	}
	return result, nil
}

// Replicated public diagnostics may lag a successful RPC. Only explicitly
// incomplete read snapshots are polled; capability/auth/assertion errors fail.
func awaitFaultSnapshot(ctx context.Context, poll time.Duration, read func() (publicWorkSnapshot, error), ready func(publicWorkSnapshot) bool) (publicWorkSnapshot, error) {
	var last publicWorkSnapshot
	for n := 0; n < maxSemanticMaintenanceObservations; n++ {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		snapshot, err := read()
		last = snapshot
		if err == nil && ready(snapshot) {
			return snapshot, nil
		}
		if err != nil && !errors.Is(err, errSemanticWorkNotReady) {
			return snapshot, err
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return snapshot, ctx.Err()
		case <-timer.C:
		}
	}
	return last, errors.New("fault snapshot observation limit exceeded")
}

func faultWorkCheckpoint(item *adminv1.SemanticMaintenanceWorkItem, semanticGeneration uint64) SemanticWorkCheckpointV1 {
	return SemanticWorkCheckpointV1{SchemaVersion: 1, WorkItemID: item.GetWorkItemId(), SpaceID: item.GetSpaceId(), DomainID: item.GetDomainId(), SemanticRuleID: item.GetSemanticRuleId(), EmbeddingBindingKey: item.GetEmbeddingBindingKey(), TargetNodeID: item.GetTargetNodeId(), Action: item.GetAction(), Status: item.GetStatus(), AttemptCount: item.GetAttemptCount(), FirstGraphRevision: item.GetFirstGraphRevision(), LastGraphRevision: item.GetLastGraphRevision(), WorkGeneration: item.GetWorkGeneration(), SemanticGeneration: semanticGeneration}
}

func sameFaultIdentity(before, after SemanticWorkCheckpointV1) bool {
	before.Status, after.Status = "pending", "pending"
	before.AttemptCount, after.AttemptCount = 0, 0
	return before == after
}

// A public operator retry requeues the same item through UpsertDirtyWorkItem,
// which advances its fencing generation exactly once. Automatic retryable
// provider backoff never calls this mutation and must keep its generation.
func validateOperatorFaultRetry(failed SemanticWorkCheckpointV1, item *adminv1.SemanticMaintenanceWorkItem) (SemanticWorkCheckpointV1, error) {
	if item == nil || failed.WorkGeneration == ^uint64(0) {
		return SemanticWorkCheckpointV1{}, errors.New("explicit work retry has no bounded generation witness")
	}
	expected := failed
	expected.WorkGeneration++
	retried := faultWorkCheckpoint(item, failed.SemanticGeneration)
	if err := validateSemanticWorkCheckpoint(retried); err != nil || !sameFaultIdentity(expected, retried) || retried.Status != "pending" || retried.AttemptCount != 1 || item.GetLastErrorCategory() != "" || item.GetLastErrorMessageSanitized() != "" || item.GetNotBefore() != "" || item.GetClaimedUntil() != "" {
		return retried, errors.New("explicit work retry changed identity, revision, attempt, or expected generation")
	}
	return retried, nil
}

func validateFaultRecovery(before SemanticWorkCheckpointV1, recovered publicWorkSnapshot, disposition string, retryGeneration, revision uint64) error {
	expected := before
	if disposition == "retryable" {
		if retryGeneration != 0 {
			return errors.New("automatic fault recovery unexpectedly changed work generation")
		}
	} else {
		if before.WorkGeneration == ^uint64(0) || retryGeneration != before.WorkGeneration+1 {
			return errors.New("operator fault recovery lacks the exact retry generation")
		}
		expected.WorkGeneration = retryGeneration
	}
	if recovered.reliability == nil || !sameFaultIdentity(expected, recovered.work) || recovered.work.Status != "complete" || recovered.work.AttemptCount != 2 || recovered.reliability.GetFreshThroughGraphRevision() < revision || recovered.reliability.GetUnresolvedGapCount() != 0 || recovered.reliability.GetDegraded() {
		return errors.New("fault recovery changed identity/generation or replayed work")
	}
	return nil
}

func validateFaultBoundary(before, failed publicWorkSnapshot, disposition string, process *adminv1.ProcessSemanticDirtyWorkResponse, code codes.Code) error {
	if before.raw == nil || failed.raw == nil || failed.reliability == nil {
		return errors.New("fault evidence is incomplete")
	}
	if !sameFaultIdentity(before.work, failed.work) || before.work.Status != "pending" || before.work.AttemptCount != 0 || failed.work.AttemptCount != 1 {
		return errors.New("fault changed work identity/generation/revisions or processing attempts")
	}
	r := failed.reliability
	if r.GetFreshThroughGraphRevision() >= failed.work.FirstGraphRevision || r.GetUnresolvedGapCount() == 0 || r.GetOldestUnresolvedGraphRevision() == 0 || r.GetOldestUnresolvedGraphRevision() > failed.work.LastGraphRevision || r.GetOldestUnresolvedGraphRevision() <= r.GetFreshThroughGraphRevision() {
		return errors.New("failed work falsely advanced freshness or lost its gap")
	}
	switch disposition {
	case "retryable":
		if code != codes.OK || process == nil || process.GetProcessedItems() != 1 || process.GetCompletedItems() != 0 || process.GetFailedItems() != 1 || failed.work.Status != "pending" || failed.raw.GetLastErrorCategory() == "" || failed.raw.GetNotBefore() == "" {
			return errors.New("retryable provider failure did not retain bounded retry state")
		}
	case "permanent":
		if code != codes.OK || process == nil || process.GetProcessedItems() != 1 || process.GetCompletedItems() != 0 || process.GetFailedItems() != 1 || failed.work.Status != "failed" || failed.raw.GetLastErrorCategory() == "" {
			return errors.New("permanent provider failure did not retain terminal work")
		}
	case "deadline":
		if code != codes.DeadlineExceeded || failed.work.Status != "running" && failed.work.Status != "pending" && failed.work.Status != "failed" {
			return errors.New("deadline interruption did not preserve unresolved work")
		}
	case "cancel":
		if code != codes.Canceled || failed.work.Status != "running" && failed.work.Status != "pending" && failed.work.Status != "failed" {
			return errors.New("cancellation did not preserve unresolved work")
		}
	default:
		return errors.New("unknown fault disposition")
	}
	return nil
}

func cancelProcessAtGate(ctx context.Context, poll time.Duration, gate orderingGate, process func(context.Context) (*adminv1.ProcessSemanticDirtyWorkResponse, error)) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	type reply struct {
		response *adminv1.ProcessSemanticDirtyWorkResponse
		err      error
	}
	done := make(chan reply, 1)
	go func() { r, e := process(child); done <- reply{r, e} }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
			}
		}
	}()
	for n := 0; n < maxSemanticMaintenanceObservations; n++ {
		select {
		case <-done:
			joined = true
			return nil, errors.New("process ended before cancellation witness")
		default:
		}
		state, err := gate(ctx, "get")
		if err != nil {
			return nil, err
		}
		if state.State == "waiting" && state.Sequence > 0 {
			cancel()
			select {
			case r := <-done:
				joined = true
				return r.response, r.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if state.State != "idle" {
			return nil, errors.New("invalid cancellation gate")
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, errors.New("cancellation observation bound exceeded")
}

func executeSemanticFault(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (SemanticFaultEvidence, error) {
	result := SemanticFaultEvidence{SchemaVersion: 1, Disposition: targetString(target, "disposition")}
	if err := spec.ValidateSemanticEventTarget("semantic-fault-assert", target); err != nil {
		return result, err
	}
	if resources == nil || resources.Semantic == nil || resources.Semantic.LiveProvider || environment.Metadata["providerEmulator"] != "true" {
		return result, errors.New("fault injection requires a credential-free emulator")
	}
	semantic := resources.Semantic
	timeout := durationTarget(target, "timeout", semantic.MaintenanceTimeout)
	poll := durationTarget(target, "pollInterval", semantic.MaintenancePollInterval)
	if timeout <= 0 || poll <= 0 || poll > timeout {
		return result, errors.New("fault wait must be bounded")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	revision, err := capturedSemanticWorkRevision(semantic, targetString(target, "expectRevisionFrom"))
	if err != nil {
		return result, err
	}
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: resources.AdminAddr, Username: firstNonEmpty(resources.AdminUsername, provision.DefaultAdminUsername), Password: firstNonEmpty(resources.AdminPassword, provision.DefaultAdminPassword), ClientName: "mycel-lab-fault"})
	if err != nil {
		return result, errors.New("fault admin authentication failed")
	}
	defer admin.Close()
	authenticated, stop := admin.AuthCallContext(ctx)
	defer stop()
	client := admin.SemanticMaintenance
	// Initial discovery may wait for durable graph notification delivery; it
	// never processes work. Failed analysis mutations are not resubmitted.
	discovery, err := awaitSemanticWork(authenticated, client, semantic, map[string]any{"nodeId": targetString(target, "nodeId"), "expectRevisionFrom": targetString(target, "expectRevisionFrom"), "expectedStatus": "pending", "expectedAttemptCount": 0, "expectBackgroundMaintenanceEnabled": false, "analyze": true, "timeout": timeout.String(), "pollInterval": poll.String()})
	result.Discovery = &discovery
	if err != nil {
		return result, err
	}
	before, err := readPublicWork(authenticated, client, semantic, targetString(target, "nodeId"), revision)
	result.Before, result.BeforeReliability = before.work, before.evidence
	if err != nil {
		return result, err
	}
	if before.work.Status != "pending" || before.work.AttemptCount != 0 || before.reliability.GetDegraded() || before.reliability.GetUnresolvedGapCount() == 0 || before.reliability.GetFreshThroughGraphRevision() >= before.work.FirstGraphRevision {
		return result, errors.New("fault input is not a durable unprocessed pending boundary")
	}
	process := func(callCtx context.Context) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
		return client.ProcessSemanticDirtyWork(callCtx, &adminv1.ProcessSemanticDirtyWorkRequest{SpaceId: semantic.SpaceID, Limit: 1})
	}
	var processed *adminv1.ProcessSemanticDirtyWorkResponse
	var processErr error
	if result.Disposition == "cancel" {
		controller, ok := driver.(env.ProviderEmulatorController)
		if !ok {
			return result, errors.New("driver lacks cancellation control")
		}
		processed, processErr = cancelProcessAtGate(authenticated, poll, controlledProviderGate(controller, environment, targetString(target, "gate")), process)
	} else if result.Disposition == "deadline" {
		processTimeout := durationTarget(target, "processTimeout", 0)
		if processTimeout <= 0 || processTimeout >= timeout {
			return result, errors.New("process deadline must be shorter than operation timeout")
		}
		child, stop := context.WithTimeout(authenticated, processTimeout)
		processed, processErr = process(child)
		stop()
	} else {
		processed, processErr = process(authenticated)
	}
	code := status.Code(processErr)
	result.ProcessCode = code.String()
	readSnapshot := func() (publicWorkSnapshot, error) {
		return readPublicWork(authenticated, client, semantic, targetString(target, "nodeId"), revision)
	}
	failed, err := awaitFaultSnapshot(ctx, poll, readSnapshot, func(s publicWorkSnapshot) bool {
		if s.work.AttemptCount < 1 {
			return false
		}
		if result.Disposition == "retryable" || result.Disposition == "permanent" {
			return s.work.Status != "running" && !(s.work.Status == "pending" && s.raw.GetLastErrorCategory() == "")
		}
		return true
	})
	result.Failed, result.FailedReliability = failed.work, failed.evidence
	result.FailedFreshThrough, result.FailedGapCount = failed.reliability.GetFreshThroughGraphRevision(), failed.reliability.GetUnresolvedGapCount()
	if err != nil {
		return result, err
	}
	if err := validateFaultBoundary(before, failed, result.Disposition, processed, code); err != nil {
		return result, err
	}
	category := failed.raw.GetLastErrorCategory()
	switch category {
	case "", "rate_limited", "provider_error", "provider_rejected", "authorization_error", "configuration_error", "vector_store_error", "internal_error":
		result.FailureCategory = category
	default:
		return result, errors.New("fault category is unsupported")
	}
	if result.Disposition == "retryable" {
		notBefore, err := time.Parse(time.RFC3339Nano, failed.raw.GetNotBefore())
		if err != nil {
			return result, errors.New("retry deadline is malformed")
		}
		updated, err := time.Parse(time.RFC3339Nano, failed.raw.GetUpdatedAt())
		if err != nil {
			return result, errors.New("retry update time is malformed")
		}
		minimum := durationTarget(target, "minimumBackoff", 0)
		maximum := durationTarget(target, "maximumBackoff", timeout)
		if minimum > maximum || !notBefore.After(updated) || notBefore.Sub(updated) < minimum || notBefore.Sub(updated) > maximum {
			return result, errors.New("provider retry delay is missing or below the declared minimum")
		}
		// A read-only observation is not a retry. An immediate normal processing
		// pass must respect not-before and make no provider request/work attempt.
		check, err := process(authenticated)
		if err != nil || check == nil || check.GetProcessedItems() != 0 || check.GetCompletedItems() != 0 || check.GetFailedItems() != 0 {
			return result, errors.New("normal processing ignored provider backoff")
		}
		result.BackoffHonored = true
	} else {
		retried, err := client.RetrySemanticMaintenanceWork(authenticated, &adminv1.RetrySemanticMaintenanceWorkRequest{SpaceId: semantic.SpaceID, WorkItemId: failed.work.WorkItemID})
		if err != nil || retried == nil {
			return result, errors.New("explicit work retry failed; mutation not resubmitted")
		}
		checkpoint, err := validateOperatorFaultRetry(failed.work, retried.GetItem())
		if err != nil {
			return result, err // Never resubmit an ambiguous or invalid mutation.
		}
		result.OperatorRetry = true
		result.OperatorRetryGeneration = checkpoint.WorkGeneration
	}
	if _, err := awaitSemanticMaintenance(ctx, admin, semantic, map[string]any{"timeout": timeout.String(), "pollInterval": poll.String()}); err != nil {
		return result, errors.New("fault recovery did not converge through normal maintenance")
	}
	recovered, err := awaitFaultSnapshot(ctx, poll, readSnapshot, func(s publicWorkSnapshot) bool { return s.work.AttemptCount >= 2 })
	result.Recovered, result.RecoveredReliability = recovered.work, recovered.evidence
	if err != nil {
		return result, err
	}
	if err := validateFaultRecovery(before.work, recovered, result.Disposition, result.OperatorRetryGeneration, revision); err != nil {
		return result, err
	}
	return result, nil
}
