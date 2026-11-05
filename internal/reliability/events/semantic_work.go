package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxSemanticWorkItems           = 256
	maxSemanticWorkCheckpointBytes = 4096
	semanticWorkCheckpointPrefix   = "semantic-work-v1:"
)

var errSemanticWorkNotReady = errors.New("semantic work has not reached the required state")

// SemanticWorkCheckpointV1 is an immutable capture of public durable work
// identity. It does not infer semantic freshness from timestamps or queue zeros.
type SemanticWorkCheckpointV1 struct {
	SchemaVersion       int    `json:"schemaVersion"`
	WorkItemID          string `json:"workItemId"`
	SpaceID             string `json:"spaceId"`
	DomainID            string `json:"domainId"`
	SemanticRuleID      string `json:"semanticRuleId"`
	EmbeddingBindingKey string `json:"embeddingBindingKey"`
	TargetNodeID        string `json:"targetNodeId"`
	Action              string `json:"action"`
	Status              string `json:"status"`
	AttemptCount        int32  `json:"attemptCount"`
	FirstGraphRevision  uint64 `json:"firstGraphRevision"`
	LastGraphRevision   uint64 `json:"lastGraphRevision"`
	WorkGeneration      uint64 `json:"workGeneration"`
	SemanticGeneration  uint64 `json:"semanticGeneration"`
}

type SemanticWorkEvidenceV1 struct {
	SchemaVersion                 int                                `json:"schemaVersion"`
	Work                          SemanticWorkCheckpointV1           `json:"work"`
	Reliability                   *SemanticWorkReliabilityEvidenceV1 `json:"reliability,omitempty"`
	Observations                  int                                `json:"observations"`
	AnalyzeCalls                  int                                `json:"analyzeCalls"`
	BackgroundMaintenanceEnabled  bool                               `json:"backgroundMaintenanceEnabled"`
	QueueDepthPending             int32                              `json:"queueDepthPending"`
	QueueDepthRunning             int32                              `json:"queueDepthRunning"`
	QueueDepthRetryable           int32                              `json:"queueDepthRetryable"`
	QueueDepthPermanent           int32                              `json:"queueDepthPermanent"`
	LatestObservedGraphRevision   uint64                             `json:"latestObservedGraphRevision"`
	FreshThroughGraphRevision     uint64                             `json:"freshThroughGraphRevision"`
	AnalyzedThroughGraphRevision  uint64                             `json:"analyzedThroughGraphRevision"`
	UnresolvedGapCount            uint64                             `json:"unresolvedGapCount"`
	OldestUnresolvedGraphRevision uint64                             `json:"oldestUnresolvedGraphRevision"`
	CapacityState                 string                             `json:"capacityState"`
	ConfiguredFrontierLimit       uint64                             `json:"configuredFrontierLimit"`
	ConfiguredGapIntervalLimit    uint64                             `json:"configuredGapIntervalLimit"`
	ConfiguredRevisionSpanLimit   uint64                             `json:"configuredRevisionSpanLimit"`
	ConfiguredSerializedByteLimit uint64                             `json:"configuredSerializedByteLimit"`
}

func awaitSemanticWork(ctx context.Context, client adminv1.AdminSemanticMaintenanceServiceClient, resources *provision.SemanticResources, target map[string]any) (SemanticWorkEvidenceV1, error) {
	result := SemanticWorkEvidenceV1{SchemaVersion: 1}
	if resources == nil || client == nil {
		return result, errors.New("semantic work requires provisioned resources and a maintenance client")
	}
	if err := spec.ValidateSemanticEventTarget("semantic-work-assert", target); err != nil {
		return result, err
	}
	if name := targetString(target, "captureWorkAs"); name != "" {
		if _, exists := resources.EvidenceValues[semanticWorkCheckpointPrefix+name]; exists {
			return result, errors.New("semantic work checkpoint must not be overwritten")
		}
	}
	if boolTarget(target, "analyze", false) && (resources.MaintenanceAnalyzeLimit <= 0 || resources.MaintenanceAnalyzeLimit > 4096) {
		return result, errors.New("semantic work analysis limit must be between 1 and 4096")
	}
	timeout := durationTarget(target, "timeout", resources.MaintenanceTimeout)
	interval := durationTarget(target, "pollInterval", resources.MaintenancePollInterval)
	if timeout <= 0 || interval <= 0 || interval > timeout {
		return result, errors.New("semantic work timeout and poll interval must be positive and bounded")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	revision, err := capturedSemanticWorkRevision(resources, targetString(target, "expectRevisionFrom"))
	if err != nil {
		return result, err
	}
	var previous *SemanticWorkCheckpointV1
	if name := targetString(target, "expectWorkFrom"); name != "" {
		checkpoint, err := loadSemanticWorkCheckpoint(resources, name)
		if err != nil {
			return result, err
		}
		previous = &checkpoint
	}
	analyze := boolTarget(target, "analyze", false)
	if analyze && previous != nil {
		return result, errors.New("semantic work preservation checks must not analyze or reconstruct work")
	}
	for result.Observations < maxSemanticMaintenanceObservations {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Observations++
		if analyze {
			result.AnalyzeCalls++
			_, err = client.AnalyzeSemanticDirtyWork(ctx, &adminv1.AnalyzeSemanticDirtyWorkRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, Limit: int32(resources.MaintenanceAnalyzeLimit)})
			if err != nil {
				// A failed analysis response may be ambiguous. Do not resubmit a
				// mutation merely because its transport category is unavailable.
				if ctx.Err() != nil {
					return result, ctx.Err()
				}
				return result, fmt.Errorf("semantic work analysis RPC rejected: %s", status.Code(err))
			}
		}
		if err == nil {
			err = observeSemanticWork(ctx, client, resources, target, revision, previous, &result)
		}
		if err == nil {
			if name := targetString(target, "captureWorkAs"); name != "" {
				if resources.EvidenceValues == nil {
					resources.EvidenceValues = map[string]string{}
				}
				key := semanticWorkCheckpointPrefix + name
				if _, exists := resources.EvidenceValues[key]; exists {
					return result, errors.New("semantic work checkpoint must not be overwritten")
				}
				raw, marshalErr := json.Marshal(result.Work)
				if marshalErr != nil || len(raw) > maxSemanticWorkCheckpointBytes {
					return result, errors.New("semantic work checkpoint exceeds the bounded contract")
				}
				resources.EvidenceValues[key] = string(raw)
			}
			return result, nil
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if !errors.Is(err, errSemanticWorkNotReady) && status.Code(err) != codes.Unavailable {
			// Never retain raw RPC/provider error messages in the evidence.
			if rpcStatus, ok := status.FromError(err); ok {
				return result, fmt.Errorf("semantic work RPC rejected: %s", rpcStatus.Code())
			}
			return result, err
		}
		err = nil
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, errors.New("semantic work observation limit reached")
}

func observeSemanticWork(ctx context.Context, client adminv1.AdminSemanticMaintenanceServiceClient, resources *provision.SemanticResources, target map[string]any, revision uint64, previous *SemanticWorkCheckpointV1, result *SemanticWorkEvidenceV1) error {
	// The extra item is a truncation sentinel. v0.21.4 honors this request
	// limit; a full sentinel-sized response is never accepted as complete proof.
	listing, err := client.ListSemanticMaintenanceWork(ctx, &adminv1.ListSemanticMaintenanceWorkRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, Limit: maxSemanticWorkItems + 1})
	if err != nil {
		return status.Error(status.Code(err), "semantic work listing RPC failed")
	}
	if listing == nil || len(listing.GetItems()) > maxSemanticWorkItems {
		return errors.New("semantic work listing is absent or cannot prove bounded completeness")
	}
	var selected *adminv1.SemanticMaintenanceWorkItem
	for _, item := range listing.GetItems() {
		if item == nil || item.GetSpaceId() != resources.SpaceID || item.GetDomainId() != resources.DomainID || item.GetSemanticRuleId() != resources.SemanticRuleID || item.GetEmbeddingBindingKey() != resources.EmbeddingBindingKey {
			return errors.New("semantic work listing contains missing or out-of-scope evidence")
		}
		if item.GetTargetNodeId() != targetString(target, "nodeId") {
			continue
		}
		if selected != nil {
			return errors.New("semantic work target has ambiguous or duplicate work items")
		}
		selected = item
	}
	if selected == nil {
		return errSemanticWorkNotReady
	}
	work := SemanticWorkCheckpointV1{SchemaVersion: 1, WorkItemID: selected.GetWorkItemId(), SpaceID: selected.GetSpaceId(), DomainID: selected.GetDomainId(), SemanticRuleID: selected.GetSemanticRuleId(), EmbeddingBindingKey: selected.GetEmbeddingBindingKey(), TargetNodeID: selected.GetTargetNodeId(), Action: selected.GetAction(), Status: selected.GetStatus(), AttemptCount: selected.GetAttemptCount(), FirstGraphRevision: selected.GetFirstGraphRevision(), LastGraphRevision: selected.GetLastGraphRevision(), WorkGeneration: selected.GetWorkGeneration()}
	maintenance, err := client.GetSemanticMaintenanceStatus(ctx, &adminv1.GetSemanticMaintenanceStatusRequest{SpaceId: resources.SpaceID})
	if err != nil {
		return status.Error(status.Code(err), "semantic work maintenance-status RPC failed")
	}
	if maintenance == nil || maintenance.GetDegraded() || maintenance.GetQueueDepthFailedPermanent() != 0 {
		return errors.New("semantic work maintenance evidence is absent, degraded, or permanently failed")
	}
	if enabled, ok := target["expectBackgroundMaintenanceEnabled"].(bool); ok && maintenance.GetEnabled() != enabled {
		return errors.New("semantic background-maintenance configuration does not match the fixture")
	}
	result.BackgroundMaintenanceEnabled = maintenance.GetEnabled()
	result.QueueDepthPending, result.QueueDepthRunning = maintenance.GetQueueDepthPending(), maintenance.GetQueueDepthRunning()
	result.QueueDepthRetryable, result.QueueDepthPermanent = maintenance.GetQueueDepthFailedRetryable(), maintenance.GetQueueDepthFailedPermanent()
	if result.QueueDepthPending < 0 || result.QueueDepthRunning < 0 || result.QueueDepthRetryable < 0 || result.QueueDepthPermanent < 0 {
		return errors.New("semantic work queue evidence has negative counts")
	}
	reliability, err := client.GetSemanticReliabilityStatus(ctx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey})
	if err != nil {
		result.Reliability = &SemanticWorkReliabilityEvidenceV1{SchemaVersion: 1, RPCCode: status.Code(err).String(), RejectionReason: "rpc-failure"}
		return status.Error(status.Code(err), "semantic work reliability-status RPC failed")
	}
	// Preserve safe observed values before any rejection, rather than reporting
	// unpopulated zeros as if the daemon returned them. Raw error/state text and
	// foreign scope IDs never enter this evidence.
	proof := captureSemanticWorkReliability(reliability, resources)
	result.Reliability = proof
	result.LatestObservedGraphRevision, result.FreshThroughGraphRevision = proof.LatestObservedGraphRevision, proof.FreshThroughGraphRevision
	result.AnalyzedThroughGraphRevision = proof.AnalyzedThroughGraphRevision
	result.UnresolvedGapCount, result.OldestUnresolvedGraphRevision = proof.UnresolvedGapCount, proof.OldestUnresolvedGraphRevision
	result.CapacityState, result.ConfiguredFrontierLimit = proof.CapacityState, proof.ConfiguredFrontierLimit
	result.ConfiguredGapIntervalLimit, result.ConfiguredRevisionSpanLimit = proof.ConfiguredGapIntervalLimit, proof.ConfiguredRevisionSpanLimit
	result.ConfiguredSerializedByteLimit = proof.ConfiguredSerializedByteLimit
	if err := validateSemanticWorkReliability(proof, false); err != nil {
		return err
	}
	work.SemanticGeneration = proof.SemanticGeneration
	if err := validateSemanticWorkCheckpoint(work); err != nil {
		return err
	}
	if work.LastGraphRevision != revision {
		return errors.New("semantic work revision does not match the acknowledged graph commit")
	}
	if previous != nil {
		if err := compareSemanticWorkCheckpoint(*previous, work); err != nil {
			return err
		}
	}
	result.Work = work
	if result.LatestObservedGraphRevision < revision || result.AnalyzedThroughGraphRevision < revision || result.AnalyzedThroughGraphRevision > result.LatestObservedGraphRevision || result.FreshThroughGraphRevision > result.AnalyzedThroughGraphRevision {
		return errors.New("semantic work reliability frontiers contradict the committed revision")
	}
	expected := targetString(target, "expectedStatus")
	if work.Status != expected {
		if expected == "complete" && (work.Status == "pending" || work.Status == "running") {
			return errSemanticWorkNotReady
		}
		return errors.New("semantic work did not preserve the required pending/completion boundary")
	}
	if expectedAttempts := intTarget(target, "expectedAttemptCount", -1); expectedAttempts >= 0 && work.AttemptCount != int32(expectedAttempts) {
		return errors.New("semantic work attempt count does not match the fixture")
	}
	if expected == "pending" {
		if result.QueueDepthPending == 0 || result.FreshThroughGraphRevision >= work.FirstGraphRevision || result.UnresolvedGapCount == 0 || result.OldestUnresolvedGraphRevision <= result.FreshThroughGraphRevision || result.OldestUnresolvedGraphRevision > revision {
			return errors.New("pending semantic work is missing queue/gap evidence or was falsely reported fresh")
		}
	} else if expected == "complete" {
		if work.AttemptCount < 1 || result.QueueDepthPending != 0 || result.QueueDepthRunning != 0 || result.QueueDepthRetryable != 0 || result.QueueDepthPermanent != 0 || result.FreshThroughGraphRevision < revision || result.UnresolvedGapCount != 0 || result.OldestUnresolvedGraphRevision != 0 {
			return errSemanticWorkNotReady
		}
	} else {
		return errors.New("semantic work expected status must be pending or complete")
	}
	return nil
}

func validateSemanticWorkCheckpoint(work SemanticWorkCheckpointV1) error {
	if work.SchemaVersion != 1 || work.EmbeddingBindingKey == "" || work.Action != "refresh" || work.FirstGraphRevision == 0 || work.FirstGraphRevision > work.LastGraphRevision || work.WorkGeneration == 0 || work.SemanticGeneration == 0 || work.AttemptCount < 0 {
		return errors.New("semantic work is missing versioned durable identity/generation/revision evidence")
	}
	for _, id := range []string{work.WorkItemID, work.SpaceID, work.DomainID, work.SemanticRuleID, work.TargetNodeID} {
		if !spec.IsCanonicalSemanticUUID(id) {
			return errors.New("semantic work contains an invalid durable identifier")
		}
	}
	if work.Status == "complete" && work.AttemptCount == 0 {
		return errors.New("completed semantic work has no processing attempt evidence")
	}
	switch work.Status {
	case "pending", "running", "complete", "failed", "cancelled":
		return nil
	default:
		return errors.New("semantic work contains an unsupported status")
	}
}

func compareSemanticWorkCheckpoint(before, after SemanticWorkCheckpointV1) error {
	if before.WorkItemID != after.WorkItemID || before.SpaceID != after.SpaceID || before.DomainID != after.DomainID || before.SemanticRuleID != after.SemanticRuleID || before.EmbeddingBindingKey != after.EmbeddingBindingKey || before.TargetNodeID != after.TargetNodeID || before.Action != after.Action || before.FirstGraphRevision != after.FirstGraphRevision || before.LastGraphRevision != after.LastGraphRevision || before.WorkGeneration != after.WorkGeneration || before.SemanticGeneration != after.SemanticGeneration {
		return errors.New("semantic work durable identity, generation, or revision range changed across the checkpoint")
	}
	if before.Status == "complete" && after.Status != "complete" {
		return errors.New("completed semantic work regressed across the checkpoint")
	}
	if after.AttemptCount < before.AttemptCount || before.Status == after.Status && before.AttemptCount != after.AttemptCount {
		return errors.New("semantic work attempts reset or replayed across the checkpoint")
	}
	return nil
}

func capturedSemanticWorkRevision(resources *provision.SemanticResources, name string) (uint64, error) {
	revision, err := strconv.ParseUint(resources.EvidenceValues[name], 10, 64)
	if strings.TrimSpace(name) == "" || err != nil || revision == 0 {
		return 0, errors.New("semantic work requires a captured positive committed graph revision")
	}
	return revision, nil
}

func loadSemanticWorkCheckpoint(resources *provision.SemanticResources, name string) (SemanticWorkCheckpointV1, error) {
	raw := resources.EvidenceValues[semanticWorkCheckpointPrefix+name]
	var work SemanticWorkCheckpointV1
	if len(raw) == 0 || len(raw) > maxSemanticWorkCheckpointBytes || json.Unmarshal([]byte(raw), &work) != nil {
		return work, errors.New("semantic work checkpoint is missing, malformed, or oversized")
	}
	canonical, err := json.Marshal(work)
	if err != nil || string(canonical) != raw {
		return work, errors.New("semantic work checkpoint is not canonical versioned evidence")
	}
	return work, validateSemanticWorkCheckpoint(work)
}
