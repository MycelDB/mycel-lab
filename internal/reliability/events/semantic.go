package events

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxSemanticMaintenanceObservations = 1024

type SemanticBackfillResult struct {
	SemanticRuleID      string `json:"semanticRuleId"`
	EmbeddingBindingKey string `json:"embeddingBindingKey"`
	Selected            int32  `json:"selected"`
	Generated           int32  `json:"generated"`
	Skipped             int32  `json:"skipped"`
	Failed              int32  `json:"failed"`
}

type SemanticMaintenanceObservation struct {
	Sequence            int    `json:"sequence"`
	ObservedAt          string `json:"observedAt"`
	ProcessedEvents     int32  `json:"processedEvents"`
	EnqueuedItems       int32  `json:"enqueuedItems"`
	ProcessedItems      int32  `json:"processedItems"`
	CompletedItems      int32  `json:"completedItems"`
	FailedItems         int32  `json:"failedItems"`
	QueueDepthPending   int32  `json:"queueDepthPending"`
	QueueDepthRunning   int32  `json:"queueDepthRunning"`
	QueueDepthRetryable int32  `json:"queueDepthRetryable"`
	QueueDepthPermanent int32  `json:"queueDepthPermanent"`
	Degraded            bool   `json:"degraded"`
	DegradedReason      string `json:"degradedReason,omitempty"`
}

type SemanticMaintenanceResult struct {
	Converged    bool                             `json:"converged"`
	Observations []SemanticMaintenanceObservation `json:"observations"`
}

type SemanticTransactionNegativeResult struct {
	RollbackTransactionID string `json:"rollbackTransactionId"`
	RollbackNodeID        string `json:"rollbackNodeId"`
	FailedTransactionID   string `json:"failedTransactionId"`
	FailedNodeID          string `json:"failedNodeId"`
	FailureCode           string `json:"failureCode"`
	GraphAbsenceProved    bool   `json:"graphAbsenceProved"`
}

type SemanticGraphMutationResult struct {
	Action            string `json:"action"`
	NodeID            string `json:"nodeId"`
	TransactionID     string `json:"transactionId"`
	CommitID          string `json:"commitId"`
	CommittedRevision int64  `json:"committedRevision"`
}

type SemanticSearchEvidence struct {
	Query                      string                     `json:"query"`
	Results                    []SemanticSearchResultItem `json:"results"`
	Warnings                   []string                   `json:"warnings,omitempty"`
	RejectedCode               string                     `json:"rejectedCode,omitempty"`
	FilterExecutionMode        string                     `json:"filterExecutionMode,omitempty"`
	ExaminedSemanticCandidates int32                      `json:"examinedSemanticCandidates,omitempty"`
	EligibleSemanticCandidates int32                      `json:"eligibleSemanticCandidates,omitempty"`
}

type SemanticSearchResultItem struct {
	NodeID                    string  `json:"nodeId"`
	RecordID                  string  `json:"recordId"`
	SemanticRuleID            string  `json:"semanticRuleId"`
	EmbeddingBindingKey       string  `json:"embeddingBindingKey"`
	Score                     float64 `json:"score"`
	SourceGraphRevision       uint64  `json:"sourceGraphRevision"`
	SemanticGeneration        uint64  `json:"semanticGeneration"`
	SemanticConfigurationHash string  `json:"semanticConfigurationHash"`
	OpaqueSourceVersion       string  `json:"opaqueSourceVersion"`
	Dimensions                int32   `json:"dimensions"`
	VectorSpaceKey            string  `json:"vectorSpaceKey"`
	ConfiguredProfileID       string  `json:"configuredProfileId"`
	ConfiguredModelEndpointID string  `json:"configuredModelEndpointId"`
	ConfiguredModelID         string  `json:"configuredModelId"`
	ConfiguredCapabilityID    string  `json:"configuredCapabilityId"`
	ActualRouteKnown          bool    `json:"actualRouteKnown"`
	ActualModelEndpointID     string  `json:"actualModelEndpointId,omitempty"`
	ActualModelID             string  `json:"actualModelId,omitempty"`
	ActualCapabilityID        string  `json:"actualCapabilityId,omitempty"`
	RebuildEpoch              string  `json:"rebuildEpoch,omitempty"`
	GeneratedAt               string  `json:"generatedAt"`
}

type SemanticReliabilityEvidence struct {
	LatestObservedGraphRevision   uint64 `json:"latestObservedGraphRevision"`
	AnalyzedThroughGraphRevision  uint64 `json:"analyzedThroughGraphRevision"`
	FreshThroughGraphRevision     uint64 `json:"freshThroughGraphRevision"`
	UnresolvedGapCount            uint64 `json:"unresolvedGapCount"`
	OldestUnresolvedGraphRevision uint64 `json:"oldestUnresolvedGraphRevision"`
	ConfiguredFrontierLimit       uint64 `json:"configuredFrontierLimit"`
	ConfiguredGapIntervalLimit    uint64 `json:"configuredGapIntervalLimit"`
	ConfiguredRevisionSpanLimit   uint64 `json:"configuredRevisionSpanLimit"`
	ConfiguredSerializedByteLimit uint64 `json:"configuredSerializedByteLimit"`
	RetainedGapIntervalCount      uint64 `json:"retainedGapIntervalCount"`
	RetainedRevisionSpan          uint64 `json:"retainedRevisionSpan"`
	RetainedSerializedBytes       uint64 `json:"retainedSerializedBytes"`
	RetainedWorkItemCount         uint64 `json:"retainedWorkItemCount"`
	SemanticGeneration            uint64 `json:"semanticGeneration"`
	CapacityState                 string `json:"capacityState"`
	Degraded                      bool   `json:"degraded"`
	DegradedReason                string `json:"degradedReason,omitempty"`
	RebuildRequired               bool   `json:"rebuildRequired"`
	RebuildEpoch                  string `json:"rebuildEpoch,omitempty"`
	RebuildState                  string `json:"rebuildState,omitempty"`
	PinnedTargetGraphRevision     uint64 `json:"pinnedTargetGraphRevision,omitempty"`
	DeltaCatchupGraphRevision     uint64 `json:"deltaCatchupGraphRevision,omitempty"`
}

func executeSemanticEvent(ctx context.Context, resources *provision.ScenarioResources, eventType string, target map[string]any) (any, error) {
	if resources == nil || resources.Semantic == nil {
		return nil, errors.New("semantic resources are not provisioned")
	}
	semanticResources := resources.Semantic
	if semanticResources.SpaceID == "" || semanticResources.DomainID == "" || semanticResources.EmbeddingBindingKey == "" {
		return nil, errors.New("semantic resources are incomplete")
	}
	needsRule := eventType != "semantic-node-create" && eventType != "semantic-node-update" && eventType != "semantic-node-delete" && eventType != "semantic-transaction-negative" && eventType != "semantic-rule-create"
	if needsRule && semanticResources.SemanticRuleID == "" {
		return nil, errors.New("semantic rule is not provisioned")
	}
	addr := strings.TrimSpace(resources.AdminAddr)
	if addr == "" {
		return nil, errors.New("semantic event requires an admin daemon address")
	}
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: addr, Username: provision.DefaultAdminUsername, Password: provision.DefaultAdminPassword, ClientName: "mycel-lab-semantic"})
	if err != nil {
		return nil, fmt.Errorf("dial semantic admin: %w", err)
	}
	defer admin.Close()
	switch eventType {
	case "semantic-rule-create":
		if err := provision.CreateSemanticRule(ctx, admin, resources); err != nil {
			return nil, fmt.Errorf("create semantic rule: %w", err)
		}
		return map[string]any{"semanticRuleId": resources.Semantic.SemanticRuleID, "key": resources.Semantic.SemanticRuleKey}, nil
	case "semantic-node-create", "semantic-node-update", "semantic-node-delete":
		return executeSemanticGraphMutation(ctx, resources, eventType, target)
	case "semantic-transaction-negative":
		return executeSemanticTransactionNegative(ctx, resources, target)
	case "semantic-search-assert":
		return executeSemanticSearchAssertion(ctx, resources, target)
	case "semantic-unauthorized-search":
		return executeSemanticUnauthorizedSearch(ctx, resources, target)
	case "semantic-reliability-assert":
		evidence, err := assertSemanticReliability(ctx, admin, semanticResources, target)
		if err == nil {
			captureSemanticReliability(resources, target, evidence)
		}
		return evidence, err
	case "semantic-rebuild":
		rebuildCtx, stopRebuild := context.WithCancel(ctx)
		var mutationDone <-chan error
		if mutationTarget, ok := nestedTarget(target, "concurrentMutation"); ok {
			done := make(chan error, 1)
			mutationDone = done
			go func() {
				done <- mutateDuringSemanticRebuild(rebuildCtx, admin, resources, semanticResources, mutationTarget)
			}()
		}
		callCtx, cancel := admin.AuthCallContext(rebuildCtx)
		response, err := admin.SemanticMaintenance.RebuildSemanticRule(callCtx, &adminv1.RebuildSemanticRuleRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID, EmbeddingBindingKey: semanticResources.EmbeddingBindingKey, BackfillLimit: int32(intTarget(target, "backfillLimit", 4096)), MaintenanceBatchLimit: int32(intTarget(target, "maintenanceBatchLimit", 256)), MaxCatchupIterations: int32(intTarget(target, "maxCatchupIterations", 100))})
		cancel()
		if err != nil {
			stopRebuild()
			return nil, fmt.Errorf("rebuild semantic rule: %w", err)
		}
		if mutationDone != nil {
			select {
			case mutationErr := <-mutationDone:
				if mutationErr != nil {
					stopRebuild()
					return nil, mutationErr
				}
			case <-rebuildCtx.Done():
				stopRebuild()
				return nil, rebuildCtx.Err()
			}
		}
		stopRebuild()
		if response.GetReliability() == nil || response.GetRebuild() == nil || response.GetRebuild().GetState() != "completed" {
			return nil, errors.New("semantic rebuild did not return completed reliability evidence")
		}
		evidence := mapSemanticReliabilityEvidence(response.GetReliability())
		captureSemanticReliability(resources, target, evidence)
		return evidence, nil
	case "semantic-rule-projection-update":
		callCtx, cancel := admin.AuthCallContext(ctx)
		current, err := admin.Semantic.GetSemanticRule(callCtx, &adminv1.GetSemanticRuleRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("get semantic rule for projection update: %w", err)
		}
		rule := current.GetRule()
		if rule == nil || rule.GetSource() == nil {
			return nil, errors.New("semantic rule projection is unavailable")
		}
		rule.Source.IncludeProperties = stringSliceTarget(target, "includeProperties")
		callCtx, cancel = admin.AuthCallContext(ctx)
		updated, err := admin.Semantic.UpdateSemanticRule(callCtx, &adminv1.UpdateSemanticRuleRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID, Rule: rule})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("update semantic rule projection: %w", err)
		}
		return map[string]any{"semanticRuleId": semanticResources.SemanticRuleID, "includeProperties": updated.GetRule().GetSource().GetIncludeProperties()}, nil
	case "semantic-rule-enable":
		enabled := boolTarget(target, "enabled", true)
		callCtx, cancel := admin.AuthCallContext(ctx)
		response, err := admin.Semantic.SetSemanticRuleEnabled(callCtx, &adminv1.SetSemanticRuleEnabledRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID, Enabled: enabled})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("set semantic rule enabled: %w", err)
		}
		if response.GetRule() == nil || response.GetRule().GetEnabled() != enabled {
			return nil, errors.New("semantic rule enable response does not match the requested state")
		}
		return map[string]any{"semanticRuleId": response.GetRule().GetSemanticRuleId(), "enabled": enabled}, nil
	case "semantic-backfill":
		limit := intTarget(target, "limit", 4096)
		if limit <= 0 || limit > 4096 {
			return nil, errors.New("semantic-backfill limit must be between 1 and 4096")
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		response, err := admin.SemanticMaintenance.BackfillSemanticRule(callCtx, &adminv1.BackfillSemanticRuleRequest{
			SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID,
			EmbeddingBindingKey: semanticResources.EmbeddingBindingKey, NodeIds: stringSliceTarget(target, "nodeIds"),
			Force: boolTarget(target, "force", false), Limit: int32(limit), ContinueOnError: false,
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("semantic backfill: %w", err)
		}
		result := SemanticBackfillResult{SemanticRuleID: response.GetSemanticRuleId(), EmbeddingBindingKey: response.GetEmbeddingBindingKey(), Selected: response.GetSelectedCount(), Generated: response.GetGeneratedCount(), Skipped: response.GetSkippedCount(), Failed: response.GetFailedCount()}
		if result.Failed != 0 {
			return result, fmt.Errorf("semantic backfill reported %d failures", result.Failed)
		}
		if expected := intTarget(target, "expectedSelected", -1); expected >= 0 && result.Selected != int32(expected) {
			return result, fmt.Errorf("semantic backfill selected %d nodes, want %d", result.Selected, expected)
		}
		if minimum := intTarget(target, "minimumGenerated", 0); result.Generated < int32(minimum) {
			return result, fmt.Errorf("semantic backfill generated %d records, want at least %d", result.Generated, minimum)
		}
		return result, nil
	case "semantic-maintenance-await":
		return awaitSemanticMaintenance(ctx, admin, semanticResources, target)
	default:
		return nil, fmt.Errorf("unsupported semantic event %q", eventType)
	}
}

func mutateDuringSemanticRebuild(ctx context.Context, admin *mycel.AdminClient, resources *provision.ScenarioResources, semanticResources *provision.SemanticResources, target map[string]any) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for observations := 0; observations < 10000; observations++ {
		callCtx, cancel := admin.AuthCallContext(ctx)
		statusResponse, err := admin.SemanticMaintenance.GetSemanticReliabilityStatus(callCtx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID, EmbeddingBindingKey: semanticResources.EmbeddingBindingKey})
		cancel()
		if err != nil {
			return fmt.Errorf("observe running semantic rebuild: %w", err)
		}
		state := statusResponse.GetRebuild().GetState()
		if state == "running" {
			eventType := targetString(target, "eventType")
			if eventType == "" {
				eventType = "semantic-node-create"
			}
			_, err := executeSemanticGraphMutation(ctx, resources, eventType, target)
			if err != nil {
				return fmt.Errorf("mutate during semantic rebuild: %w", err)
			}
			return nil
		}
		if state == "completed" || state == "failed" {
			return fmt.Errorf("semantic rebuild reached %s before the concurrent mutation", state)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return errors.New("semantic rebuild did not expose a bounded running interval for delta mutation")
}

func nestedTarget(target map[string]any, key string) (map[string]any, bool) {
	value, ok := target[key]
	if !ok || value == nil {
		return nil, false
	}
	out, ok := value.(map[string]any)
	return out, ok
}

func assertSemanticReliability(ctx context.Context, admin *mycel.AdminClient, resources *provision.SemanticResources, target map[string]any) (SemanticReliabilityEvidence, error) {
	callCtx, cancel := admin.AuthCallContext(ctx)
	response, err := admin.SemanticMaintenance.GetSemanticReliabilityStatus(callCtx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey})
	cancel()
	if err != nil {
		return SemanticReliabilityEvidence{}, fmt.Errorf("get semantic reliability status: %w", err)
	}
	evidence := mapSemanticReliabilityEvidence(response)
	if evidence.ConfiguredFrontierLimit == 0 || evidence.ConfiguredGapIntervalLimit == 0 || evidence.ConfiguredRevisionSpanLimit == 0 || evidence.ConfiguredSerializedByteLimit == 0 || evidence.CapacityState == "" {
		return evidence, errors.New("semantic reliability status is missing bounded capacity evidence")
	}
	expectRebuild := boolTarget(target, "expectRebuildRequired", false)
	if expectRebuild {
		if !evidence.RebuildRequired || evidence.RebuildState != "required" {
			return evidence, errors.New("semantic reliability status did not require an explicit rebuild")
		}
		return evidence, nil
	}
	if evidence.Degraded {
		return evidence, fmt.Errorf("semantic reliability status is degraded: %s", evidence.DegradedReason)
	}
	if maximum := intTarget(target, "maximumUnresolvedGapCount", 0); evidence.UnresolvedGapCount > uint64(maximum) {
		return evidence, fmt.Errorf("semantic unresolved gap count %d exceeds %d", evidence.UnresolvedGapCount, maximum)
	}
	if boolTarget(target, "requireFreshAtLatest", true) && evidence.FreshThroughGraphRevision < evidence.LatestObservedGraphRevision {
		return evidence, fmt.Errorf("semantic fresh-through revision %d is behind latest observed revision %d", evidence.FreshThroughGraphRevision, evidence.LatestObservedGraphRevision)
	}
	if evidence.FreshThroughGraphRevision > evidence.LatestObservedGraphRevision || evidence.AnalyzedThroughGraphRevision > evidence.LatestObservedGraphRevision {
		return evidence, errors.New("semantic reliability frontier exceeds the latest observed graph revision")
	}
	if minimum := intTarget(target, "minimumSemanticGeneration", 0); evidence.SemanticGeneration < uint64(minimum) {
		return evidence, fmt.Errorf("semantic generation %d is below %d", evidence.SemanticGeneration, minimum)
	}
	if err := requireCapturedRevision(resources, target, "expectFreshRevisionFrom", evidence.FreshThroughGraphRevision); err != nil {
		return evidence, err
	}
	if err := requireCapturedRevision(resources, target, "expectDeltaCatchupRevisionFrom", evidence.DeltaCatchupGraphRevision); err != nil {
		return evidence, err
	}
	if boolTarget(target, "requireDeltaAfterPinned", false) && evidence.DeltaCatchupGraphRevision <= evidence.PinnedTargetGraphRevision {
		return evidence, fmt.Errorf("semantic delta catch-up revision %d did not advance past pinned revision %d", evidence.DeltaCatchupGraphRevision, evidence.PinnedTargetGraphRevision)
	}
	return evidence, nil
}

func requireCapturedRevision(resources *provision.SemanticResources, target map[string]any, field string, actual uint64) error {
	key := targetString(target, field)
	if key == "" {
		return nil
	}
	raw, ok := resources.EvidenceValues[key]
	if !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("semantic revision capture %q is missing", key)
	}
	expected, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("semantic revision capture %q is malformed", key)
	}
	if actual != expected {
		return fmt.Errorf("semantic revision %d does not match capture %q", actual, key)
	}
	return nil
}

func mapSemanticReliabilityEvidence(response *adminv1.GetSemanticReliabilityStatusResponse) SemanticReliabilityEvidence {
	evidence := SemanticReliabilityEvidence{LatestObservedGraphRevision: response.GetLatestObservedGraphRevision(), AnalyzedThroughGraphRevision: response.GetAnalyzedThroughGraphRevision(), FreshThroughGraphRevision: response.GetFreshThroughGraphRevision(), UnresolvedGapCount: response.GetUnresolvedGapCount(), OldestUnresolvedGraphRevision: response.GetOldestUnresolvedGraphRevision(), ConfiguredFrontierLimit: response.GetConfiguredFrontierLimit(), ConfiguredGapIntervalLimit: response.GetConfiguredGapIntervalLimit(), ConfiguredRevisionSpanLimit: response.GetConfiguredRevisionSpanLimit(), ConfiguredSerializedByteLimit: response.GetConfiguredSerializedByteLimit(), RetainedGapIntervalCount: response.GetRetainedGapIntervalCount(), RetainedRevisionSpan: response.GetRetainedRevisionSpan(), RetainedSerializedBytes: response.GetRetainedSerializedBytes(), RetainedWorkItemCount: response.GetRetainedWorkItemCount(), SemanticGeneration: response.GetSemanticGeneration(), CapacityState: response.GetCapacityState(), Degraded: response.GetDegraded(), DegradedReason: response.GetDegradedReason(), RebuildRequired: response.GetRebuildRequired()}
	if rebuild := response.GetRebuild(); rebuild != nil {
		evidence.RebuildEpoch = rebuild.GetRebuildEpoch()
		evidence.RebuildState = rebuild.GetState()
		evidence.PinnedTargetGraphRevision = rebuild.GetPinnedTargetGraphRevision()
		evidence.DeltaCatchupGraphRevision = rebuild.GetDeltaCatchupGraphRevision()
	}
	return evidence
}

func captureSemanticReliability(resources *provision.ScenarioResources, target map[string]any, evidence SemanticReliabilityEvidence) {
	if resources == nil || resources.Semantic == nil {
		return
	}
	if key := targetString(target, "captureGenerationAs"); key != "" {
		resources.Semantic.EvidenceValues[key] = strconv.FormatUint(evidence.SemanticGeneration, 10)
	}
	if key := targetString(target, "captureRebuildEpochAs"); key != "" {
		resources.Semantic.EvidenceValues[key] = evidence.RebuildEpoch
	}
	if key := targetString(target, "captureFreshRevisionAs"); key != "" {
		resources.Semantic.EvidenceValues[key] = strconv.FormatUint(evidence.FreshThroughGraphRevision, 10)
	}
	if key := targetString(target, "capturePinnedRevisionAs"); key != "" {
		resources.Semantic.EvidenceValues[key] = strconv.FormatUint(evidence.PinnedTargetGraphRevision, 10)
	}
	if key := targetString(target, "captureDeltaCatchupRevisionAs"); key != "" {
		resources.Semantic.EvidenceValues[key] = strconv.FormatUint(evidence.DeltaCatchupGraphRevision, 10)
	}
}

func executeSemanticTransactionNegative(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (SemanticTransactionNegativeResult, error) {
	client, assignment, err := dialSemanticActor(ctx, resources)
	if err != nil {
		return SemanticTransactionNegativeResult{}, err
	}
	defer client.Close()
	rollbackNodeID := strings.TrimSpace(fmt.Sprint(target["rollbackNodeId"]))
	failedNodeID := strings.TrimSpace(fmt.Sprint(target["failedNodeId"]))
	if rollbackNodeID == "" || rollbackNodeID == "<nil>" || failedNodeID == "" || failedNodeID == "<nil>" || rollbackNodeID == failedNodeID {
		return SemanticTransactionNegativeResult{}, errors.New("semantic transaction negative event requires distinct rollbackNodeId and failedNodeId")
	}
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return SemanticTransactionNegativeResult{}, fmt.Errorf("open semantic negative transaction session: %w", err)
	}
	defer client.CloseSession(context.WithoutCancel(ctx), sessionID)
	payload, _ := structpb.NewStruct(map[string]any{"text": "axis-6"})
	rollbackTx, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return SemanticTransactionNegativeResult{}, err
	}
	if _, err := client.CreateNode(ctx, rollbackTx, &clientv1.NodeCreate{NodeId: proto.String(rollbackNodeID), Labels: []string{"Document"}, Payload: payload}); err != nil {
		return SemanticTransactionNegativeResult{}, fmt.Errorf("create rollback fixture: %w", err)
	}
	callCtx, cancel := client.AuthCallContext(ctx)
	_, err = client.Transaction.RollbackTransaction(callCtx, &clientv1.RollbackTransactionRequest{TransactionId: rollbackTx})
	cancel()
	if err != nil {
		return SemanticTransactionNegativeResult{}, fmt.Errorf("rollback semantic fixture: %w", err)
	}

	failedTx, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return SemanticTransactionNegativeResult{}, err
	}
	if _, err := client.CreateNode(ctx, failedTx, &clientv1.NodeCreate{NodeId: proto.String(failedNodeID), Labels: []string{"Document"}, Payload: payload}); err != nil {
		return SemanticTransactionNegativeResult{}, fmt.Errorf("create failed-statement fixture: %w", err)
	}
	_, statementErr := client.CreateNode(ctx, failedTx, &clientv1.NodeCreate{NodeId: proto.String(failedNodeID), Labels: []string{"Document"}, Payload: payload})
	if statementErr == nil {
		return SemanticTransactionNegativeResult{}, errors.New("duplicate-node failed statement unexpectedly succeeded")
	}
	callCtx, cancel = client.AuthCallContext(ctx)
	_, rollbackErr := client.Transaction.RollbackTransaction(callCtx, &clientv1.RollbackTransactionRequest{TransactionId: failedTx})
	cancel()
	if rollbackErr != nil {
		return SemanticTransactionNegativeResult{}, fmt.Errorf("rollback failed-statement transaction: %w", rollbackErr)
	}
	for _, nodeID := range []string{rollbackNodeID, failedNodeID} {
		readTx, beginErr := client.BeginReadOnlyTransaction(ctx, sessionID)
		if beginErr != nil {
			return SemanticTransactionNegativeResult{}, beginErr
		}
		_, getErr := client.GetNode(ctx, readTx, nodeID)
		callCtx, cancel = client.AuthCallContext(ctx)
		_, _ = client.Transaction.RollbackTransaction(callCtx, &clientv1.RollbackTransactionRequest{TransactionId: readTx})
		cancel()
		if status.Code(getErr) != codes.NotFound {
			return SemanticTransactionNegativeResult{}, fmt.Errorf("negative transaction node %q remained visible: %v", nodeID, getErr)
		}
	}
	return SemanticTransactionNegativeResult{RollbackTransactionID: rollbackTx, RollbackNodeID: rollbackNodeID, FailedTransactionID: failedTx, FailedNodeID: failedNodeID, FailureCode: status.Code(statementErr).String(), GraphAbsenceProved: true}, nil
}

func executeSemanticGraphMutation(ctx context.Context, resources *provision.ScenarioResources, eventType string, target map[string]any) (SemanticGraphMutationResult, error) {
	actorGroup := strings.TrimSpace(fmt.Sprint(target["actorGroup"]))
	if actorGroup == "<nil>" {
		actorGroup = ""
	}
	fallbackActorID := ""
	if resources != nil && resources.Semantic != nil {
		fallbackActorID = resources.Semantic.TargetActorID
	}
	client, assignment, err := dialGraphActorGroup(ctx, resources, actorGroup, fallbackActorID)
	if err != nil {
		return SemanticGraphMutationResult{}, err
	}
	defer client.Close()
	nodeID := strings.TrimSpace(fmt.Sprint(target["nodeId"]))
	if nodeID == "" || nodeID == "<nil>" {
		return SemanticGraphMutationResult{}, errors.New("semantic graph mutation requires nodeId")
	}
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return SemanticGraphMutationResult{}, fmt.Errorf("open semantic graph session: %w", err)
	}
	defer client.CloseSession(context.WithoutCancel(ctx), sessionID)
	txID, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return SemanticGraphMutationResult{}, fmt.Errorf("begin semantic graph transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			callCtx, cancel := client.AuthCallContext(context.WithoutCancel(ctx))
			_, _ = client.Transaction.RollbackTransaction(callCtx, &clientv1.RollbackTransactionRequest{TransactionId: txID})
			cancel()
		}
	}()
	switch eventType {
	case "semantic-node-create":
		payload := map[string]any{"text": strings.TrimSpace(fmt.Sprint(target["text"]))}
		if properties, ok := target["properties"].(map[string]any); ok {
			for key, value := range properties {
				payload[key] = value
			}
		}
		encoded, encodeErr := structpb.NewStruct(payload)
		if encodeErr != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("encode semantic node payload: %w", encodeErr)
		}
		if _, err = client.CreateNode(ctx, txID, &clientv1.NodeCreate{NodeId: proto.String(nodeID), Labels: stringSliceTarget(target, "labels"), Payload: encoded}); err != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("create semantic node: %w", err)
		}
	case "semantic-node-update":
		current, readErr := client.GetNode(ctx, txID, nodeID)
		if readErr != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("read semantic node for update: %w", readErr)
		}
		payload := map[string]any{}
		if current.GetPayload() != nil {
			payload = current.GetPayload().AsMap()
		}
		if rawText, ok := target["text"]; ok {
			payload["text"] = strings.TrimSpace(fmt.Sprint(rawText))
		}
		if properties, ok := target["properties"].(map[string]any); ok {
			for key, value := range properties {
				payload[key] = value
			}
		}
		encoded, encodeErr := structpb.NewStruct(payload)
		if encodeErr != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("encode semantic node update: %w", encodeErr)
		}
		callCtx, cancel := client.AuthCallContext(ctx)
		_, err = client.Graph.UpdateNode(callCtx, &clientv1.UpdateNodeRequest{TransactionId: txID, Node: &clientv1.Node{NodeId: nodeID, Payload: encoded}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"payload"}}})
		cancel()
		if err != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("update semantic node: %w", err)
		}
	case "semantic-node-delete":
		if err = client.DeleteNode(ctx, txID, nodeID, false); err != nil {
			return SemanticGraphMutationResult{}, fmt.Errorf("delete semantic node: %w", err)
		}
	}
	commit, err := client.CommitTransactionResult(ctx, txID)
	if err != nil {
		return SemanticGraphMutationResult{}, fmt.Errorf("commit semantic graph mutation: %w", err)
	}
	committed = true
	result := SemanticGraphMutationResult{Action: eventType, NodeID: nodeID, TransactionID: txID, CommitID: commit.GetCommitId(), CommittedRevision: commit.GetCommittedRevision()}
	if capture := targetString(target, "captureRevisionAs"); capture != "" {
		if resources.Semantic == nil {
			return SemanticGraphMutationResult{}, errors.New("semantic revision capture requires semantic resources")
		}
		resources.Semantic.EvidenceValues[capture] = strconv.FormatInt(result.CommittedRevision, 10)
	}
	return result, nil
}

func executeSemanticUnauthorizedSearch(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (map[string]any, error) {
	groupName := strings.TrimSpace(fmt.Sprint(target["actorGroup"]))
	if groupName == "" || groupName == "<nil>" {
		return nil, errors.New("semantic unauthorized search requires actorGroup")
	}
	var outsider provision.ActorAssignment
	for _, assignment := range resources.Assignments {
		if assignment.GroupName == groupName {
			outsider = assignment
			break
		}
	}
	if outsider.ActorID == "" || outsider.AccessToken == "" {
		return nil, fmt.Errorf("semantic unauthorized actor group %q has no authenticated assignment", groupName)
	}
	client, err := mycel.Dial(ctx, mycel.Config{Addr: outsider.DaemonAddr, AccessToken: outsider.AccessToken, RefreshToken: outsider.RefreshToken, ClientName: "mycel-lab-semantic-negative"})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	callCtx, cancel := client.AuthCallContext(ctx)
	_, searchErr := client.Semantic.SemanticSearch(callCtx, &clientv1.SemanticSearchRequest{SpaceId: resources.Semantic.SpaceID, DomainId: resources.Semantic.DomainID, SemanticRuleId: proto.String(resources.Semantic.SemanticRuleID), EmbeddingBindingKey: proto.String(resources.Semantic.EmbeddingBindingKey), Query: "axis-0", Limit: 1})
	cancel()
	code := status.Code(searchErr)
	if code != codes.PermissionDenied && code != codes.NotFound {
		return nil, fmt.Errorf("unauthorized semantic search returned %s, want PermissionDenied or NotFound", code)
	}
	return map[string]any{"actorGroup": groupName, "rejectedCode": code.String(), "targetSpaceId": resources.Semantic.SpaceID, "targetDomainId": resources.Semantic.DomainID}, nil
}

func executeSemanticSearchAssertion(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (SemanticSearchEvidence, error) {
	client, assignment, err := dialSemanticActor(ctx, resources)
	if err != nil {
		return SemanticSearchEvidence{}, err
	}
	defer client.Close()
	query := strings.TrimSpace(fmt.Sprint(target["query"]))
	if query == "" || query == "<nil>" {
		return SemanticSearchEvidence{}, errors.New("semantic search assertion requires query")
	}
	limit := intTarget(target, "limit", 10)
	if limit <= 0 || limit > 1000 {
		return SemanticSearchEvidence{}, errors.New("semantic search assertion limit must be between 1 and 1000")
	}
	request := &clientv1.SemanticSearchRequest{
		SpaceId: assignment.SpaceID, DomainId: assignment.DomainID, Query: query,
		SemanticRuleId: proto.String(resources.Semantic.SemanticRuleID), EmbeddingBindingKey: proto.String(resources.Semantic.EmbeddingBindingKey), Limit: int32(limit),
	}
	if minimum, ok := floatTarget(target, "minimumScore"); ok {
		request.MinScore = proto.Float64(minimum)
	}
	filter := &clientv1.SemanticEligibilityFilter{NodeLabels: stringSliceTarget(target, "filterNodeLabels"), NodeIds: stringSliceTarget(target, "filterNodeIds")}
	if properties, ok := target["filterProperties"].(map[string]any); ok {
		for key, value := range properties {
			encoded, encodeErr := structpb.NewValue(value)
			if encodeErr != nil {
				return SemanticSearchEvidence{}, fmt.Errorf("encode semantic filter property %q: %w", key, encodeErr)
			}
			filter.Properties = append(filter.Properties, &clientv1.SemanticPropertyPredicate{Key: key, Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EQUAL, Value: encoded})
		}
	}
	if len(filter.NodeLabels) > 0 || len(filter.NodeIds) > 0 || len(filter.Properties) > 0 {
		request.Filter = filter
	}
	var searchResults []*clientv1.SemanticSearchResult
	var warnings []string
	filterMode := ""
	var examinedCandidates, eligibleCandidates int32
	seenRecords := map[string]bool{}
	for page := 0; ; page++ {
		if page >= 1000 {
			return SemanticSearchEvidence{}, errors.New("semantic search page limit reached")
		}
		callCtx, cancel := client.AuthCallContext(ctx)
		response, callErr := client.Semantic.SemanticSearch(callCtx, request)
		cancel()
		if callErr != nil {
			expectedCode := strings.TrimSpace(fmt.Sprint(target["expectFailureCode"]))
			actualCode := status.Code(callErr).String()
			if expectedCode != "" && expectedCode != "<nil>" && strings.EqualFold(expectedCode, actualCode) {
				return SemanticSearchEvidence{Query: query, RejectedCode: actualCode}, nil
			}
			return SemanticSearchEvidence{}, fmt.Errorf("direct semantic search: %w", callErr)
		}
		warnings = append(warnings, response.GetWarnings()...)
		if response.GetFilterExecutionMode() != "" {
			if filterMode != "" && filterMode != response.GetFilterExecutionMode() {
				return SemanticSearchEvidence{}, errors.New("semantic filter execution mode changed between pages")
			}
			filterMode = response.GetFilterExecutionMode()
		}
		examinedCandidates = response.GetExaminedSemanticCandidates()
		eligibleCandidates = response.GetEligibleSemanticCandidates()
		for _, item := range response.GetResults() {
			if seenRecords[item.GetRecordId()] {
				return SemanticSearchEvidence{}, fmt.Errorf("semantic pagination returned duplicate record %q", item.GetRecordId())
			}
			seenRecords[item.GetRecordId()] = true
			searchResults = append(searchResults, item)
		}
		next := strings.TrimSpace(response.GetNextPageToken())
		if next == "" || !boolTarget(target, "allPages", false) {
			break
		}
		request.PageToken = proto.String(next)
	}
	if expectedCode := strings.TrimSpace(fmt.Sprint(target["expectFailureCode"])); expectedCode != "" && expectedCode != "<nil>" {
		return SemanticSearchEvidence{Query: query, Results: make([]SemanticSearchResultItem, 0, len(searchResults)), Warnings: warnings}, fmt.Errorf("semantic search succeeded, want failure code %s", expectedCode)
	}
	evidence := SemanticSearchEvidence{Query: query, Results: make([]SemanticSearchResultItem, 0, len(searchResults)), Warnings: warnings, FilterExecutionMode: filterMode, ExaminedSemanticCandidates: examinedCandidates, EligibleSemanticCandidates: eligibleCandidates}
	if request.GetFilter() != nil && (evidence.FilterExecutionMode == "" || evidence.ExaminedSemanticCandidates < evidence.EligibleSemanticCandidates) {
		return evidence, errors.New("semantic filter diagnostics are incomplete")
	}
	if len(evidence.Warnings) > 0 && !boolTarget(target, "allowWarnings", false) {
		return evidence, fmt.Errorf("semantic search returned warnings: %s", strings.Join(evidence.Warnings, "; "))
	}
	actualIDs := make([]string, 0, len(searchResults))
	for _, item := range searchResults {
		if item.GetNodeId() == "" || item.GetRecordId() == "" || item.GetSemanticRuleId() == "" || item.GetEmbeddingBindingKey() == "" || math.IsNaN(item.GetScore()) || math.IsInf(item.GetScore(), 0) {
			return evidence, errors.New("semantic search returned incomplete or non-finite evidence")
		}
		actualIDs = append(actualIDs, item.GetNodeId())
		provenance := item.GetProvenance()
		mapped := SemanticSearchResultItem{NodeID: item.GetNodeId(), RecordID: item.GetRecordId(), SemanticRuleID: item.GetSemanticRuleId(), EmbeddingBindingKey: item.GetEmbeddingBindingKey(), Score: item.GetScore()}
		if provenance != nil {
			mapped.SourceGraphRevision = provenance.GetSourceGraphRevision()
			mapped.SemanticGeneration = provenance.GetSemanticGeneration()
			mapped.SemanticConfigurationHash = provenance.GetSemanticConfigurationHash()
			mapped.OpaqueSourceVersion = provenance.GetOpaqueSourceVersion()
			mapped.Dimensions = provenance.GetDimensions()
			mapped.VectorSpaceKey = provenance.GetVectorSpaceKey()
			mapped.ConfiguredProfileID = provenance.GetConfiguredIntelligenceProfileId()
			mapped.ConfiguredModelEndpointID = provenance.GetConfiguredModelEndpointId()
			mapped.ConfiguredModelID = provenance.GetConfiguredModelId()
			mapped.ConfiguredCapabilityID = provenance.GetConfiguredCapabilityId()
			mapped.ActualRouteKnown = provenance.GetActualRouteKnown()
			mapped.ActualModelEndpointID = provenance.GetActualModelEndpointId()
			mapped.ActualModelID = provenance.GetActualModelId()
			mapped.ActualCapabilityID = provenance.GetActualCapabilityId()
			mapped.RebuildEpoch = provenance.GetRebuildEpoch()
			mapped.GeneratedAt = provenance.GetGeneratedAt()
		}
		if boolTarget(target, "requireProvenance", false) && (mapped.SourceGraphRevision == 0 || mapped.SemanticGeneration == 0 || mapped.SemanticConfigurationHash == "" || mapped.OpaqueSourceVersion == "" || mapped.Dimensions <= 0 || mapped.VectorSpaceKey == "" || mapped.ConfiguredProfileID == "" || mapped.ConfiguredModelEndpointID == "" || mapped.ConfiguredModelID == "" || mapped.ConfiguredCapabilityID == "" || mapped.GeneratedAt == "") {
			return evidence, fmt.Errorf("semantic search result %q has incomplete configured provenance", item.GetRecordId())
		}
		if boolTarget(target, "requireActualRoute", false) && (!mapped.ActualRouteKnown || mapped.ActualModelEndpointID == "" || mapped.ActualModelID == "" || mapped.ActualCapabilityID == "") {
			return evidence, fmt.Errorf("semantic search result %q has incomplete provider-observed route provenance", item.GetRecordId())
		}
		if key := targetString(target, "expectSourceRevisionFrom"); key != "" {
			expected, parseErr := strconv.ParseUint(resources.Semantic.EvidenceValues[key], 10, 64)
			if parseErr != nil || mapped.SourceGraphRevision != expected {
				return evidence, fmt.Errorf("semantic result source revision %d does not match capture %q", mapped.SourceGraphRevision, key)
			}
		}
		if key := targetString(target, "expectGenerationFrom"); key != "" {
			expected, parseErr := strconv.ParseUint(resources.Semantic.EvidenceValues[key], 10, 64)
			if parseErr != nil || mapped.SemanticGeneration != expected {
				return evidence, fmt.Errorf("semantic result generation %d does not match capture %q", mapped.SemanticGeneration, key)
			}
		}
		if key := targetString(target, "expectRebuildEpochFrom"); key != "" {
			expected := resources.Semantic.EvidenceValues[key]
			if expected == "" || mapped.RebuildEpoch != expected {
				return evidence, fmt.Errorf("semantic result rebuild epoch does not match capture %q", key)
			}
		}
		if key := targetString(target, "expectConfigurationHashFrom"); key != "" {
			expected := resources.Semantic.EvidenceValues[key]
			if expected == "" || mapped.SemanticConfigurationHash != expected {
				return evidence, fmt.Errorf("semantic result configuration hash does not match capture %q", key)
			}
		}
		if key := targetString(target, "rejectConfigurationHashFrom"); key != "" {
			if rejected := resources.Semantic.EvidenceValues[key]; rejected != "" && mapped.SemanticConfigurationHash == rejected {
				return evidence, fmt.Errorf("semantic result retained stale configuration hash capture %q", key)
			}
		}
		evidence.Results = append(evidence.Results, mapped)
	}
	if capture := targetString(target, "captureRecordAs"); capture != "" {
		if len(evidence.Results) == 0 {
			return evidence, fmt.Errorf("cannot capture semantic record %q from an empty result", capture)
		}
		resources.Semantic.EvidenceValues[capture] = evidence.Results[0].RecordID
	}
	if capture := targetString(target, "captureConfigurationHashAs"); capture != "" {
		if len(evidence.Results) == 0 || evidence.Results[0].SemanticConfigurationHash == "" {
			return evidence, fmt.Errorf("cannot capture semantic configuration hash %q from an empty result", capture)
		}
		resources.Semantic.EvidenceValues[capture] = evidence.Results[0].SemanticConfigurationHash
	}
	if expectedCapture := strings.TrimSpace(fmt.Sprint(target["expectRecordFrom"])); expectedCapture != "" && expectedCapture != "<nil>" {
		expectedRecordID := resources.Semantic.EvidenceValues[expectedCapture]
		if expectedRecordID == "" || len(evidence.Results) == 0 || evidence.Results[0].RecordID != expectedRecordID {
			return evidence, fmt.Errorf("semantic record identity does not match capture %q", expectedCapture)
		}
	}
	if rejectedCapture := strings.TrimSpace(fmt.Sprint(target["rejectRecordFrom"])); rejectedCapture != "" && rejectedCapture != "<nil>" {
		rejectedRecordID := resources.Semantic.EvidenceValues[rejectedCapture]
		for _, result := range evidence.Results {
			if rejectedRecordID != "" && result.RecordID == rejectedRecordID {
				return evidence, fmt.Errorf("semantic search returned stale captured record %q", rejectedCapture)
			}
		}
	}
	if expectedCount := intTarget(target, "expectedResultCount", -1); expectedCount >= 0 && len(evidence.Results) != expectedCount {
		return evidence, fmt.Errorf("semantic search returned %d results, want %d", len(evidence.Results), expectedCount)
	}
	expected := stringSliceTarget(target, "expectedNodeIds")
	if len(expected) > 0 {
		if len(actualIDs) < len(expected) {
			return evidence, fmt.Errorf("semantic search returned %d results, want at least %d", len(actualIDs), len(expected))
		}
		for index, nodeID := range expected {
			if actualIDs[index] != nodeID {
				return evidence, fmt.Errorf("semantic search result[%d]=%q, want %q", index, actualIDs[index], nodeID)
			}
		}
	}
	for _, forbidden := range stringSliceTarget(target, "forbiddenNodeIds") {
		for _, actual := range actualIDs {
			if actual == forbidden {
				return evidence, fmt.Errorf("semantic search returned forbidden node %q", forbidden)
			}
		}
	}
	if rawScores, ok := target["expectedScores"].([]any); ok {
		if len(rawScores) != len(evidence.Results) {
			return evidence, fmt.Errorf("semantic search returned %d scores, want %d", len(evidence.Results), len(rawScores))
		}
		tolerance := 1e-9
		if configured, ok := floatTarget(target, "scoreTolerance"); ok {
			tolerance = configured
		}
		for index, raw := range rawScores {
			expectedScore, ok := floatTarget(map[string]any{"score": raw}, "score")
			if !ok || math.Abs(evidence.Results[index].Score-expectedScore) > tolerance {
				return evidence, fmt.Errorf("semantic search score[%d]=%.12f, want %.12f +/- %.12f", index, evidence.Results[index].Score, expectedScore, tolerance)
			}
		}
	}
	minScore, hasMinimum := floatTarget(target, "minimumScore")
	if hasMinimum {
		for _, item := range evidence.Results {
			if item.Score < minScore {
				return evidence, fmt.Errorf("semantic search score %.8f is below minimum %.8f", item.Score, minScore)
			}
		}
	}
	return evidence, nil
}

func dialSemanticActor(ctx context.Context, resources *provision.ScenarioResources) (*mycel.Client, provision.ActorAssignment, error) {
	return dialSemanticActorGroup(ctx, resources, "")
}

func dialSemanticActorGroup(ctx context.Context, resources *provision.ScenarioResources, groupName string) (*mycel.Client, provision.ActorAssignment, error) {
	if resources == nil || resources.Semantic == nil {
		return nil, provision.ActorAssignment{}, errors.New("semantic resources are not provisioned")
	}
	return dialGraphActorGroup(ctx, resources, groupName, resources.Semantic.TargetActorID)
}

func dialGraphActorGroup(ctx context.Context, resources *provision.ScenarioResources, groupName, fallbackActorID string) (*mycel.Client, provision.ActorAssignment, error) {
	if resources == nil {
		return nil, provision.ActorAssignment{}, errors.New("graph actor resources are not provisioned")
	}
	for _, assignment := range resources.Assignments {
		if groupName != "" {
			if assignment.GroupName != groupName {
				continue
			}
		} else if assignment.ActorID != fallbackActorID {
			continue
		}
		if assignment.AccessToken == "" {
			return nil, assignment, errors.New("graph actor access token is unavailable")
		}
		client, err := mycel.Dial(ctx, mycel.Config{Addr: assignment.DaemonAddr, AccessToken: assignment.AccessToken, RefreshToken: assignment.RefreshToken, ClientName: "mycel-lab-graph-mutation"})
		if err != nil {
			return nil, assignment, fmt.Errorf("dial graph actor: %w", err)
		}
		return client, assignment, nil
	}
	return nil, provision.ActorAssignment{}, errors.New("graph target actor assignment was not found")
}

func targetString(target map[string]any, key string) string {
	value := strings.TrimSpace(fmt.Sprint(target[key]))
	if value == "<nil>" {
		return ""
	}
	return value
}

func floatTarget(target map[string]any, key string) (float64, bool) {
	value, ok := target[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func awaitSemanticMaintenance(ctx context.Context, admin *mycel.AdminClient, resources *provision.SemanticResources, target map[string]any) (SemanticMaintenanceResult, error) {
	timeout := resources.MaintenanceTimeout
	if value := durationTarget(target, "timeout", 0); value > 0 {
		timeout = value
	}
	pollInterval := resources.MaintenancePollInterval
	if value := durationTarget(target, "pollInterval", 0); value > 0 {
		pollInterval = value
	}
	if timeout <= 0 || pollInterval <= 0 || pollInterval > timeout {
		return SemanticMaintenanceResult{}, errors.New("semantic maintenance timeout and poll interval are invalid")
	}
	deadline := time.Now().Add(timeout)
	result := SemanticMaintenanceResult{}
	for sequence := 1; ; sequence++ {
		if len(result.Observations) >= maxSemanticMaintenanceObservations {
			return result, errors.New("semantic maintenance observation limit reached")
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		analyzed, analyzeErr := admin.SemanticMaintenance.AnalyzeSemanticDirtyWork(callCtx, &adminv1.AnalyzeSemanticDirtyWorkRequest{
			SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, Limit: int32(resources.MaintenanceAnalyzeLimit),
		})
		cancel()
		if analyzeErr != nil {
			return result, fmt.Errorf("analyze semantic maintenance: %w", analyzeErr)
		}
		callCtx, cancel = admin.AuthCallContext(ctx)
		processed, processErr := admin.SemanticMaintenance.ProcessSemanticDirtyWork(callCtx, &adminv1.ProcessSemanticDirtyWorkRequest{SpaceId: resources.SpaceID, Limit: int32(resources.MaintenanceProcessLimit)})
		cancel()
		if processErr != nil {
			return result, fmt.Errorf("process semantic maintenance: %w", processErr)
		}
		callCtx, cancel = admin.AuthCallContext(ctx)
		statusResponse, statusErr := admin.SemanticMaintenance.GetSemanticMaintenanceStatus(callCtx, &adminv1.GetSemanticMaintenanceStatusRequest{SpaceId: resources.SpaceID})
		cancel()
		if statusErr != nil {
			return result, fmt.Errorf("get semantic maintenance status: %w", statusErr)
		}
		observation := SemanticMaintenanceObservation{
			Sequence: sequence, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), ProcessedEvents: analyzed.GetProcessedEvents(), EnqueuedItems: analyzed.GetEnqueuedItems(),
			ProcessedItems: processed.GetProcessedItems(), CompletedItems: processed.GetCompletedItems(), FailedItems: processed.GetFailedItems(),
			QueueDepthPending: statusResponse.GetQueueDepthPending(), QueueDepthRunning: statusResponse.GetQueueDepthRunning(),
			QueueDepthRetryable: statusResponse.GetQueueDepthFailedRetryable(), QueueDepthPermanent: statusResponse.GetQueueDepthFailedPermanent(),
			Degraded: statusResponse.GetDegraded(), DegradedReason: statusResponse.GetDegradedReason(),
		}
		result.Observations = append(result.Observations, observation)
		if observation.Degraded || observation.QueueDepthPermanent > 0 {
			return result, fmt.Errorf("semantic maintenance failed closed: degraded=%t permanent=%d", observation.Degraded, observation.QueueDepthPermanent)
		}
		if observation.QueueDepthPending == 0 && observation.QueueDepthRunning == 0 && observation.QueueDepthRetryable == 0 && observation.QueueDepthPermanent == 0 {
			result.Converged = true
			return result, nil
		}
		if !time.Now().Before(deadline) {
			return result, fmt.Errorf("semantic maintenance did not converge within %s", timeout)
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
}
