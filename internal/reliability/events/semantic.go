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
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: addr, Username: firstNonEmpty(resources.AdminUsername, provision.DefaultAdminUsername), Password: firstNonEmpty(resources.AdminPassword, provision.DefaultAdminPassword), ClientName: "mycel-lab-semantic"})
	if err != nil {
		return nil, fmt.Errorf("dial semantic admin: %w", err)
	}
	defer admin.Close()
	if eventType == "semantic-rule-create" {
		attempts, mutationAttempted, err := executeSemanticRuleMutation(ctx, func(probeCtx context.Context) error {
			return provision.ValidateSemanticRuleForCreate(probeCtx, admin, resources)
		}, func(mutationCtx context.Context) error {
			return provision.CreateSemanticRule(mutationCtx, admin, resources)
		})
		if err != nil {
			if mutationAttempted {
				return nil, fmt.Errorf("create semantic rule: %w", err)
			}
			return nil, fmt.Errorf("create semantic rule: %w", semanticRuleReadinessError(attempts, err))
		}
		return map[string]any{"semanticRuleId": resources.Semantic.SemanticRuleID, "key": resources.Semantic.SemanticRuleKey, "semanticRuleReadinessAttempts": attempts}, nil
	}
	switch eventType {
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
	case "semantic-provider-budget-reconcile":
		return reconcileSemanticProviderBudget(ctx, admin, addr, resources, target)
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

type SemanticProviderBudgetReconciliation struct {
	ScopeState                string `json:"scopeState"`
	DenialReason              string `json:"denialReason"`
	Requests                  int64  `json:"requests"`
	Attempts                  int64  `json:"attempts"`
	RetryAttempts             int64  `json:"retryAttempts"`
	BatchItems                int64  `json:"batchItems"`
	CompletedAttempts         int64  `json:"completedAttempts"`
	AmbiguousAttempts         int64  `json:"ambiguousAttempts"`
	ReservedEvents            int64  `json:"reservedEvents"`
	TerminalEvents            int64  `json:"terminalEvents"`
	InputTokens               int64  `json:"inputTokens"`
	ProviderMillis            int64  `json:"providerMillis"`
	EstimatedCostMicros       int64  `json:"estimatedCostMicros"`
	AccountingOverflow        bool   `json:"accountingOverflow"`
	EarliestRetainedSequence  uint64 `json:"earliestRetainedSequence"`
	SnapshotHighWaterSequence uint64 `json:"snapshotHighWaterSequence"`
	HistoryComplete           bool   `json:"historyComplete"`
	AttemptPairsValid         bool   `json:"attemptPairsValid"`
	UsageWithinLimits         bool   `json:"usageWithinLimits"`
	RetainedScopeDiscovered   bool   `json:"retainedScopeDiscovered"`
}

func advertisedSemanticProviderBudgetPageSize(advertised int32) int32 {
	if advertised > 0 {
		return advertised
	}
	return 100
}

func validateRetainedSemanticProviderBudgetScope(scope *adminv1.SemanticProviderBudgetScopeReference, resources *provision.SemanticResources) error {
	if scope.GetBudgetScopeId() != resources.ProviderBudgetScopeID || scope.GetSemanticRuleId() != resources.SemanticRuleID || scope.GetEmbeddingBindingKey() != resources.EmbeddingBindingKey || scope.GetCorrelationId() != resources.ExpectedProviderBudget.GetCorrelationId() {
		return errors.New("semantic provider budget retained scope filter integrity is invalid")
	}
	if scope.GetCreatedAt() == nil || scope.GetRetentionUntil() == nil || scope.GetCreatedAt().CheckValid() != nil || scope.GetRetentionUntil().CheckValid() != nil || !scope.GetRetentionUntil().AsTime().After(scope.GetCreatedAt().AsTime()) {
		return errors.New("semantic provider budget retained scope evidence is inconsistent")
	}
	return nil
}

func verifyRetainedSemanticProviderBudgetScope(ctx context.Context, admin *mycel.AdminClient, adminAddr string, scenario *provision.ScenarioResources) error {
	resources := scenario.Semantic
	pageSize := advertisedSemanticProviderBudgetPageSize(resources.ProviderBudgetMaxScopePage)
	pageToken := ""
	seenTokens := map[string]struct{}{}
	var previousSequence uint64
	matches := 0
	for pages := 0; ; pages++ {
		if pages > 10_000 {
			return errors.New("semantic provider budget scope pagination exceeded its bound")
		}
		if pageToken != "" {
			if _, duplicate := seenTokens[pageToken]; duplicate {
				return errors.New("semantic provider budget scope pagination repeated a token")
			}
			seenTokens[pageToken] = struct{}{}
		}
		page, err := provision.ReadSemanticAdminAcrossDaemons(ctx, admin, adminAddr, scenario, func(callCtx context.Context, client *mycel.AdminClient) (*adminv1.ListSemanticProviderBudgetScopesResponse, error) {
			return client.SemanticMaintenance.ListSemanticProviderBudgetScopes(callCtx, &adminv1.ListSemanticProviderBudgetScopesRequest{SpaceId: resources.SpaceID, CorrelationId: resources.ExpectedProviderBudget.GetCorrelationId(), SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, PageSize: pageSize, PageToken: pageToken})
		})
		if err != nil {
			return fmt.Errorf("list retained semantic provider budget scopes: %w", err)
		}
		for _, scope := range page.GetScopes() {
			if scope.GetScopeSequence() == 0 || previousSequence > 0 && scope.GetScopeSequence() <= previousSequence {
				return errors.New("semantic provider budget retained scope ordering is invalid")
			}
			previousSequence = scope.GetScopeSequence()
			if err := validateRetainedSemanticProviderBudgetScope(scope, resources); err != nil {
				return err
			}
			matches++
		}
		pageToken = page.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if matches != 1 {
		return errors.New("semantic provider budget retained scope was not discovered exactly once")
	}
	return nil
}

func knownSemanticBudgetScopeState(state adminv1.SemanticProviderBudgetScopeState) bool {
	switch state {
	case adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_ACTIVE,
		adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_COMPLETED,
		adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXPIRED,
		adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXHAUSTED:
		return true
	default:
		return false
	}
}

func knownSemanticBudgetDenialReason(reason adminv1.SemanticProviderBudgetDenialReason) bool {
	return reason >= adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE && reason <= adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_CAPACITY
}

func knownSemanticBudgetAttemptState(state adminv1.SemanticProviderBudgetAttemptState) bool {
	return state >= adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_RESERVED && state <= adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_AMBIGUOUS
}

func knownSemanticBudgetOutcome(outcome adminv1.SemanticProviderBudgetOutcomeCategory) bool {
	return outcome >= adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_NONE && outcome <= adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_AMBIGUOUS
}

func semanticBudgetEventTimestampsValid(event *adminv1.SemanticProviderBudgetAttemptEvent) bool {
	return event != nil && event.GetRecordedAt() != nil && event.GetReservedUntil() != nil && event.GetRecordedAt().CheckValid() == nil && event.GetReservedUntil().CheckValid() == nil
}

func semanticBudgetTerminalOutcomeMatches(event *adminv1.SemanticProviderBudgetAttemptEvent) bool {
	switch event.GetState() {
	case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_SUCCEEDED:
		return event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_SUCCESS && event.GetOutcomeKnown()
	case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_FAILED:
		if event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_AMBIGUOUS {
			return !event.GetOutcomeKnown()
		}
		return (event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_TRANSIENT_FAILURE || event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_PERMANENT_FAILURE) && event.GetOutcomeKnown()
	case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_CANCELED:
		return event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_CANCELED_BEFORE_EFFECT && event.GetOutcomeKnown() || event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_AMBIGUOUS && !event.GetOutcomeKnown()
	case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_AMBIGUOUS:
		return event.GetOutcomeCategory() == adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_AMBIGUOUS && !event.GetOutcomeKnown()
	default:
		return false
	}
}

func semanticBudgetUnknownOutcomeIsConservative(event *adminv1.SemanticProviderBudgetAttemptEvent) bool {
	return event.GetOutcomeKnown() || !event.GetUsageKnown() && !event.GetProviderDurationKnown() && event.GetChargedInputTokens() == event.GetReservedInputTokens() && event.GetChargedProviderMillis() == event.GetReservedProviderMillis() && event.GetChargedCostMicros() == event.GetReservedCostMicros()
}

func checkedSemanticBudgetSum(current, additional int64, overflow bool) (int64, bool) {
	if additional < 0 || current > math.MaxInt64-additional {
		return math.MaxInt64, true
	}
	return current + additional, overflow
}

func captureSemanticProviderBudgetSnapshot(pageIndex int64, page *adminv1.ListSemanticProviderBudgetAttemptEventsResponse, earliest, snapshot *uint64) error {
	if pageIndex == 0 {
		*earliest = page.GetEarliestRetainedEventSequence()
		*snapshot = page.GetSnapshotHighWaterSequence()
		return nil
	}
	if page.GetSnapshotHighWaterSequence() != *snapshot {
		return errors.New("semantic provider budget event snapshot changed during pagination")
	}
	return nil
}

func reconcileSemanticProviderBudget(ctx context.Context, admin *mycel.AdminClient, adminAddr string, scenario *provision.ScenarioResources, target map[string]any) (SemanticProviderBudgetReconciliation, error) {
	resources := scenario.Semantic
	if resources.ProviderBudgetScopeID == "" {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget scope is unavailable")
	}
	statusResponse, err := provision.ReadSemanticAdminAcrossDaemons(ctx, admin, adminAddr, scenario, func(callCtx context.Context, client *mycel.AdminClient) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
		return client.SemanticMaintenance.GetSemanticProviderBudgetStatus(callCtx, &adminv1.GetSemanticProviderBudgetStatusRequest{SpaceId: resources.SpaceID, Selector: &adminv1.GetSemanticProviderBudgetStatusRequest_BudgetScopeId{BudgetScopeId: resources.ProviderBudgetScopeID}})
	})
	if err != nil {
		return SemanticProviderBudgetReconciliation{}, fmt.Errorf("get semantic provider budget status: %w", err)
	}
	statusValue := statusResponse.GetStatus()
	if err := provision.ValidateSemanticProviderBudgetStatusIdentity(statusValue, resources); err != nil {
		return SemanticProviderBudgetReconciliation{}, err
	}
	if statusValue.GetUsage() == nil || statusValue.GetLimits() == nil {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget status is incomplete")
	}
	if !proto.Equal(statusValue.GetLimits(), resources.ExpectedProviderBudget) || statusValue.GetModelEndpointId() != resources.EndpointID || statusValue.GetModelId() != resources.ModelID || statusValue.GetModelEndpointCapabilityId() != resources.CapabilityID {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget status policy or route differs from the requested configuration")
	}
	if !knownSemanticBudgetScopeState(statusValue.GetState()) || !knownSemanticBudgetDenialReason(statusValue.GetDenialReason()) || statusValue.GetState() == adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXPIRED || statusValue.GetState() == adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_ACTIVE && statusValue.GetDenialReason() != adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE || statusValue.GetState() == adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_COMPLETED && statusValue.GetDenialReason() != adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE || statusValue.GetState() == adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXHAUSTED && statusValue.GetDenialReason() == adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget status has an unacceptable state")
	}
	limits := statusValue.GetLimits()
	if limits.GetMaxRetryAttempts() > math.MaxInt64-limits.GetMaxRequests() {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget attempt limit overflows")
	}
	maxAttempts := limits.GetMaxRequests() + limits.GetMaxRetryAttempts()
	if maxAttempts <= 0 || maxAttempts > 10000 {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget attempt limit is outside reconciliation bounds")
	}
	maxEvents := maxAttempts * 2
	if err := verifyRetainedSemanticProviderBudgetScope(ctx, admin, adminAddr, scenario); err != nil {
		return SemanticProviderBudgetReconciliation{}, err
	}
	var events []*adminv1.SemanticProviderBudgetAttemptEvent
	pageToken := ""
	seenPageTokens := map[string]struct{}{}
	var earliest, snapshot uint64
	pageSize := advertisedSemanticProviderBudgetPageSize(resources.ProviderBudgetMaxAttemptEventPage)
	for pages := int64(0); ; pages++ {
		if pages > maxEvents+1 {
			return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget event pagination exceeded its bound")
		}
		if pageToken != "" {
			if _, duplicate := seenPageTokens[pageToken]; duplicate {
				return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget event pagination repeated a token")
			}
			seenPageTokens[pageToken] = struct{}{}
		}
		page, listErr := provision.ReadSemanticAdminAcrossDaemons(ctx, admin, adminAddr, scenario, func(callCtx context.Context, client *mycel.AdminClient) (*adminv1.ListSemanticProviderBudgetAttemptEventsResponse, error) {
			return client.SemanticMaintenance.ListSemanticProviderBudgetAttemptEvents(callCtx, &adminv1.ListSemanticProviderBudgetAttemptEventsRequest{SpaceId: resources.SpaceID, BudgetScopeId: resources.ProviderBudgetScopeID, PageSize: pageSize, PageToken: pageToken})
		})
		if listErr != nil {
			return SemanticProviderBudgetReconciliation{}, fmt.Errorf("list semantic provider budget events: %w", listErr)
		}
		if page.GetHistoryGap() {
			return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget history has a retention gap")
		}
		if err := captureSemanticProviderBudgetSnapshot(pages, page, &earliest, &snapshot); err != nil {
			return SemanticProviderBudgetReconciliation{}, err
		}
		if int64(len(events))+int64(len(page.GetEvents())) > maxEvents {
			return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget event history exceeds the configured attempt bound")
		}
		events = append(events, page.GetEvents()...)
		pageToken = page.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if statusValue.GetEarliestRetainedEventSequence() != earliest || statusValue.GetHighWaterEventSequence() != snapshot {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget status/event snapshot mismatch")
	}
	usage := statusValue.GetUsage()
	if usage.GetRequests() < 0 || usage.GetAttempts() < 0 || usage.GetRetryAttempts() < 0 || usage.GetBatchItems() < 0 || usage.GetActiveProviderCalls() < 0 || usage.GetInputTokens() < 0 || usage.GetProviderMillis() < 0 || usage.GetEstimatedCostMicros() < 0 || usage.GetAmbiguousAttempts() < 0 {
		return SemanticProviderBudgetReconciliation{}, errors.New("semantic provider budget usage contains negative counters")
	}
	evidence := SemanticProviderBudgetReconciliation{
		ScopeState: statusValue.GetState().String(), DenialReason: statusValue.GetDenialReason().String(),
		Requests: usage.GetRequests(), Attempts: usage.GetAttempts(), RetryAttempts: usage.GetRetryAttempts(), AmbiguousAttempts: usage.GetAmbiguousAttempts(),
		InputTokens: usage.GetInputTokens(), ProviderMillis: usage.GetProviderMillis(), EstimatedCostMicros: usage.GetEstimatedCostMicros(), AccountingOverflow: usage.GetAccountingOverflow(),
		EarliestRetainedSequence: earliest, SnapshotHighWaterSequence: snapshot, HistoryComplete: true, RetainedScopeDiscovered: true,
	}
	type attemptVersions struct {
		reserved, terminal int
		requestID          string
		retryIndex         int64
		batchItems         int64
		reservedEvent      *adminv1.SemanticProviderBudgetAttemptEvent
		terminalEvent      *adminv1.SemanticProviderBudgetAttemptEvent
	}
	attempts := map[string]attemptVersions{}
	requestRetries := map[string]map[int64]struct{}{}
	lastSequence := uint64(0)
	for _, event := range events {
		if event.GetEventSequence() <= lastSequence || event.GetEventSequence() > snapshot || event.GetBudgetScopeId() != resources.ProviderBudgetScopeID || event.GetAttemptId() == "" || event.GetRequestId() == "" || event.GetRetryIndex() < 0 || event.GetRetryIndex() > limits.GetMaxRetriesPerRequest() || event.GetBatchItems() <= 0 || event.GetBatchItems() > limits.GetMaxBatchItems() || !semanticBudgetEventTimestampsValid(event) || event.GetReservedInputTokens() < 0 || event.GetChargedInputTokens() < 0 || event.GetReservedProviderMillis() < 0 || event.GetChargedProviderMillis() < 0 || event.GetReservedCostMicros() < 0 || event.GetChargedCostMicros() < 0 || event.GetReservedInputTokens() > limits.GetMaxInputTokensPerRequest() || event.GetReservedProviderMillis() > limits.GetRequestTimeout().AsDuration().Milliseconds() {
			return evidence, errors.New("semantic provider budget event identity, sequence, or per-request bound is invalid")
		}
		if !knownSemanticBudgetAttemptState(event.GetState()) || !knownSemanticBudgetOutcome(event.GetOutcomeCategory()) {
			return evidence, errors.New("semantic provider budget event identity, sequence, or per-request bound is invalid")
		}
		lastSequence = event.GetEventSequence()
		version := attempts[event.GetAttemptId()]
		switch event.GetState() {
		case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_RESERVED:
			if event.GetAttemptVersion() != 1 || version.reserved != 0 || version.terminal != 0 {
				return evidence, errors.New("semantic provider budget reservation event is not immutable version 1")
			}
			indexes := requestRetries[event.GetRequestId()]
			if indexes == nil {
				indexes = map[int64]struct{}{}
				requestRetries[event.GetRequestId()] = indexes
			}
			if _, duplicate := indexes[event.GetRetryIndex()]; duplicate {
				return evidence, errors.New("semantic provider budget request contains a duplicate retry index")
			}
			indexes[event.GetRetryIndex()] = struct{}{}
			if event.GetOutcomeCategory() != adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_NONE || event.GetOutcomeKnown() || event.GetUsageKnown() || event.GetProviderDurationKnown() || event.GetChargedInputTokens() != event.GetReservedInputTokens() || event.GetChargedProviderMillis() != event.GetReservedProviderMillis() || event.GetChargedCostMicros() != event.GetReservedCostMicros() {
				return evidence, errors.New("semantic provider budget reservation has terminal or non-conservative accounting fields")
			}
			version = attemptVersions{reserved: 1, requestID: event.GetRequestId(), retryIndex: event.GetRetryIndex(), batchItems: event.GetBatchItems(), reservedEvent: event}
			evidence.ReservedEvents++
			var batchOverflow bool
			evidence.BatchItems, batchOverflow = checkedSemanticBudgetSum(evidence.BatchItems, event.GetBatchItems(), false)
			if batchOverflow {
				return evidence, errors.New("semantic provider budget batch accounting overflowed")
			}
		case adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_SUCCEEDED,
			adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_FAILED,
			adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_CANCELED,
			adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_AMBIGUOUS:
			if event.GetAttemptVersion() != 2 || version.reserved != 1 || version.terminal != 0 || event.GetRequestId() != version.requestID || event.GetRetryIndex() != version.retryIndex || event.GetBatchItems() != version.batchItems || !semanticBudgetTerminalOutcomeMatches(event) {
				return evidence, errors.New("semantic provider budget terminal event is not paired with one reservation")
			}
			reserved := version.reservedEvent
			if event.GetReservedInputTokens() != reserved.GetReservedInputTokens() || event.GetReservedProviderMillis() != reserved.GetReservedProviderMillis() || event.GetReservedCostMicros() != reserved.GetReservedCostMicros() || !event.GetReservedUntil().AsTime().Equal(reserved.GetReservedUntil().AsTime()) {
				return evidence, errors.New("semantic provider budget terminal event changed its immutable reservation")
			}
			if !event.GetUsageKnown() && (event.GetChargedInputTokens() != event.GetReservedInputTokens() || event.GetChargedCostMicros() != event.GetReservedCostMicros()) {
				return evidence, errors.New("semantic provider budget terminal event does not conservatively charge unknown usage")
			}
			if !event.GetProviderDurationKnown() && event.GetChargedProviderMillis() != event.GetReservedProviderMillis() {
				return evidence, errors.New("semantic provider budget terminal event does not conservatively charge unknown duration")
			}
			if !semanticBudgetUnknownOutcomeIsConservative(event) {
				return evidence, errors.New("semantic provider budget unknown outcome released a conservative reservation")
			}
			version.terminal++
			version.terminalEvent = event
			evidence.TerminalEvents++
		default:
			return evidence, errors.New("semantic provider budget event has an unspecified state")
		}
		attempts[event.GetAttemptId()] = version
	}
	derivedRetries := int64(0)
	for _, indexes := range requestRetries {
		if _, ok := indexes[0]; !ok || int64(len(indexes)-1) > limits.GetMaxRetriesPerRequest() {
			return evidence, errors.New("semantic provider budget request retry sequence is invalid")
		}
		for index := int64(0); index < int64(len(indexes)); index++ {
			if _, ok := indexes[index]; !ok {
				return evidence, errors.New("semantic provider budget request retry sequence has a gap")
			}
		}
		derivedRetries += int64(len(indexes) - 1)
	}
	requireTerminal := boolTarget(target, "requireTerminal", true)
	if requireTerminal && evidence.TerminalEvents != evidence.ReservedEvents {
		return evidence, fmt.Errorf("semantic provider budget has %d reservations but %d terminal events", evidence.ReservedEvents, evidence.TerminalEvents)
	}
	minimumAttempts := int64(intTarget(target, "minimumAttempts", 1))
	if evidence.ReservedEvents < minimumAttempts {
		return evidence, fmt.Errorf("semantic provider budget observed %d attempts, want at least %d", evidence.ReservedEvents, minimumAttempts)
	}
	evidence.CompletedAttempts = evidence.TerminalEvents
	var derivedInput, derivedMillis, derivedCost, derivedActive, derivedAmbiguous int64
	derivedOverflow := false
	reservationExceeded := false
	now := time.Now().UTC()
	for _, version := range attempts {
		selected := version.reservedEvent
		if version.terminalEvent != nil {
			selected = version.terminalEvent
			reservationExceeded = reservationExceeded || selected.GetChargedInputTokens() > selected.GetReservedInputTokens() || selected.GetChargedProviderMillis() > selected.GetReservedProviderMillis() || selected.GetChargedCostMicros() > selected.GetReservedCostMicros() || selected.GetChargedCostSaturated()
		}
		if selected == nil {
			return evidence, errors.New("semantic provider budget attempt lacks a reservation event")
		}
		derivedInput, derivedOverflow = checkedSemanticBudgetSum(derivedInput, selected.GetChargedInputTokens(), derivedOverflow)
		derivedMillis, derivedOverflow = checkedSemanticBudgetSum(derivedMillis, selected.GetChargedProviderMillis(), derivedOverflow)
		derivedCost, derivedOverflow = checkedSemanticBudgetSum(derivedCost, selected.GetChargedCostMicros(), derivedOverflow || selected.GetChargedCostSaturated())
		if version.terminalEvent == nil {
			derivedActive++
			if selected.GetReservedUntil() != nil && !selected.GetReservedUntil().AsTime().After(now) {
				derivedAmbiguous++
			}
		} else if !selected.GetOutcomeKnown() {
			derivedActive++
			derivedAmbiguous++
		} else if selected.GetState() == adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_AMBIGUOUS {
			derivedAmbiguous++
		}
	}
	if evidence.ReservedEvents != usage.GetAttempts() || int64(len(requestRetries)) != usage.GetRequests() || derivedRetries != usage.GetRetryAttempts() || evidence.BatchItems != usage.GetBatchItems() || derivedInput != usage.GetInputTokens() || derivedMillis != usage.GetProviderMillis() || derivedCost != usage.GetEstimatedCostMicros() || derivedActive != usage.GetActiveProviderCalls() || derivedAmbiguous != usage.GetAmbiguousAttempts() || derivedOverflow != usage.GetAccountingOverflow() {
		return evidence, errors.New("semantic provider budget aggregate usage does not reconcile with immutable events")
	}
	if reservationExceeded && (statusValue.GetState() != adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXHAUSTED || statusValue.GetDenialReason() != adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_ACCOUNTING) {
		return evidence, errors.New("semantic provider budget reservation overage did not exhaust the scope with accounting denial")
	}
	evidence.BatchItems = usage.GetBatchItems()
	evidence.AttemptPairsValid = true
	if limits.GetMaxBatchItems() <= 0 || limits.GetMaxBatchItems() > math.MaxInt64/maxAttempts {
		return evidence, errors.New("semantic provider budget batch aggregate bound overflows")
	}
	maxBatchItems := limits.GetMaxBatchItems() * maxAttempts
	evidence.UsageWithinLimits = usage.GetRequests() <= limits.GetMaxRequests() && usage.GetAttempts() <= maxAttempts && usage.GetRetryAttempts() <= limits.GetMaxRetryAttempts() && usage.GetBatchItems() <= maxBatchItems && usage.GetActiveProviderCalls() <= limits.GetMaxConcurrentProviderCalls() && usage.GetAmbiguousAttempts() <= usage.GetAttempts() && usage.GetInputTokens() <= limits.GetMaxInputTokens() && usage.GetProviderMillis() <= limits.GetTotalTimeout().AsDuration().Milliseconds() && usage.GetEstimatedCostMicros() <= limits.GetCost().GetMaxEstimatedCostMicros() && !usage.GetAccountingOverflow()
	if !evidence.UsageWithinLimits {
		return evidence, errors.New("semantic provider budget usage exceeded configured limits")
	}
	return evidence, nil
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

type semanticBudgetAuthorizationSnapshot struct {
	highWater uint64
	usage     *adminv1.SemanticProviderBudgetUsage
	state     adminv1.SemanticProviderBudgetScopeState
	denial    adminv1.SemanticProviderBudgetDenialReason
}

// SemanticAuthorizationProbeResult contains only evidence-safe authorization
// and aggregate budget fields. It deliberately excludes semantic identifiers,
// credentials, provider inputs, and provider outputs so a failed live probe can
// retain enough evidence to distinguish authorization from authentication and
// to account for provider effects.
type SemanticAuthorizationProbeResult struct {
	Stage                   string
	ActorAuthenticated      bool
	SearchAttempted         bool
	Rejected                bool
	RejectedCode            string
	ProviderBudgetObserved  bool
	ProviderBudgetUnchanged bool
	Requests                int64
	Attempts                int64
	RetryAttempts           int64
	BatchItems              int64
	InputTokens             int64
	EstimatedCostMicros     int64
	ActiveProviderCalls     int64
	AmbiguousAttempts       int64
}

func semanticAuthorizationProbeResult(code codes.Code, before, after semanticBudgetAuthorizationSnapshot, observed, searchAttempted bool) SemanticAuthorizationProbeResult {
	result := SemanticAuthorizationProbeResult{
		SearchAttempted:         searchAttempted,
		Rejected:                searchAttempted && (code == codes.PermissionDenied || code == codes.NotFound),
		RejectedCode:            code.String(),
		ProviderBudgetObserved:  observed,
		ProviderBudgetUnchanged: observed && before.highWater == after.highWater && before.state == after.state && before.denial == after.denial && proto.Equal(before.usage, after.usage),
	}
	if observed && after.usage != nil {
		result.Requests = after.usage.GetRequests()
		result.Attempts = after.usage.GetAttempts()
		result.RetryAttempts = after.usage.GetRetryAttempts()
		result.BatchItems = after.usage.GetBatchItems()
		result.InputTokens = after.usage.GetInputTokens()
		result.EstimatedCostMicros = after.usage.GetEstimatedCostMicros()
		result.ActiveProviderCalls = after.usage.GetActiveProviderCalls()
		result.AmbiguousAttempts = after.usage.GetAmbiguousAttempts()
	}
	return result
}

func readSemanticBudgetAuthorizationSnapshot(ctx context.Context, client *mycel.AdminClient, clientAddr string, scenario *provision.ScenarioResources) (semanticBudgetAuthorizationSnapshot, error) {
	resources := scenario.Semantic
	if resources == nil || strings.TrimSpace(resources.ProviderBudgetScopeID) == "" {
		return semanticBudgetAuthorizationSnapshot{}, errors.New("semantic provider budget scope is unavailable for authorization proof")
	}
	response, err := provision.ReadSemanticAdminAcrossDaemons(ctx, client, clientAddr, scenario, func(callCtx context.Context, peer *mycel.AdminClient) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
		return peer.SemanticMaintenance.GetSemanticProviderBudgetStatus(callCtx, &adminv1.GetSemanticProviderBudgetStatusRequest{SpaceId: resources.SpaceID, Selector: &adminv1.GetSemanticProviderBudgetStatusRequest_BudgetScopeId{BudgetScopeId: resources.ProviderBudgetScopeID}})
	})
	if err != nil {
		return semanticBudgetAuthorizationSnapshot{}, err
	}
	value := response.GetStatus()
	if err := provision.ValidateSemanticProviderBudgetStatusIdentity(value, resources); err != nil {
		return semanticBudgetAuthorizationSnapshot{}, err
	}
	if value.GetUsage() == nil {
		return semanticBudgetAuthorizationSnapshot{}, errors.New("semantic provider budget status is incomplete")
	}
	return semanticBudgetAuthorizationSnapshot{highWater: value.GetHighWaterEventSequence(), usage: proto.Clone(value.GetUsage()).(*adminv1.SemanticProviderBudgetUsage), state: value.GetState(), denial: value.GetDenialReason()}, nil
}

func retainSemanticAuthorizationFailureEvidence(ctx context.Context, controller *mycel.AdminClient, controllerAddr string, resources *provision.ScenarioResources, before semanticBudgetAuthorizationSnapshot, stage string, actorAuthenticated bool, cause error) (SemanticAuthorizationProbeResult, error) {
	after, snapshotErr := readSemanticBudgetAuthorizationSnapshot(ctx, controller, controllerAddr, resources)
	result := semanticAuthorizationProbeResult(status.Code(cause), before, after, snapshotErr == nil, false)
	result.Stage = stage
	result.ActorAuthenticated = actorAuthenticated
	if snapshotErr != nil {
		result.Stage = stage + "-budget-after-unavailable"
	}
	return result, fmt.Errorf("%s: %w", stage, cause)
}

func executeSemanticUnauthorizedSearch(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (SemanticAuthorizationProbeResult, error) {
	result := SemanticAuthorizationProbeResult{Stage: "controller-resolution"}
	controllerClient, controllerAssignment, err := dialSemanticActor(ctx, resources)
	if err != nil {
		return result, fmt.Errorf("resolve semantic controller for authorization proof: %w", err)
	}
	controllerClient.Close()
	result.Stage = "controller-client"
	controller, err := mycel.DialAdmin(ctx, mycel.Config{Addr: controllerAssignment.DaemonAddr, AccessToken: controllerAssignment.AccessToken, ClientName: "mycel-lab-semantic-authorization-proof"})
	if err != nil {
		return result, fmt.Errorf("dial semantic controller for authorization proof: %w", err)
	}
	defer controller.Close()
	result.Stage = "budget-before"
	before, err := readSemanticBudgetAuthorizationSnapshot(ctx, controller, controllerAssignment.DaemonAddr, resources)
	if err != nil {
		return result, fmt.Errorf("capture semantic provider budget before unauthorized search: %w", err)
	}
	result.Stage = "negative-actor-assignment"
	groupName := strings.TrimSpace(fmt.Sprint(target["actorGroup"]))
	if groupName == "" || groupName == "<nil>" {
		return result, errors.New("semantic unauthorized search requires actorGroup")
	}
	var outsider provision.ActorAssignment
	for _, assignment := range resources.Assignments {
		if assignment.GroupName == groupName {
			outsider = assignment
			break
		}
	}
	if outsider.ActorID == "" || strings.TrimSpace(outsider.PrincipalID) == "" || strings.TrimSpace(outsider.Username) == "" || strings.TrimSpace(outsider.AccessToken) == "" || strings.TrimSpace(outsider.DaemonAddr) == "" {
		return result, fmt.Errorf("semantic unauthorized actor group %q has no node-local session assignment", groupName)
	}

	// Provisioning creates this session through an admin client authenticated to
	// outsider.DaemonAddr. Reuse that node-local session instead of performing a
	// second password login, which would add an unrelated Raft session mutation
	// to an otherwise read-only authorization negative control.
	result.Stage = "negative-actor-client"
	client, err := mycel.Dial(ctx, mycel.Config{Addr: outsider.DaemonAddr, AccessToken: outsider.AccessToken, ClientName: "mycel-lab-semantic-negative"})
	if err != nil {
		return retainSemanticAuthorizationFailureEvidence(ctx, controller, controllerAssignment.DaemonAddr, resources, before, result.Stage, false, err)
	}
	defer client.Close()
	result.Stage = "negative-actor-authentication"
	identity, err := client.WhoAmI(ctx)
	if err != nil {
		return retainSemanticAuthorizationFailureEvidence(ctx, controller, controllerAssignment.DaemonAddr, resources, before, result.Stage, false, err)
	}
	if identity.PrincipalID != outsider.PrincipalID || identity.Username != outsider.Username {
		return retainSemanticAuthorizationFailureEvidence(ctx, controller, controllerAssignment.DaemonAddr, resources, before, result.Stage, false, status.Error(codes.FailedPrecondition, "semantic unauthorized actor identity does not match its assignment"))
	}
	result.ActorAuthenticated = true
	result.Stage = "semantic-search"
	callCtx, cancel := client.AuthCallContext(ctx)
	_, searchErr := client.Semantic.SemanticSearch(callCtx, &clientv1.SemanticSearchRequest{SpaceId: resources.Semantic.SpaceID, DomainId: resources.Semantic.DomainID, SemanticRuleId: proto.String(resources.Semantic.SemanticRuleID), EmbeddingBindingKey: proto.String(resources.Semantic.EmbeddingBindingKey), Query: "axis-0", Limit: 1})
	cancel()
	code := status.Code(searchErr)

	// Always perform the read-only after snapshot, even for an unexpected code,
	// so a failed live negative control still proves whether provider accounting
	// changed and retains bounded aggregate usage without provider content.
	after, snapshotErr := readSemanticBudgetAuthorizationSnapshot(ctx, controller, controllerAssignment.DaemonAddr, resources)
	result = semanticAuthorizationProbeResult(code, before, after, snapshotErr == nil, true)
	result.Stage = "budget-after"
	result.ActorAuthenticated = true
	if snapshotErr != nil {
		return result, fmt.Errorf("capture semantic provider budget after unauthorized search: %w", snapshotErr)
	}
	result.Stage = "semantic-search-result"
	if code != codes.PermissionDenied && code != codes.NotFound {
		if searchErr == nil {
			return result, status.Error(codes.FailedPrecondition, "unauthorized semantic search unexpectedly succeeded")
		}
		return result, fmt.Errorf("unauthorized semantic search returned %s, want PermissionDenied or NotFound: %w", code, searchErr)
	}
	if !result.ProviderBudgetUnchanged {
		result.Stage = "budget-verification"
		return result, status.Error(codes.FailedPrecondition, "unauthorized semantic search changed provider budget evidence")
	}
	result.Stage = "complete"
	return result, nil
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
