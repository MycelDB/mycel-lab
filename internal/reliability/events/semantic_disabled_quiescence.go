package events

import (
	"context"
	"errors"
	"strconv"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/status"
)

// Quiescence preserves a previously fresh certificate while the rule is
// intentionally disabled. It is not a fresh-at-latest certificate.
type disabledSemanticQuiescence struct{ generation, fresh, latest uint64 }

func loadDisabledSemanticQuiescence(resources *provision.SemanticResources, target map[string]any) (*disabledSemanticQuiescence, error) {
	if target["completion"] != "disabled-quiescent" {
		return nil, nil
	}
	values := make([]uint64, 0, 3)
	for _, field := range []string{"expectGenerationFrom", "expectFreshRevisionFrom", "expectRevisionFrom"} {
		value, err := strconv.ParseUint(resources.EvidenceValues[targetString(target, field)], 10, 64)
		if err != nil || value == 0 {
			return nil, errors.New("disabled-quiescent completion requires positive captured generation/fresh/commit evidence")
		}
		values = append(values, value)
	}
	if values[2] <= values[1] {
		return nil, errors.New("disabled-quiescent commit must advance beyond the captured fresh revision")
	}
	return &disabledSemanticQuiescence{generation: values[0], fresh: values[1], latest: values[2]}, nil
}

func (q *disabledSemanticQuiescence) validate(proof *SemanticWorkReliabilityEvidenceV1) (bool, error) {
	if proof.SemanticGeneration != q.generation || proof.LatestObservedGraphRevision > q.latest || proof.AnalyzedThroughGraphRevision != q.fresh || proof.FreshThroughGraphRevision != q.fresh {
		return false, errors.New("disabled-quiescent generation or captured frontiers changed")
	}
	if proof.UnresolvedGapCount != 0 || proof.RebuildRequired {
		return false, errors.New("disabled-quiescent completion requires zero gaps and no rebuild flag")
	}
	// A read which has not observed the acknowledged commit may wait, but
	// cannot pass or authorize maintenance. Final evidence must match exactly.
	return proof.LatestObservedGraphRevision == q.latest, nil
}

// Only safe booleans/categories are retained, including on rejected reads. No
// foreign scope IDs, rule descriptions, provider settings or raw errors escape.
type SemanticDisabledRuleEvidenceV1 struct {
	SchemaVersion   int    `json:"schemaVersion"`
	Observed        bool   `json:"observed"`
	ScopeMatches    bool   `json:"scopeMatches"`
	BindingMatches  bool   `json:"bindingMatches"`
	Enabled         bool   `json:"enabled"`
	RPCCode         string `json:"rpcCode"`
	RejectionReason string `json:"rejectionReason,omitempty"`
}

func readDisabledSemanticRule(ctx context.Context, admin *mycel.AdminClient, resources *provision.SemanticResources) (*SemanticDisabledRuleEvidenceV1, error) {
	e := &SemanticDisabledRuleEvidenceV1{SchemaVersion: 1, RPCCode: "OK"}
	callCtx, cancel := admin.AuthCallContext(ctx)
	response, err := admin.Semantic.GetSemanticRule(callCtx, &adminv1.GetSemanticRuleRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID})
	cancel()
	if err != nil {
		e.RPCCode, e.RejectionReason = status.Code(err).String(), "rpc-failure"
		return e, maintenanceRPCFailure(ctx, "get disabled semantic rule", err)
	}
	rule := response.GetRule()
	e.Observed = rule != nil
	if !e.Observed {
		e.RejectionReason = "missing-rule"
		return e, errors.New("disabled-quiescent rule evidence is missing")
	}
	e.Enabled = rule.GetEnabled()
	e.ScopeMatches = rule.GetSpaceId() == resources.SpaceID && rule.GetDomainId() == resources.DomainID && rule.GetSemanticRuleId() == resources.SemanticRuleID
	matches := 0
	for _, binding := range rule.GetEmbeddings() {
		if binding.GetKey() == resources.EmbeddingBindingKey {
			matches++
			e.BindingMatches = binding.GetEnabled()
		}
	}
	e.BindingMatches = e.BindingMatches && matches == 1
	if !e.ScopeMatches || !e.BindingMatches {
		e.RejectionReason = "scope-or-binding-mismatch"
		return e, errors.New("disabled-quiescent rule scope or binding evidence is invalid")
	}
	if e.Enabled {
		e.RejectionReason = "rule-enabled"
		return e, errors.New("disabled-quiescent completion requires a publicly observed disabled rule")
	}
	return e, nil
}
