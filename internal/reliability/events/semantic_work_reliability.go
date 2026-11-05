package events

import (
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
)

// SemanticWorkReliabilityEvidenceV1 retains only safe public scalar diagnostics,
// even when validation fails. Observed distinguishes an absent/RPC-failed response
// from genuine zero values. Provider/error bodies and foreign scope IDs are never
// copied; unrecognized state strings are replaced with a fixed marker.
type SemanticWorkReliabilityEvidenceV1 struct {
	SchemaVersion                 int    `json:"schemaVersion"`
	Observed                      bool   `json:"observed"`
	ScopeMatches                  bool   `json:"scopeMatches"`
	RPCCode                       string `json:"rpcCode"`
	RejectionReason               string `json:"rejectionReason,omitempty"`
	Degraded                      bool   `json:"degraded"`
	RebuildRequired               bool   `json:"rebuildRequired"`
	RebuildPresent                bool   `json:"rebuildPresent"`
	RebuildState                  string `json:"rebuildState"`
	SemanticGeneration            uint64 `json:"semanticGeneration"`
	LatestObservedGraphRevision   uint64 `json:"latestObservedGraphRevision"`
	AnalyzedThroughGraphRevision  uint64 `json:"analyzedThroughGraphRevision"`
	FreshThroughGraphRevision     uint64 `json:"freshThroughGraphRevision"`
	UnresolvedGapCount            uint64 `json:"unresolvedGapCount"`
	OldestUnresolvedGraphRevision uint64 `json:"oldestUnresolvedGraphRevision"`
	CapacityState                 string `json:"capacityState"`
	ConfiguredFrontierLimit       uint64 `json:"configuredFrontierLimit"`
	ConfiguredGapIntervalLimit    uint64 `json:"configuredGapIntervalLimit"`
	ConfiguredRevisionSpanLimit   uint64 `json:"configuredRevisionSpanLimit"`
	ConfiguredSerializedByteLimit uint64 `json:"configuredSerializedByteLimit"`
}

func captureSemanticWorkReliability(r *adminv1.GetSemanticReliabilityStatusResponse, resources *provision.SemanticResources) *SemanticWorkReliabilityEvidenceV1 {
	e := &SemanticWorkReliabilityEvidenceV1{SchemaVersion: 1, Observed: r != nil, RPCCode: "OK"}
	if r == nil {
		return e
	}
	e.ScopeMatches = resources != nil && r.GetSpaceId() == resources.SpaceID && r.GetSemanticRuleId() == resources.SemanticRuleID && r.GetEmbeddingBindingKey() == resources.EmbeddingBindingKey
	e.Degraded, e.RebuildRequired, e.RebuildPresent = r.GetDegraded(), r.GetRebuildRequired(), r.GetRebuild() != nil
	switch state := r.GetRebuild().GetState(); state {
	case "", "completed", "required", "running", "failed":
		e.RebuildState = state
	default:
		e.RebuildState = "unknown"
	}
	switch state := r.GetCapacityState(); state {
	case "", "ok", "exhausted":
		e.CapacityState = state
	default:
		e.CapacityState = "unknown"
	}
	e.SemanticGeneration = r.GetSemanticGeneration()
	e.LatestObservedGraphRevision, e.AnalyzedThroughGraphRevision, e.FreshThroughGraphRevision = r.GetLatestObservedGraphRevision(), r.GetAnalyzedThroughGraphRevision(), r.GetFreshThroughGraphRevision()
	e.UnresolvedGapCount, e.OldestUnresolvedGraphRevision = r.GetUnresolvedGapCount(), r.GetOldestUnresolvedGraphRevision()
	e.ConfiguredFrontierLimit, e.ConfiguredGapIntervalLimit = r.GetConfiguredFrontierLimit(), r.GetConfiguredGapIntervalLimit()
	e.ConfiguredRevisionSpanLimit, e.ConfiguredSerializedByteLimit = r.GetConfiguredRevisionSpanLimit(), r.GetConfiguredSerializedByteLimit()
	return e
}

func (e *SemanticWorkReliabilityEvidenceV1) reject(reason string) error {
	e.RejectionReason = reason
	return errors.New("semantic work reliability evidence rejected: " + reason)
}

// The v0.21.4 public status sets rebuild_required for any unresolved gap, not
// just an actual rebuild transition. With an idle/completed rebuild and healthy
// bounded capacity, that boolean MUST equal (gap count > 0). It is not permission
// to rebuild/repair: pending/failure callers still prove gaps and stale freshness;
// completion still requires no gaps, a false flag, and the expected frontier.
func validateSemanticWorkReliability(e *SemanticWorkReliabilityEvidenceV1, allowDegraded bool) error {
	if !e.Observed {
		return e.reject("missing-response")
	}
	if !e.ScopeMatches {
		return e.reject("scope-mismatch")
	}
	if e.SemanticGeneration == 0 {
		return e.reject("missing-generation")
	}
	if e.ConfiguredFrontierLimit == 0 || e.ConfiguredGapIntervalLimit == 0 || e.ConfiguredRevisionSpanLimit == 0 || e.ConfiguredSerializedByteLimit == 0 {
		return e.reject("missing-capacity-limits")
	}
	if e.CapacityState != "ok" {
		return e.reject("capacity-not-ok")
	}
	if !e.RebuildPresent {
		return e.reject("missing-rebuild-status")
	}
	if e.RebuildState != "" && e.RebuildState != "completed" {
		return e.reject("unsafe-rebuild-state")
	}
	if e.RebuildRequired != (e.UnresolvedGapCount > 0) {
		return e.reject("gap-flag-mismatch")
	}
	if e.AnalyzedThroughGraphRevision > e.LatestObservedGraphRevision || e.FreshThroughGraphRevision > e.AnalyzedThroughGraphRevision {
		return e.reject("frontier-order")
	}
	if e.UnresolvedGapCount == 0 {
		if e.OldestUnresolvedGraphRevision != 0 {
			return e.reject("gap-range")
		}
	} else if e.OldestUnresolvedGraphRevision == 0 || e.OldestUnresolvedGraphRevision <= e.FreshThroughGraphRevision || e.OldestUnresolvedGraphRevision > e.LatestObservedGraphRevision {
		return e.reject("gap-range")
	}
	if e.Degraded && !allowDegraded {
		return e.reject("degraded-pending-or-complete")
	}
	return nil
}
