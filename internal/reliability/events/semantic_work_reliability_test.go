package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticWorkV0214GapFlagIsNotAnActualRebuild(t *testing.T) {
	for _, state := range []string{"", "completed"} {
		t.Run("state-"+state, func(t *testing.T) {
			p, resources, target := semanticWorkFixture()
			p.reliability.Rebuild.State = state
			proof, err := awaitSemanticWork(context.Background(), p, resources, target)
			if err != nil {
				t.Fatalf("released v0.21.4 pending contract rejected: %v", err)
			}
			if proof.Reliability == nil || !proof.Reliability.Observed || !proof.Reliability.RebuildRequired || proof.Reliability.RebuildState != state || proof.Reliability.UnresolvedGapCount != 1 || proof.Reliability.FreshThroughGraphRevision != 1 || proof.Reliability.RejectionReason != "" {
				t.Fatalf("missing public pending-state evidence: %+v", proof)
			}
			snapshot, err := readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2)
			if err != nil || snapshot.evidence == nil || !snapshot.evidence.RebuildRequired {
				t.Fatalf("fault reader lost the gap contract: %+v %v", snapshot.evidence, err)
			}
			completeSemanticWork(p)
			target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
			done, err := awaitSemanticWork(context.Background(), p, resources, target)
			if err != nil || done.Reliability.RebuildRequired || done.Reliability.UnresolvedGapCount != 0 || done.Reliability.FreshThroughGraphRevision != 2 {
				t.Fatalf("completion fence failed: %+v %v", done, err)
			}
			snapshot, err = readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2)
			if err != nil || snapshot.evidence.RebuildRequired {
				t.Fatalf("completion fault read failed: %+v %v", snapshot.evidence, err)
			}
			if p.analyzeCalls != 0 {
				t.Fatal("read-only contract checks mutated maintenance")
			}
		})
	}
}

func TestSemanticWorkReliabilityRejectsUnsafeStatesAndRetainsActualDiagnostics(t *testing.T) {
	cases := []struct {
		name, reason string
		mutate       func(*semanticWorkProbe)
	}{
		{"missing-response", "missing-response", func(p *semanticWorkProbe) { p.reliability = nil }},
		{"foreign-scope", "scope-mismatch", func(p *semanticWorkProbe) { p.reliability.SpaceId = "foreign-private-identity" }},
		{"zero-generation", "missing-generation", func(p *semanticWorkProbe) { p.reliability.SemanticGeneration = 0 }},
		{"missing-frontier-limit", "missing-capacity-limits", func(p *semanticWorkProbe) { p.reliability.ConfiguredFrontierLimit = 0 }},
		{"missing-gap-limit", "missing-capacity-limits", func(p *semanticWorkProbe) { p.reliability.ConfiguredGapIntervalLimit = 0 }},
		{"missing-span-limit", "missing-capacity-limits", func(p *semanticWorkProbe) { p.reliability.ConfiguredRevisionSpanLimit = 0 }},
		{"missing-byte-limit", "missing-capacity-limits", func(p *semanticWorkProbe) { p.reliability.ConfiguredSerializedByteLimit = 0 }},
		{"exhausted-capacity", "capacity-not-ok", func(p *semanticWorkProbe) { p.reliability.CapacityState = "exhausted" }},
		{"unknown-capacity", "capacity-not-ok", func(p *semanticWorkProbe) { p.reliability.CapacityState = "private-capacity-details" }},
		{"nil-rebuild", "missing-rebuild-status", func(p *semanticWorkProbe) { p.reliability.Rebuild = nil }},
		{"required-rebuild", "unsafe-rebuild-state", func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "required" }},
		{"running-rebuild", "unsafe-rebuild-state", func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "running" }},
		{"failed-rebuild", "unsafe-rebuild-state", func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "failed" }},
		{"unknown-rebuild", "unsafe-rebuild-state", func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "private-rebuild-details" }},
		{"pending-flag-cleared", "gap-flag-mismatch", func(p *semanticWorkProbe) { p.reliability.RebuildRequired = false }},
		{"flag-without-gaps", "gap-flag-mismatch", func(p *semanticWorkProbe) { p.reliability.UnresolvedGapCount = 0 }},
		{"future-analyzer", "frontier-order", func(p *semanticWorkProbe) { p.reliability.AnalyzedThroughGraphRevision = 3 }},
		{"future-freshness", "frontier-order", func(p *semanticWorkProbe) { p.reliability.FreshThroughGraphRevision = 3 }},
		{"missing-oldest-gap", "gap-range", func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 0 }},
		{"gap-behind-fresh", "gap-range", func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 1 }},
		{"gap-beyond-latest", "gap-range", func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 3 }},
		{"false-freshness", "gap-range", func(p *semanticWorkProbe) { p.reliability.FreshThroughGraphRevision = 2 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, resources, target := semanticWorkFixture()
			tc.mutate(p)
			result, err := awaitSemanticWork(context.Background(), p, resources, target)
			if err == nil || result.Reliability == nil || result.Reliability.RejectionReason != tc.reason || result.Reliability.Observed != (p.reliability != nil) {
				t.Fatalf("unsafe evidence accepted or diagnostics omitted: %+v %v", result.Reliability, err)
			}
			if p.reliability != nil && (result.LatestObservedGraphRevision != p.reliability.GetLatestObservedGraphRevision() || result.ConfiguredFrontierLimit != p.reliability.GetConfiguredFrontierLimit() || result.Reliability.RebuildRequired != p.reliability.GetRebuildRequired()) {
				t.Fatal("rejected response replaced actual fields with unpopulated zeros")
			}
			if p.listCalls != 1 || p.analyzeCalls != 0 || len(resources.EvidenceValues) != 1 {
				t.Fatal("invalid evidence was retried, mutated, or captured")
			}
			snapshot, err := readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2)
			if err == nil || snapshot.evidence == nil || snapshot.evidence.RejectionReason != tc.reason {
				t.Fatalf("fault reader bypassed shared fence: %+v %v", snapshot.evidence, err)
			}
		})
	}
}

func TestSemanticWorkCompleteCannotRetainGapOrRebuildFlag(t *testing.T) {
	for _, mutate := range []func(*semanticWorkProbe){
		func(p *semanticWorkProbe) { p.reliability.RebuildRequired = true },
		func(p *semanticWorkProbe) {
			p.reliability.RebuildRequired = true
			p.reliability.UnresolvedGapCount = 1
			p.reliability.OldestUnresolvedGraphRevision = 2
		},
		func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 1 },
	} {
		p, resources, target := semanticWorkFixture()
		completeSemanticWork(p)
		mutate(p)
		target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
		if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil {
			t.Fatal("completion accepted a gap/flag despite claimed freshness")
		}
	}
}

func TestSemanticWorkReliabilityDiagnosticsAreBoundedAndSecretSafe(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	private := strings.Repeat("private-response-body", 10000)
	p.reliability.SpaceId = private
	p.reliability.DegradedReason = private
	p.reliability.CapacityState = private
	p.reliability.Rebuild.State = private
	result, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err == nil || result.Reliability == nil || result.Reliability.ScopeMatches || result.Reliability.CapacityState != "unknown" || result.Reliability.RebuildState != "unknown" {
		t.Fatal("untrusted state/identity did not fail closed")
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 4096 || strings.Contains(string(raw), "private-response-body") {
		t.Fatal("rejected evidence leaked or retained unbounded raw diagnostics")
	}
	p, resources, target = semanticWorkFixture()
	p.reliabilityError = status.Error(codes.PermissionDenied, private)
	result, err = awaitSemanticWork(context.Background(), p, resources, target)
	if err == nil || result.Reliability == nil || result.Reliability.Observed || result.Reliability.RPCCode != "PermissionDenied" || result.Reliability.RejectionReason != "rpc-failure" || strings.Contains(err.Error(), "private-response-body") || p.listCalls != 1 {
		t.Fatalf("RPC failure diagnostics leaked or falsely claimed observed response: %+v %v", result.Reliability, err)
	}
	snapshot, err := readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2)
	if err == nil || snapshot.evidence == nil || snapshot.evidence.Observed || snapshot.evidence.RPCCode != "PermissionDenied" || strings.Contains(err.Error(), "private-response-body") {
		t.Fatal("fault RPC error leaked or fabricated evidence")
	}
}

func TestSemanticFaultCanObserveExpectedDegradedWorkWithoutPermittingRebuild(t *testing.T) {
	p, resources, _ := semanticWorkFixture()
	p.work.Status = "failed"
	p.work.AttemptCount = 1
	p.reliability.Degraded = true
	snapshot, err := readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2)
	if err != nil || !snapshot.evidence.Degraded || !snapshot.evidence.RebuildRequired {
		t.Fatalf("expected failure state was rejected: %+v %v", snapshot.evidence, err)
	}
	for _, state := range []string{"required", "running", "failed", "unknown"} {
		p.reliability.Rebuild.State = state
		if _, err := readPublicWork(context.Background(), p, resources, p.work.TargetNodeId, 2); err == nil {
			t.Fatal("provider failure authorized unsafe rebuild state")
		}
	}
}

func TestSemanticFaultSnapshotTimeoutRetainsLastSafeEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	proof := &SemanticWorkReliabilityEvidenceV1{SchemaVersion: 1, Observed: true, RebuildRequired: true, UnresolvedGapCount: 1, RebuildState: "completed"}
	result, err := awaitFaultSnapshot(ctx, time.Millisecond, func() (publicWorkSnapshot, error) { return publicWorkSnapshot{evidence: proof}, nil }, func(publicWorkSnapshot) bool { return false })
	if !errors.Is(err, context.DeadlineExceeded) || result.evidence != proof {
		t.Fatal("timeout discarded last observed safe diagnostics")
	}
}
