package events

import (
	"context"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func faultSnapshots(t *testing.T) (publicWorkSnapshot, publicWorkSnapshot) {
	t.Helper()
	p, resources, target := semanticWorkFixture()
	proof, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err != nil {
		t.Fatal(err)
	}
	before := publicWorkSnapshot{work: proof.Work, raw: proto.Clone(p.work).(*adminv1.SemanticMaintenanceWorkItem), reliability: proto.Clone(p.reliability).(*adminv1.GetSemanticReliabilityStatusResponse)}
	failed := publicWorkSnapshot{work: proof.Work, raw: proto.Clone(p.work).(*adminv1.SemanticMaintenanceWorkItem), reliability: proto.Clone(p.reliability).(*adminv1.GetSemanticReliabilityStatusResponse)}
	failed.work.AttemptCount = 1
	failed.raw.AttemptCount = 1
	failed.raw.LastErrorCategory = "provider_error"
	failed.raw.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	failed.raw.NotBefore = time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
	return before, failed
}
func TestSemanticFaultRequiresFailureBeforeAnyRecovery(t *testing.T) {
	for _, mode := range []string{"retryable", "permanent", "deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			before, failed := faultSnapshots(t)
			code := codes.OK
			processed := &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: 1, FailedItems: 1}
			if mode == "permanent" {
				failed.work.Status = "failed"
			}
			if mode == "deadline" {
				code = codes.DeadlineExceeded
				failed.work.Status = "running"
				processed = nil
			}
			if mode == "cancel" {
				code = codes.Canceled
				failed.work.Status = "running"
				processed = nil
			}
			if err := validateFaultBoundary(before, failed, mode, processed, code); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSemanticFaultRejectsFalseFreshnessResetReplayAndUnsupportedEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*publicWorkSnapshot){
		"missing reliability":     func(s *publicWorkSnapshot) { s.reliability = nil },
		"missing work":            func(s *publicWorkSnapshot) { s.raw = nil },
		"false fresh":             func(s *publicWorkSnapshot) { s.reliability.FreshThroughGraphRevision = 2 },
		"gap missing":             func(s *publicWorkSnapshot) { s.reliability.UnresolvedGapCount = 0 },
		"oldest gap missing":      func(s *publicWorkSnapshot) { s.reliability.OldestUnresolvedGraphRevision = 0 },
		"new work ID":             func(s *publicWorkSnapshot) { s.work.WorkItemID = "00000000-0000-4000-8000-000000000999" },
		"generation changed":      func(s *publicWorkSnapshot) { s.work.SemanticGeneration++ },
		"work generation changed": func(s *publicWorkSnapshot) { s.work.WorkGeneration++ },
		"attempt reset":           func(s *publicWorkSnapshot) { s.work.AttemptCount = 0 },
		"attempt replay":          func(s *publicWorkSnapshot) { s.work.AttemptCount = 2 },
		"complete despite error":  func(s *publicWorkSnapshot) { s.work.Status = "complete" },
		"missing retry category":  func(s *publicWorkSnapshot) { s.raw.LastErrorCategory = "" },
		"missing retry delay":     func(s *publicWorkSnapshot) { s.raw.NotBefore = "" },
	} {
		t.Run(name, func(t *testing.T) {
			before, failed := faultSnapshots(t)
			mutate(&failed)
			if validateFaultBoundary(before, failed, "retryable", &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: 1, FailedItems: 1}, codes.OK) == nil {
				t.Fatal("false failure proof passed")
			}
		})
	}
}
func TestOperatorFaultRetryRequiresExactlyOneVersionedUpsertAndScopedResponse(t *testing.T) {
	_, failed := faultSnapshots(t)
	response := proto.Clone(failed.raw).(*adminv1.SemanticMaintenanceWorkItem)
	response.WorkGeneration++
	response.Status = "pending"
	response.NotBefore = ""
	response.LastErrorCategory = ""
	checkpoint, err := validateOperatorFaultRetry(failed.work, response)
	if err != nil || checkpoint.WorkGeneration != failed.work.WorkGeneration+1 || checkpoint.WorkItemID != failed.work.WorkItemID || checkpoint.AttemptCount != 1 {
		t.Fatalf("exact public retry response rejected: checkpoint=%+v error=%v", checkpoint, err)
	}
	for name, mutate := range map[string]func(*adminv1.SemanticMaintenanceWorkItem){
		"unchanged generation": func(item *adminv1.SemanticMaintenanceWorkItem) { item.WorkGeneration-- },
		"skipped generation":   func(item *adminv1.SemanticMaintenanceWorkItem) { item.WorkGeneration++ },
		"foreign work ID": func(item *adminv1.SemanticMaintenanceWorkItem) {
			item.WorkItemId = "00000000-0000-4000-8000-000000000999"
		},
		"foreign domain": func(item *adminv1.SemanticMaintenanceWorkItem) {
			item.DomainId = "00000000-0000-4000-8000-000000000999"
		},
		"changed revision": func(item *adminv1.SemanticMaintenanceWorkItem) { item.LastGraphRevision++ },
		"reset attempt":    func(item *adminv1.SemanticMaintenanceWorkItem) { item.AttemptCount = 0 },
		"replayed attempt": func(item *adminv1.SemanticMaintenanceWorkItem) { item.AttemptCount++ },
		"still failed":     func(item *adminv1.SemanticMaintenanceWorkItem) { item.Status = "failed" },
		"stale failure":    func(item *adminv1.SemanticMaintenanceWorkItem) { item.LastErrorCategory = "provider_rejected" },
		"stale safe error": func(item *adminv1.SemanticMaintenanceWorkItem) { item.LastErrorMessageSanitized = "old failure" },
		"not-before retained": func(item *adminv1.SemanticMaintenanceWorkItem) {
			item.NotBefore = time.Now().UTC().Format(time.RFC3339Nano)
		},
		"claim retained": func(item *adminv1.SemanticMaintenanceWorkItem) {
			item.ClaimedUntil = time.Now().UTC().Format(time.RFC3339Nano)
		},
	} {
		t.Run(name, func(t *testing.T) {
			item := proto.Clone(response).(*adminv1.SemanticMaintenanceWorkItem)
			mutate(item)
			if _, err := validateOperatorFaultRetry(failed.work, item); err == nil {
				t.Fatal("unsafe retry response was accepted")
			}
		})
	}
	if _, err := validateOperatorFaultRetry(failed.work, nil); err == nil {
		t.Fatal("missing retry response accepted")
	}
	failed.work.WorkGeneration = ^uint64(0)
	if _, err := validateOperatorFaultRetry(failed.work, response); err == nil {
		t.Fatal("overflowing retry generation accepted")
	}
}

func TestSemanticFaultRecoveryRequiresExactDispositionSpecificGeneration(t *testing.T) {
	before, failed := faultSnapshots(t)
	recovered := failed
	recovered.work = before.work
	recovered.work.Status = "complete"
	recovered.work.AttemptCount = 2
	recovered.reliability = proto.Clone(failed.reliability).(*adminv1.GetSemanticReliabilityStatusResponse)
	recovered.reliability.FreshThroughGraphRevision = before.work.LastGraphRevision
	recovered.reliability.UnresolvedGapCount = 0
	if err := validateFaultRecovery(before.work, recovered, "retryable", 0, before.work.LastGraphRevision); err != nil {
		t.Fatalf("ordinary backoff changed no work generation: %v", err)
	}
	recovered.work.WorkGeneration++
	if err := validateFaultRecovery(before.work, recovered, "permanent", recovered.work.WorkGeneration, before.work.LastGraphRevision); err != nil {
		t.Fatalf("single explicit retry advanced generation exactly once: %v", err)
	}
	for name, mutate := range map[string]func(*publicWorkSnapshot){
		"unchanged generation": func(s *publicWorkSnapshot) { s.work.WorkGeneration-- },
		"skipped generation":   func(s *publicWorkSnapshot) { s.work.WorkGeneration++ },
		"foreign work ID":      func(s *publicWorkSnapshot) { s.work.WorkItemID = "00000000-0000-4000-8000-000000000999" },
		"foreign scope":        func(s *publicWorkSnapshot) { s.work.SpaceID = "00000000-0000-4000-8000-000000000999" },
		"semantic generation":  func(s *publicWorkSnapshot) { s.work.SemanticGeneration++ },
		"replayed revision":    func(s *publicWorkSnapshot) { s.work.FirstGraphRevision++ },
		"replayed attempts":    func(s *publicWorkSnapshot) { s.work.AttemptCount++ },
		"not complete":         func(s *publicWorkSnapshot) { s.work.Status = "pending" },
		"false freshness":      func(s *publicWorkSnapshot) { s.reliability.FreshThroughGraphRevision-- },
		"unresolved gap":       func(s *publicWorkSnapshot) { s.reliability.UnresolvedGapCount++ },
		"degraded":             func(s *publicWorkSnapshot) { s.reliability.Degraded = true },
		"missing reliability":  func(s *publicWorkSnapshot) { s.reliability = nil },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := recovered
			candidate.reliability = proto.Clone(recovered.reliability).(*adminv1.GetSemanticReliabilityStatusResponse)
			mutate(&candidate)
			if err := validateFaultRecovery(before.work, candidate, "permanent", before.work.WorkGeneration+1, before.work.LastGraphRevision); err == nil {
				t.Fatal("unsafe recovery passed the exact operator-retry contract")
			}
		})
	}
	if err := validateFaultRecovery(before.work, recovered, "permanent", 0, before.work.LastGraphRevision); err == nil {
		t.Fatal("recovery without a witnessed operator retry accepted")
	}
	if err := validateFaultRecovery(before.work, recovered, "retryable", recovered.work.WorkGeneration, before.work.LastGraphRevision); err == nil {
		t.Fatal("ordinary automatic retry accepted an operator generation")
	}
}

func TestPublicFaultWorkReadIsScopedBoundedAndCapabilityChecked(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	if _, err := readPublicWork(context.Background(), p, resources, targetString(target, "nodeId"), 2); err != nil {
		t.Fatal(err)
	}
	p.work.WorkGeneration = 0
	if _, err := readPublicWork(context.Background(), p, resources, targetString(target, "nodeId"), 2); err == nil {
		t.Fatal("missing work capability passed")
	}
}
