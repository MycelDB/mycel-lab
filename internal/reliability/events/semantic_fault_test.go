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
