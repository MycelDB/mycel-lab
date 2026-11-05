package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type semanticWorkProbe struct {
	adminv1.AdminSemanticMaintenanceServiceClient
	work                    *adminv1.SemanticMaintenanceWorkItem
	maintenance             *adminv1.GetSemanticMaintenanceStatusResponse
	reliability             *adminv1.GetSemanticReliabilityStatusResponse
	analyzeCalls, listCalls int
	analyzeError            error
	reliabilityError        error
	list                    func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error)
}

func (p *semanticWorkProbe) AnalyzeSemanticDirtyWork(context.Context, *adminv1.AnalyzeSemanticDirtyWorkRequest, ...grpc.CallOption) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	p.analyzeCalls++
	if p.analyzeError != nil {
		return nil, p.analyzeError
	}
	return &adminv1.AnalyzeSemanticDirtyWorkResponse{ProcessedEvents: 1, EnqueuedItems: 1}, nil
}
func (p *semanticWorkProbe) ListSemanticMaintenanceWork(ctx context.Context, req *adminv1.ListSemanticMaintenanceWorkRequest, _ ...grpc.CallOption) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
	p.listCalls++
	if p.list != nil {
		return p.list(ctx, req)
	}
	return &adminv1.ListSemanticMaintenanceWorkResponse{Items: []*adminv1.SemanticMaintenanceWorkItem{p.work}}, nil
}
func (p *semanticWorkProbe) GetSemanticMaintenanceStatus(context.Context, *adminv1.GetSemanticMaintenanceStatusRequest, ...grpc.CallOption) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
	return p.maintenance, nil
}
func (p *semanticWorkProbe) GetSemanticReliabilityStatus(context.Context, *adminv1.GetSemanticReliabilityStatusRequest, ...grpc.CallOption) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
	return p.reliability, p.reliabilityError
}

func semanticWorkFixture() (*semanticWorkProbe, *provision.SemanticResources, map[string]any) {
	resources := &provision.SemanticResources{SpaceID: "00000000-0000-4000-8000-000000000101", DomainID: "00000000-0000-4000-8000-000000000102", SemanticRuleID: "00000000-0000-4000-8000-000000000103", EmbeddingBindingKey: "search", MaintenanceTimeout: 100 * time.Millisecond, MaintenancePollInterval: time.Millisecond, MaintenanceAnalyzeLimit: 16, EvidenceValues: map[string]string{"committed": "2"}}
	work := &adminv1.SemanticMaintenanceWorkItem{WorkItemId: "00000000-0000-4000-8000-000000000105", SpaceId: resources.SpaceID, DomainId: resources.DomainID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, TargetNodeId: "00000000-0000-4000-8000-000000000104", Action: "refresh", Status: "pending", FirstGraphRevision: 2, LastGraphRevision: 2, WorkGeneration: 3}
	probe := &semanticWorkProbe{work: work, maintenance: &adminv1.GetSemanticMaintenanceStatusResponse{QueueDepthPending: 1, ThrottleState: "ok"}, reliability: &adminv1.GetSemanticReliabilityStatusResponse{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, EmbeddingBindingKey: resources.EmbeddingBindingKey, LatestObservedGraphRevision: 2, AnalyzedThroughGraphRevision: 2, FreshThroughGraphRevision: 1, UnresolvedGapCount: 1, OldestUnresolvedGraphRevision: 2, RebuildRequired: true, Rebuild: &adminv1.SemanticRebuildStatus{State: "completed"}, SemanticGeneration: 1, ConfiguredFrontierLimit: 1024, ConfiguredGapIntervalLimit: 1024, ConfiguredRevisionSpanLimit: 1024, ConfiguredSerializedByteLimit: 1048576, CapacityState: "ok"}}
	return probe, resources, map[string]any{"nodeId": work.TargetNodeId, "expectedStatus": "pending", "expectRevisionFrom": "committed", "expectBackgroundMaintenanceEnabled": false, "expectedAttemptCount": 0}
}

func completeSemanticWork(p *semanticWorkProbe) {
	p.work.Status, p.work.AttemptCount = "complete", 1
	p.maintenance.QueueDepthPending = 0
	p.reliability.FreshThroughGraphRevision, p.reliability.UnresolvedGapCount, p.reliability.OldestUnresolvedGraphRevision = 2, 0, 0
	p.reliability.RebuildRequired = false
}

func TestSemanticWorkCheckpointSurvivesPendingAndCompletedRestart(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	target["analyze"], target["captureWorkAs"] = true, "before-restart"
	before, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err != nil || before.AnalyzeCalls != 1 || p.analyzeCalls != 1 {
		t.Fatalf("capture pending work: %+v, %v", before, err)
	}
	if before.Work.SemanticGeneration != 1 || before.AnalyzedThroughGraphRevision != 2 || before.UnresolvedGapCount != 1 || before.OldestUnresolvedGraphRevision != 2 || before.CapacityState != "ok" || before.ConfiguredFrontierLimit != 1024 || before.ConfiguredGapIntervalLimit != 1024 || before.ConfiguredRevisionSpanLimit != 1024 || before.ConfiguredSerializedByteLimit != 1048576 {
		t.Fatalf("accepted evidence omitted its public frontier/capacity proof: %+v", before)
	}
	// Rehydrate the retained capture instead of trusting an in-memory pointer.
	raw, _ := json.Marshal(resources.EvidenceValues)
	resources.EvidenceValues = nil
	if err := json.Unmarshal(raw, &resources.EvidenceValues); err != nil {
		t.Fatal(err)
	}
	delete(target, "analyze")
	delete(target, "captureWorkAs")
	target["expectWorkFrom"] = "before-restart"
	after, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err != nil || after.Work != before.Work || after.AnalyzeCalls != 0 || p.analyzeCalls != 1 {
		t.Fatalf("read-only pending preservation: %+v, %v", after, err)
	}
	completeSemanticWork(p)
	target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
	target["captureWorkAs"] = "completed"
	if _, err := awaitSemanticWork(context.Background(), p, resources, target); err != nil {
		t.Fatal(err)
	}
	delete(target, "captureWorkAs")
	target["expectWorkFrom"] = "completed"
	if result, err := awaitSemanticWork(context.Background(), p, resources, target); err != nil || result.AnalyzeCalls != 0 || p.analyzeCalls != 1 {
		t.Fatalf("read-only completed preservation: %+v, %v", result, err)
	}
}

func TestSemanticWorkRejectsMissingContradictoryOrUnsupportedEvidence(t *testing.T) {
	cases := map[string]func(*semanticWorkProbe){
		"nil work":                               func(p *semanticWorkProbe) { p.work = nil },
		"foreign scope":                          func(p *semanticWorkProbe) { p.work.SpaceId = "00000000-0000-4000-8000-000000000106" },
		"foreign domain":                         func(p *semanticWorkProbe) { p.work.DomainId = "00000000-0000-4000-8000-000000000106" },
		"missing revision capability":            func(p *semanticWorkProbe) { p.work.FirstGraphRevision = 0 },
		"missing generation capability":          func(p *semanticWorkProbe) { p.work.WorkGeneration = 0 },
		"wrong committed revision":               func(p *semanticWorkProbe) { p.work.LastGraphRevision = 3 },
		"inverted revision range":                func(p *semanticWorkProbe) { p.work.FirstGraphRevision = 3 },
		"invalid identity":                       func(p *semanticWorkProbe) { p.work.WorkItemId = "not-a-work-id" },
		"unsupported status":                     func(p *semanticWorkProbe) { p.work.Status = "ready-ish" },
		"unsupported action":                     func(p *semanticWorkProbe) { p.work.Action = "repair" },
		"negative attempt count":                 func(p *semanticWorkProbe) { p.work.AttemptCount = -1 },
		"provider attempt before restart":        func(p *semanticWorkProbe) { p.work.AttemptCount = 1 },
		"summary zeros are not pending evidence": func(p *semanticWorkProbe) { p.maintenance.QueueDepthPending = 0 },
		"negative queue":                         func(p *semanticWorkProbe) { p.maintenance.QueueDepthRunning = -1 },
		"unexpected background scheduling":       func(p *semanticWorkProbe) { p.maintenance.Enabled = true },
		"degraded maintenance":                   func(p *semanticWorkProbe) { p.maintenance.Degraded = true },
		"permanent failure":                      func(p *semanticWorkProbe) { p.maintenance.QueueDepthFailedPermanent = 1 },
		"missing maintenance":                    func(p *semanticWorkProbe) { p.maintenance = nil },
		"missing reliability":                    func(p *semanticWorkProbe) { p.reliability = nil },
		"unclassified commit":                    func(p *semanticWorkProbe) { p.reliability.AnalyzedThroughGraphRevision = 1 },
		"missing oldest gap":                     func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 0 },
		"gap beyond commit":                      func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 3 },
		"gap behind fresh":                       func(p *semanticWorkProbe) { p.reliability.OldestUnresolvedGraphRevision = 1 },
		"foreign reliability scope":              func(p *semanticWorkProbe) { p.reliability.SemanticRuleId = "00000000-0000-4000-8000-000000000106" },
		"missing semantic generation":            func(p *semanticWorkProbe) { p.reliability.SemanticGeneration = 0 },
		"running rebuild":                        func(p *semanticWorkProbe) { p.reliability.Rebuild = &adminv1.SemanticRebuildStatus{State: "running"} },
		"unknown rebuild":                        func(p *semanticWorkProbe) { p.reliability.Rebuild = &adminv1.SemanticRebuildStatus{State: "unknown"} },
		"missing capacity evidence":              func(p *semanticWorkProbe) { p.reliability.ConfiguredFrontierLimit = 0 },
		"unknown capacity state":                 func(p *semanticWorkProbe) { p.reliability.CapacityState = "unknown" },
		"degraded reliability":                   func(p *semanticWorkProbe) { p.reliability.Degraded = true },
		"rebuild required":                       func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "required" },
		"failed rebuild":                         func(p *semanticWorkProbe) { p.reliability.Rebuild.State = "failed" },
		"missing rebuild status":                 func(p *semanticWorkProbe) { p.reliability.Rebuild = nil },
		"gap falsely clears rebuild flag":        func(p *semanticWorkProbe) { p.reliability.RebuildRequired = false },
		"pending falsely fresh":                  func(p *semanticWorkProbe) { p.reliability.FreshThroughGraphRevision = 2 },
		"pending range falsely partly fresh":     func(p *semanticWorkProbe) { p.work.FirstGraphRevision = 1 },
		"missing unresolved gap":                 func(p *semanticWorkProbe) { p.reliability.UnresolvedGapCount = 0 },
		"future freshness":                       func(p *semanticWorkProbe) { p.reliability.FreshThroughGraphRevision = 3 },
		"future analyzer":                        func(p *semanticWorkProbe) { p.reliability.AnalyzedThroughGraphRevision = 3 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p, resources, target := semanticWorkFixture()
			mutate(p)
			if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil {
				t.Fatal("unsupported or contradictory evidence passed")
			}
			if p.listCalls != 1 {
				t.Fatalf("assertion failure was retried: %d", p.listCalls)
			}
		})
	}
}

func TestSemanticWorkRejectsDuplicatesAndTruncatedLists(t *testing.T) {
	for _, count := range []int{2, maxSemanticWorkItems + 1} {
		p, resources, target := semanticWorkFixture()
		p.list = func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
			items := make([]*adminv1.SemanticMaintenanceWorkItem, count)
			for i := range items {
				items[i] = proto.Clone(p.work).(*adminv1.SemanticMaintenanceWorkItem)
			}
			return &adminv1.ListSemanticMaintenanceWorkResponse{Items: items}, nil
		}
		if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil {
			t.Fatalf("ambiguous/incomplete listing of %d items passed", count)
		}
	}
}

func TestSemanticWorkRejectsCheckpointIdentityChangesAndAttemptReplay(t *testing.T) {
	for name, mutate := range map[string]func(*semanticWorkProbe){
		"work ID":             func(p *semanticWorkProbe) { p.work.WorkItemId = "00000000-0000-4000-8000-000000000106" },
		"generation":          func(p *semanticWorkProbe) { p.work.WorkGeneration++ },
		"semantic generation": func(p *semanticWorkProbe) { p.reliability.SemanticGeneration++ },
		"revision range":      func(p *semanticWorkProbe) { p.work.FirstGraphRevision = 1 },
		"attempt replay":      func(p *semanticWorkProbe) { p.work.AttemptCount++ },
	} {
		t.Run(name, func(t *testing.T) {
			p, resources, target := semanticWorkFixture()
			target["captureWorkAs"] = "before"
			if _, err := awaitSemanticWork(context.Background(), p, resources, target); err != nil {
				t.Fatal(err)
			}
			delete(target, "captureWorkAs")
			delete(target, "expectedAttemptCount")
			target["expectWorkFrom"] = "before"
			mutate(p)
			if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil {
				t.Fatal("work was reset/replaced/replayed")
			}
		})
	}
}

func TestSemanticWorkCapturesAreVersionedCanonicalAndImmutable(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	target["captureWorkAs"] = "before"
	if _, err := awaitSemanticWork(context.Background(), p, resources, target); err != nil {
		t.Fatal(err)
	}
	raw := resources.EvidenceValues[semanticWorkCheckpointPrefix+"before"]
	for name, broken := range map[string]string{
		"missing": "", "oversized": strings.Repeat("x", maxSemanticWorkCheckpointBytes+1), "malformed": "{",
		"unknown version": strings.Replace(raw, `"schemaVersion":1`, `"schemaVersion":2`, 1),
		"duplicate field": strings.Replace(raw, `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1),
		"unknown field":   strings.Replace(raw, `"schemaVersion":1`, `"schemaVersion":1,"ignored":true`, 1),
		"noncanonical":    raw + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			resources.EvidenceValues[semanticWorkCheckpointPrefix+"before"] = broken
			if _, err := loadSemanticWorkCheckpoint(resources, "before"); err == nil {
				t.Fatal("invalid capture passed")
			}
		})
	}
	resources.EvidenceValues[semanticWorkCheckpointPrefix+"before"] = raw
	target["analyze"] = true
	beforeCalls := p.analyzeCalls
	if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil || p.analyzeCalls != beforeCalls {
		t.Fatal("capture overwrite performed an analysis mutation")
	}
}

func TestSemanticWorkSanitizesPlainRPCErrors(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	p.list = func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
		return nil, errors.New("sensitive raw transport details")
	}
	if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil || p.listCalls != 1 || strings.Contains(err.Error(), "sensitive raw") || !strings.Contains(err.Error(), "Unknown") {
		t.Fatalf("plain RPC error was leaked or retried: calls=%d err=%v", p.listCalls, err)
	}
}

func TestSemanticWorkCanAwaitCompletionWithoutReconstructingWork(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
	p.list = func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
		if p.listCalls == 2 {
			completeSemanticWork(p)
		}
		return &adminv1.ListSemanticMaintenanceWorkResponse{Items: []*adminv1.SemanticMaintenanceWorkItem{p.work}}, nil
	}
	result, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err != nil || result.Observations != 2 || result.AnalyzeCalls != 0 || p.analyzeCalls != 0 {
		t.Fatalf("read-only completion wait failed: result=%+v err=%v", result, err)
	}
}

func TestSemanticWorkDoesNotRetryAmbiguousAnalysis(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	target["analyze"], target["captureWorkAs"] = true, "ambiguous"
	p.analyzeError = status.Error(codes.Unavailable, "sensitive raw provider message")
	_, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err == nil || p.analyzeCalls != 1 || p.listCalls != 0 || strings.Contains(err.Error(), "sensitive raw") {
		t.Fatalf("ambiguous mutation was retried or exposed raw error: calls=%d lists=%d err=%v", p.analyzeCalls, p.listCalls, err)
	}
	if _, captured := resources.EvidenceValues[semanticWorkCheckpointPrefix+"ambiguous"]; captured {
		t.Fatal("failed analysis produced accepted evidence")
	}
}

func TestSemanticWorkRejectsCompletionWithoutAttemptEvidence(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	completeSemanticWork(p)
	p.work.AttemptCount = 0
	target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
	if _, err := awaitSemanticWork(context.Background(), p, resources, target); err == nil || p.listCalls != 1 {
		t.Fatal("incomplete completion evidence passed or was retried")
	}
}

func TestSemanticWorkTimeoutBoundsRPCAndParentDeadline(t *testing.T) {
	for _, parentBound := range []bool{false, true} {
		p, resources, target := semanticWorkFixture()
		target["timeout"], target["pollInterval"] = "20ms", "1ms"
		ctx := context.Background()
		if parentBound {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			target["timeout"] = "1s"
		}
		p.list = func(ctx context.Context, _ *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		started := time.Now()
		_, err := awaitSemanticWork(ctx, p, resources, target)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("RPC escaped operation/parent deadline: %v", err)
		}
	}
}

func TestSemanticWorkRetriesOnlyTransientReadiness(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.PermissionDenied, codes.Unimplemented, codes.Internal, codes.Unknown} {
		p, resources, target := semanticWorkFixture()
		p.list = func(_ context.Context, req *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
			if req.GetLimit() != maxSemanticWorkItems+1 || req.GetStatus() != "" || req.GetSpaceId() != resources.SpaceID || req.GetSemanticRuleId() != resources.SemanticRuleID || req.GetEmbeddingBindingKey() != resources.EmbeddingBindingKey {
				t.Fatal("work query was not scoped/bounded")
			}
			if p.listCalls == 1 {
				return nil, status.Error(code, "sensitive raw provider message")
			}
			return &adminv1.ListSemanticMaintenanceWorkResponse{Items: []*adminv1.SemanticMaintenanceWorkItem{p.work}}, nil
		}
		_, err := awaitSemanticWork(context.Background(), p, resources, target)
		if code == codes.Unavailable {
			if err != nil || p.listCalls != 2 {
				t.Fatalf("transient retry: %v / %d", err, p.listCalls)
			}
		} else if err == nil || p.listCalls != 1 || strings.Contains(err.Error(), "sensitive raw") {
			t.Fatalf("rejected RPC retried or exposed raw error: %v / %d", err, p.listCalls)
		}
		if p.analyzeCalls != 0 {
			t.Fatal("read-only checkpoint unexpectedly analyzed work")
		}
	}
}
