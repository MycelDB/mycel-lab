package events

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const healthNoLeader = "semantic raft partition for space 00000000-0000-4000-8000-000000000101 has no leader"

func TestMaintenanceHealthRPCDeadlineIdentityBeforeParentTimer(t *testing.T) {
	// Deterministic ordering: the RPC timer has fired, the parent has not.
	err := maintenanceRPCFailure(context.Background(), "fixture", status.Error(codes.DeadlineExceeded, "private details"))
	if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "private") {
		t.Fatal("RPC deadline identity was lost before the parent timer fired")
	}
}

func TestMaintenanceHealthClassifiesOnlyExactBoundedOwnershipErrors(t *testing.T) {
	for _, reason := range []string{healthNoLeader, "semantic operation leadership changed", "semantic raft partition leader changed to node 2", "rpc error: code = Unavailable desc = semantic raft partition group 9 has no leader", healthNoLeader + "\nsemantic operation leadership changed"} {
		class, known := maintenanceCauseClass(true, reason)
		if !known || class == "unclassified" {
			t.Fatalf("released transient not classified: %q", class)
		}
	}
	for _, reason := range []string{"", "private provider failure", healthNoLeader + " private suffix", "private prefix " + healthNoLeader, "rpc error: code = PermissionDenied desc = " + healthNoLeader, "semantic raft partition leader changed to node 0", healthNoLeader + "\nprivate details", strings.Repeat(healthNoLeader+"\n", 17), strings.Repeat("x", 4097), "semantic raft partition group 9 is not available"} {
		class, known := maintenanceCauseClass(true, reason)
		if known || class != "unclassified" {
			t.Fatal("unknown/auth/provider/malformed reason became a retryable ownership condition")
		}
	}
	if class, known := maintenanceCauseClass(false, healthNoLeader); class != "inconsistent" || known {
		t.Fatal("clear health retained an unexplained error")
	}
}

type maintenanceHealthProbe struct {
	*boundedMaintenanceProbe
	adminv1.UnimplementedAdminSemanticServiceServer
	ruleCalls                                                 int
	rule                                                      func(context.Context, int) (*adminv1.GetSemanticRuleResponse, error)
	mu                                                        sync.Mutex
	analyzeCalls, processCalls, statusCalls, reliabilityCalls int
	analyzeError, processError                                error
	node                                                      func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error)
	reliability                                               func(context.Context, int) (*adminv1.GetSemanticReliabilityStatusResponse, error)
}

func (p *maintenanceHealthProbe) AnalyzeSemanticDirtyWork(ctx context.Context, _ *adminv1.AnalyzeSemanticDirtyWorkRequest) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.analyzeCalls++
	return &adminv1.AnalyzeSemanticDirtyWorkResponse{}, p.analyzeError
}
func (p *maintenanceHealthProbe) ProcessSemanticDirtyWork(ctx context.Context, _ *adminv1.ProcessSemanticDirtyWorkRequest) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processCalls++
	return &adminv1.ProcessSemanticDirtyWorkResponse{}, p.processError
}
func (p *maintenanceHealthProbe) GetSemanticMaintenanceStatus(ctx context.Context, _ *adminv1.GetSemanticMaintenanceStatusRequest) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statusCalls++
	return p.node(p.statusCalls)
}
func (p *maintenanceHealthProbe) GetSemanticReliabilityStatus(ctx context.Context, _ *adminv1.GetSemanticReliabilityStatusRequest) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.reliabilityCalls++
	call := p.reliabilityCalls
	p.mu.Unlock()
	return p.reliability(ctx, call)
}
func clearMaintenanceNode() *adminv1.GetSemanticMaintenanceStatusResponse {
	return &adminv1.GetSemanticMaintenanceStatusResponse{Enabled: true, ThrottleState: "ok", AnalyzerRuns: 2, WorkerRuns: 2, LastWorkerSuccessAt: "2026-11-05T11:49:39Z"}
}
func transientMaintenanceNode() *adminv1.GetSemanticMaintenanceStatusResponse {
	r := clearMaintenanceNode()
	r.Degraded = true
	r.DegradedReason = healthNoLeader
	r.LastWorkerErrorAt = "2026-11-05T11:49:34Z"
	return r
}
func healthProbeFixture(t *testing.T) (context.Context, *mycel.AdminClient, *provision.SemanticResources, *maintenanceHealthProbe, *adminv1.GetSemanticReliabilityStatusResponse) {
	t.Helper()
	work, resources, _ := semanticWorkFixture()
	completeSemanticWork(work)
	proof := work.reliability
	p := &maintenanceHealthProbe{boundedMaintenanceProbe: &boundedMaintenanceProbe{}}
	p.rule = func(context.Context, int) (*adminv1.GetSemanticRuleResponse, error) {
		return &adminv1.GetSemanticRuleResponse{Rule: &adminv1.SemanticGenerationRule{SpaceId: resources.SpaceID, DomainId: resources.DomainID, SemanticRuleId: resources.SemanticRuleID, Enabled: true, Embeddings: []*adminv1.SemanticEmbeddingBinding{{Key: resources.EmbeddingBindingKey, Enabled: true}}}}, nil
	}
	p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) { return clearMaintenanceNode(), nil }
	p.reliability = func(context.Context, int) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
		return proto.Clone(proof).(*adminv1.GetSemanticReliabilityStatusResponse), nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(server, p)
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(server, p)
	adminv1.RegisterAdminSemanticServiceServer(server, p)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: listener.Addr().String(), Username: "fixture-admin", Password: "fixture-password"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	resources.MaintenanceTimeout = time.Second
	resources.MaintenanceProcessLimit = 64
	return ctx, admin, resources, p, proof
}

func TestMaintenanceHealthWaitsReadOnlyButRequiresBothSignalsToClear(t *testing.T) {
	ctx, admin, resources, p, _ := healthProbeFixture(t)
	p.node = func(n int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
		if n == 1 {
			return transientMaintenanceNode(), nil
		}
		return clearMaintenanceNode(), nil
	}
	result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err != nil || !result.Converged || len(result.Observations) != 2 {
		t.Fatalf("safe finite convergence failed: %+v %v", result, err)
	}
	first, last := result.Observations[0], result.Observations[1]
	if !first.Degraded || !first.NodeHealth.RecognizedTransient || first.NodeHealth.CauseClass != "raft-no-leader" || first.NodeHealth.LastWorkerErrorAt == "" || first.Reliability.FreshThroughGraphRevision != 2 || last.Degraded || !last.ReadOnly || last.Reliability.UnresolvedGapCount != 0 {
		t.Fatal("node background and current scope proof were conflated or discarded")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.analyzeCalls != 1 || p.processCalls != 1 || p.statusCalls != 2 || p.reliabilityCalls != 2 {
		t.Fatal("historical-health wait repeated successful maintenance mutations")
	}
}

func TestMaintenanceHealthPersistentKnownErrorTimesOutWithoutPassing(t *testing.T) {
	ctx, admin, resources, p, _ := healthProbeFixture(t)
	resources.MaintenanceTimeout = 100 * time.Millisecond
	p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
		return transientMaintenanceNode(), nil
	}
	result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if !errors.Is(err, context.DeadlineExceeded) || result.Converged || len(result.Observations) == 0 {
		t.Fatalf("persistent degraded signal passed or lost bounded evidence: %+v %v", result, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.analyzeCalls != 1 || p.processCalls != 1 || p.statusCalls > maxSemanticMaintenanceObservations {
		t.Fatal("historical wait retried mutations or escaped bounds")
	}
}

func TestMaintenanceHealthDoesNotConvergeOnQueueZerosOrStaleCertificate(t *testing.T) {
	ctx, admin, resources, _, proof := healthProbeFixture(t)
	resources.MaintenanceTimeout = 100 * time.Millisecond
	proof.UnresolvedGapCount = 1
	proof.OldestUnresolvedGraphRevision = 2
	proof.FreshThroughGraphRevision = 1
	proof.RebuildRequired = true
	result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err == nil || result.Converged {
		t.Fatal("queue zeros bypassed unresolved scoped work")
	}
}

func TestMaintenanceHealthUnknownOrUnsafeNodeStatusFailsImmediatelyAndSafely(t *testing.T) {
	for name, mutate := range map[string]func(*adminv1.GetSemanticMaintenanceStatusResponse){
		"unknown": func(r *adminv1.GetSemanticMaintenanceStatusResponse) {
			r.Degraded = true
			r.DegradedReason = "private provider failure"
		},
		"negative-counters": func(r *adminv1.GetSemanticMaintenanceStatusResponse) { r.WorkerRuns = -1 },
		"negative-queue":    func(r *adminv1.GetSemanticMaintenanceStatusResponse) { r.QueueDepthRunning = -1 },
		"permanent-work":    func(r *adminv1.GetSemanticMaintenanceStatusResponse) { r.QueueDepthFailedPermanent = 1 },
		"missing-throttle":  func(r *adminv1.GetSemanticMaintenanceStatusResponse) { r.ThrottleState = "" },
		"disabled-degraded": func(r *adminv1.GetSemanticMaintenanceStatusResponse) {
			r.Degraded = true
			r.DegradedReason = healthNoLeader
			r.Enabled = false
		},
		"private-timestamp": func(r *adminv1.GetSemanticMaintenanceStatusResponse) {
			r.LastWorkerErrorAt = strings.Repeat("private timestamp", 1000)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, admin, resources, p, _ := healthProbeFixture(t)
			p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
				r := clearMaintenanceNode()
				mutate(r)
				return r, nil
			}
			result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
			raw, _ := json.Marshal(result)
			if err == nil || result.Converged || len(result.Observations) != 1 || strings.Contains(string(raw), "private") || strings.Contains(err.Error(), "private") {
				t.Fatalf("unsafe node metadata accepted/leaked/retried: %+v %v", result, err)
			}
			if result.Observations[0].Reliability == nil {
				t.Fatal("node rejection discarded current scoped evidence")
			}
		})
	}
}

func TestMaintenanceHealthScopedFailuresCannotHideBehindTransientNodeHistory(t *testing.T) {
	for name, mutate := range map[string]func(*adminv1.GetSemanticReliabilityStatusResponse){
		"foreign-scope":   func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.SpaceId = "foreign" },
		"zero-generation": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.SemanticGeneration = 0 },
		"capacity":        func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.CapacityState = "exhausted" },
		"missing-limits":  func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.ConfiguredFrontierLimit = 0 },
		"missing-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Rebuild = nil },
		"required-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.Rebuild.State = "required"
			r.RebuildRequired = true
		},
		"failed-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.Rebuild.State = "failed"
			r.RebuildRequired = true
		},
		"running-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.Rebuild.State = "running"
			r.RebuildRequired = true
		},
		"current-degradation": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Degraded = true },
		"false-freshness": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.UnresolvedGapCount = 1
			r.OldestUnresolvedGraphRevision = 2
			r.RebuildRequired = true
		},
		"future-frontier": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.FreshThroughGraphRevision = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, admin, resources, p, proof := healthProbeFixture(t)
			mutate(proof)
			p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
				return transientMaintenanceNode(), nil
			}
			result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
			if err == nil || result.Converged || len(result.Observations) != 1 || result.Observations[0].Reliability == nil {
				t.Fatal("unsafe current scope was treated as historical health")
			}
		})
	}
}

func TestMaintenanceHealthRejectsGenerationAndFrontierRegressionDuringReadOnlyWait(t *testing.T) {
	for _, generation := range []bool{false, true} {
		ctx, admin, resources, p, base := healthProbeFixture(t)
		p.node = func(n int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
			if n == 1 {
				return transientMaintenanceNode(), nil
			}
			return clearMaintenanceNode(), nil
		}
		p.reliability = func(_ context.Context, n int) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
			r := proto.Clone(base).(*adminv1.GetSemanticReliabilityStatusResponse)
			if n > 1 {
				if generation {
					r.SemanticGeneration++
				} else {
					r.LatestObservedGraphRevision = 1
					r.AnalyzedThroughGraphRevision = 1
					r.FreshThroughGraphRevision = 1
				}
			}
			return r, nil
		}
		result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
		if err == nil || result.Converged || len(result.Observations) != 2 {
			t.Fatal("read-only wait reset the generation/frontier")
		}
	}
}

func TestMaintenanceHealthNeverResubmitsFailedOrAmbiguousRPCs(t *testing.T) {
	for _, stage := range []string{"analyze", "process", "status", "reliability"} {
		t.Run(stage, func(t *testing.T) {
			ctx, admin, resources, p, _ := healthProbeFixture(t)
			failure := status.Error(codes.Unavailable, "private ambiguous RPC details")
			switch stage {
			case "analyze":
				p.analyzeError = failure
			case "process":
				p.processError = failure
			case "status":
				p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) { return nil, failure }
			case "reliability":
				p.reliability = func(context.Context, int) (*adminv1.GetSemanticReliabilityStatusResponse, error) { return nil, failure }
			}
			result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
			if err == nil || result.Converged || strings.Contains(err.Error(), "private") {
				t.Fatal("ambiguous failure passed or leaked")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.analyzeCalls != 1 || p.processCalls > 1 || p.statusCalls > 1 || p.reliabilityCalls > 1 {
				t.Fatal("failed RPC was retried")
			}
		})
	}
}

func TestMaintenanceHealthReliabilityRPCIsOperationBounded(t *testing.T) {
	ctx, admin, resources, p, _ := healthProbeFixture(t)
	resources.MaintenanceTimeout = 100 * time.Millisecond
	p.reliability = func(ctx context.Context, _ int) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
		<-ctx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "private details")
	}
	started := time.Now()
	result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err == nil || result.Converged || time.Since(started) > time.Second || len(result.Observations) != 1 || result.Observations[0].Reliability == nil || result.Observations[0].Reliability.Observed {
		t.Fatal("scoped status escaped its deadline or fabricated response evidence")
	}
}

func TestSemanticWorkReadOnlyPreservationWaitsForKnownNodeHistoryWithoutCapturingEarly(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	p.maintenance.Enabled = true
	target["expectBackgroundMaintenanceEnabled"] = true
	p.maintenance.Degraded = true
	p.maintenance.DegradedReason = healthNoLeader
	target["captureWorkAs"] = "after-health-clears"
	p.list = func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
		if p.listCalls == 2 {
			if _, exists := resources.EvidenceValues[semanticWorkCheckpointPrefix+"after-health-clears"]; exists {
				t.Fatal("captured while node still degraded")
			}
			p.maintenance.Degraded = false
			p.maintenance.DegradedReason = ""
		}
		return &adminv1.ListSemanticMaintenanceWorkResponse{Items: []*adminv1.SemanticMaintenanceWorkItem{p.work}}, nil
	}
	result, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err != nil || result.Observations != 2 || result.NodeHealth.Degraded || p.analyzeCalls != 0 || len(result.NodeHealthHistory) != 2 || !result.NodeHealthHistory[0].Degraded {
		t.Fatalf("read-only health convergence failed: %+v %v", result, err)
	}
}

func TestSemanticWorkPinsCertificateAndStopsAnalysisDuringNodeHealthWait(t *testing.T) {
	for _, drift := range []bool{false, true} {
		p, resources, target := semanticWorkFixture()
		p.maintenance.Enabled, p.maintenance.Degraded = true, true
		p.maintenance.DegradedReason = healthNoLeader
		target["expectBackgroundMaintenanceEnabled"], target["analyze"] = true, true
		target["captureWorkAs"] = "health-pinned"
		p.list = func(context.Context, *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
			if p.listCalls == 2 {
				p.maintenance.Degraded, p.maintenance.DegradedReason = false, ""
				if drift {
					p.reliability.SemanticGeneration++
				}
			}
			return &adminv1.ListSemanticMaintenanceWorkResponse{Items: []*adminv1.SemanticMaintenanceWorkItem{p.work}}, nil
		}
		result, err := awaitSemanticWork(context.Background(), p, resources, target)
		if (err != nil) != drift || result.Observations != 2 || p.analyzeCalls != 1 {
			t.Fatalf("health wait resubmitted analysis or lost certificate fence: %+v %v", result, err)
		}
		_, captured := resources.EvidenceValues[semanticWorkCheckpointPrefix+"health-pinned"]
		if captured == drift {
			t.Fatal("checkpoint captured an invalid generation or was not captured after clear health")
		}
	}
}

func TestSemanticWorkUnknownNodeHealthFailsBeforeWaitingForCompletion(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	p.maintenance.Degraded, p.maintenance.DegradedReason = true, "private unknown failure"
	target["expectedStatus"], target["expectedAttemptCount"] = "complete", 1
	result, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err == nil || result.Observations != 1 || strings.Contains(err.Error(), "private") || result.NodeHealth.CauseClass != "unclassified" {
		t.Fatal("unknown health was polled as ordinary incomplete work")
	}
}

func TestSemanticWorkNodeHealthHistoryIsBounded(t *testing.T) {
	p, resources, target := semanticWorkFixture()
	resources.MaintenanceTimeout = 5 * time.Second
	p.maintenance.Enabled, p.maintenance.Degraded = true, true
	p.maintenance.DegradedReason = healthNoLeader
	target["expectBackgroundMaintenanceEnabled"], target["pollInterval"] = true, "1ns"
	result, err := awaitSemanticWork(context.Background(), p, resources, target)
	if err == nil || result.Observations != maxSemanticMaintenanceObservations || len(result.NodeHealthHistory) != maxSemanticMaintenanceObservations || p.analyzeCalls != 0 {
		t.Fatal("node health history escaped its fixed read-only observation bound")
	}
}

func TestMaintenanceHealthNewScopedLagResumesNormalMaintenance(t *testing.T) {
	ctx, admin, resources, p, base := healthProbeFixture(t)
	p.node = func(n int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
		if n == 1 {
			return transientMaintenanceNode(), nil
		}
		return clearMaintenanceNode(), nil
	}
	p.reliability = func(_ context.Context, n int) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
		r := proto.Clone(base).(*adminv1.GetSemanticReliabilityStatusResponse)
		if n > 1 {
			r.LatestObservedGraphRevision, r.AnalyzedThroughGraphRevision = 3, 3
		}
		if n == 2 {
			r.UnresolvedGapCount, r.OldestUnresolvedGraphRevision, r.RebuildRequired = 1, 3, true
		}
		if n > 2 {
			r.FreshThroughGraphRevision = 3
		}
		return r, nil
	}
	result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err != nil || !result.Converged || len(result.Observations) != 3 || !result.Observations[1].ReadOnly || result.Observations[2].ReadOnly {
		t.Fatal("new scoped lag was hidden by an earlier certificate")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.analyzeCalls != 2 || p.processCalls != 2 {
		t.Fatal("normal maintenance did not resume for new lag")
	}
}
