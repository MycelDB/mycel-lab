package events

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type semanticFaultContractGRPCProbe struct {
	*semanticWorkGRPCProbe
	processes, retries  int
	retryResponseMutate func(*adminv1.SemanticMaintenanceWorkItem)
}

func (s *semanticFaultContractGRPCProbe) ProcessSemanticDirtyWork(ctx context.Context, _ *adminv1.ProcessSemanticDirtyWorkRequest) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processes++
	if s.processes == 1 {
		s.probe.work.Status = "failed"
		s.probe.work.AttemptCount = 1
		s.probe.work.LastErrorCategory = "provider_rejected"
		s.probe.reliability.Degraded = true
		return &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: 1, FailedItems: 1}, nil
	}
	completeSemanticWork(s.probe)
	s.probe.work.AttemptCount = 2
	s.probe.reliability.Degraded = false
	return &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: 1, CompletedItems: 1}, nil
}
func (s *semanticFaultContractGRPCProbe) RetrySemanticMaintenanceWork(ctx context.Context, _ *adminv1.RetrySemanticMaintenanceWorkRequest) (*adminv1.RetrySemanticMaintenanceWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retries++
	s.probe.work.Status = "pending"
	s.probe.work.WorkGeneration++ // v0.21.4 requeues via one versioned upsert.
	s.probe.work.LastErrorCategory = ""
	s.probe.reliability.Degraded = false
	item := proto.Clone(s.probe.work).(*adminv1.SemanticMaintenanceWorkItem)
	if s.retryResponseMutate != nil {
		s.retryResponseMutate(item)
	}
	return &adminv1.RetrySemanticMaintenanceWorkResponse{Item: item}, nil
}

// Authenticated fake public RPCs exercise the entire oracle flow, not an actual
// provider/daemon qualification. Every unresolved snapshot uses v0.21.4's flag.
func TestSemanticFaultPublicRPCGapFlagFailureAndSameWorkRecovery(t *testing.T) {
	p, semantic, target := semanticWorkFixture()
	semantic.MaintenanceTimeout = time.Second
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	probe := &semanticFaultContractGRPCProbe{semanticWorkGRPCProbe: &semanticWorkGRPCProbe{probe: p}}
	server := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(server, probe)
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(server, probe)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resources := &provision.ScenarioResources{AdminAddr: listener.Addr().String(), AdminUsername: "fixture-admin", AdminPassword: "fixture-password", Semantic: semantic}
	evidence, err := executeSemanticFault(ctx, nil, env.Environment{Metadata: map[string]string{"providerEmulator": "true"}}, resources, map[string]any{"nodeId": target["nodeId"], "expectRevisionFrom": "committed", "disposition": "permanent"})
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Discovery.Reliability.RebuildRequired || !evidence.BeforeReliability.RebuildRequired || !evidence.FailedReliability.RebuildRequired || !evidence.FailedReliability.Degraded || evidence.RecoveredReliability.RebuildRequired || evidence.RecoveredReliability.Degraded || evidence.RecoveredReliability.UnresolvedGapCount != 0 || evidence.Recovered.AttemptCount != 2 || !evidence.OperatorRetry {
		t.Fatalf("v0.21.4 flags/failure/completion were not proved: %+v", evidence)
	}
	expected := evidence.Before
	expected.WorkGeneration++
	if evidence.OperatorRetryGeneration != expected.WorkGeneration || !sameFaultIdentity(expected, evidence.Recovered) {
		t.Fatal("recovery failed exact single-retry generation advance or replaced durable identity/revision")
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.processes != 2 || probe.retries != 1 {
		t.Fatalf("unbounded processing/retry: process=%d retry=%d", probe.processes, probe.retries)
	}
}

func TestSemanticFaultPublicRPCRejectsBadRetryReceiptWithoutResubmission(t *testing.T) {
	for name, mutate := range map[string]func(*adminv1.SemanticMaintenanceWorkItem){
		"unchanged generation": func(item *adminv1.SemanticMaintenanceWorkItem) { item.WorkGeneration-- },
		"skipped generation":   func(item *adminv1.SemanticMaintenanceWorkItem) { item.WorkGeneration++ },
		"foreign identity": func(item *adminv1.SemanticMaintenanceWorkItem) {
			item.WorkItemId = "00000000-0000-4000-8000-000000000999"
		},
		"attempt reset": func(item *adminv1.SemanticMaintenanceWorkItem) { item.AttemptCount = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			p, semantic, target := semanticWorkFixture()
			semantic.MaintenanceTimeout = time.Second
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			probe := &semanticFaultContractGRPCProbe{semanticWorkGRPCProbe: &semanticWorkGRPCProbe{probe: p}, retryResponseMutate: mutate}
			server := grpc.NewServer()
			commonv1.RegisterAuthServiceServer(server, probe)
			adminv1.RegisterAdminSemanticMaintenanceServiceServer(server, probe)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resources := &provision.ScenarioResources{AdminAddr: listener.Addr().String(), AdminUsername: "fixture-admin", AdminPassword: "fixture-password", Semantic: semantic}
			evidence, err := executeSemanticFault(ctx, nil, env.Environment{Metadata: map[string]string{"providerEmulator": "true"}}, resources, map[string]any{"nodeId": target["nodeId"], "expectRevisionFrom": "committed", "disposition": "permanent"})
			if err == nil || !strings.Contains(err.Error(), "explicit work retry changed identity") || evidence.Recovered.WorkItemID != "" || evidence.OperatorRetry || evidence.OperatorRetryGeneration != 0 {
				t.Fatalf("invalid retry receipt continued or lost its fail-closed boundary: evidence=%+v error=%v", evidence, err)
			}
			probe.mu.Lock()
			defer probe.mu.Unlock()
			if probe.retries != 1 || probe.processes != 1 {
				t.Fatalf("replayed a rejected mutation: retries=%d processes=%d", probe.retries, probe.processes)
			}
		})
	}
}

func TestSemanticFaultPublicRPCRejectionRetainsDiscoveryReliability(t *testing.T) {
	p, semantic, target := semanticWorkFixture()
	semantic.MaintenanceTimeout = time.Second
	p.reliability.Rebuild.State = "required"
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	probe := &semanticWorkGRPCProbe{probe: p}
	server := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(server, probe)
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(server, probe)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resources := &provision.ScenarioResources{AdminAddr: listener.Addr().String(), AdminUsername: "fixture-admin", AdminPassword: "fixture-password", Semantic: semantic}
	evidence, err := executeSemanticFault(ctx, nil, env.Environment{Metadata: map[string]string{"providerEmulator": "true"}}, resources, map[string]any{"nodeId": target["nodeId"], "expectRevisionFrom": "committed", "disposition": "permanent"})
	if err == nil || evidence.Discovery == nil || evidence.Discovery.Reliability == nil {
		t.Fatal("public rejection lost its discovery diagnostics")
	}
	proof := evidence.Discovery.Reliability
	if !proof.Observed || !proof.ScopeMatches || !proof.RebuildRequired || proof.RebuildState != "required" || proof.UnresolvedGapCount != 1 || proof.LatestObservedGraphRevision != 2 || proof.ConfiguredFrontierLimit != 1024 || proof.RejectionReason != "unsafe-rebuild-state" {
		t.Fatalf("public mapping discarded rejected fields: %+v", proof)
	}
	if evidence.BeforeReliability != nil || evidence.OperatorRetry || evidence.BackoffHonored {
		t.Fatal("unsafe discovery continued to recovery")
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if p.analyzeCalls != 1 || p.listCalls != 1 {
		t.Fatal("unsafe discovery was resubmitted")
	}
}
