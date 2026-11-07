package events

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type semanticWorkGRPCProbe struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer
	mu     sync.Mutex
	probe  *semanticWorkProbe
	logins int
}

func (s *semanticWorkGRPCProbe) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.mu.Lock()
	s.logins++
	s.mu.Unlock()
	return &commonv1.LoginResponse{AccessToken: "integration-work-token", AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}
func semanticWorkProbeAuthenticated(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || values[0] != "Bearer integration-work-token" {
		return status.Error(codes.Unauthenticated, "work probe requires authentication")
	}
	return nil
}
func (s *semanticWorkGRPCProbe) AnalyzeSemanticDirtyWork(ctx context.Context, req *adminv1.AnalyzeSemanticDirtyWorkRequest) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probe.AnalyzeSemanticDirtyWork(ctx, req)
}
func (s *semanticWorkGRPCProbe) ListSemanticMaintenanceWork(ctx context.Context, req *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probe.ListSemanticMaintenanceWork(ctx, req)
}
func (s *semanticWorkGRPCProbe) GetSemanticMaintenanceStatus(ctx context.Context, req *adminv1.GetSemanticMaintenanceStatusRequest) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probe.GetSemanticMaintenanceStatus(ctx, req)
}
func (s *semanticWorkGRPCProbe) GetSemanticReliabilityStatus(ctx context.Context, req *adminv1.GetSemanticReliabilityStatusRequest) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probe.GetSemanticReliabilityStatus(ctx, req)
}

func TestSemanticWorkEventUsesAuthenticatedReadOnlyRestartCheck(t *testing.T) {
	p, semantic, target := semanticWorkFixture()
	semantic.MaintenanceTimeout = time.Second
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
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resources := &provision.ScenarioResources{AdminAddr: listener.Addr().String(), AdminUsername: "admin", AdminPassword: "integration-password", Semantic: semantic}
	target["analyze"], target["captureWorkAs"] = true, "pending"
	before, err := executeSemanticEvent(ctx, resources, "semantic-work-assert", target)
	if err != nil {
		t.Fatal(err)
	}
	delete(target, "analyze")
	delete(target, "captureWorkAs")
	target["expectWorkFrom"] = "pending"
	after, err := executeSemanticEvent(ctx, resources, "semantic-work-assert", target)
	if err != nil {
		t.Fatal(err)
	}
	if before.(SemanticWorkEvidenceV1).Work != after.(SemanticWorkEvidenceV1).Work {
		t.Fatal("work identity changed through public RPC mapping")
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.logins != 2 || p.analyzeCalls != 1 || p.listCalls != 2 {
		t.Fatalf("expected one authenticated session per event and no post-restart analysis; logins=%d analyze=%d list=%d", probe.logins, p.analyzeCalls, p.listCalls)
	}
}
