package events

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type semanticAuthorizationProbeGRPCServer struct {
	commonv1.UnimplementedAuthServiceServer
	clientv1.UnimplementedSemanticServiceServer
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer

	token        string
	searchCode   codes.Code
	mutateBudget bool
	budget       *adminv1.SemanticProviderBudgetStatus
	budgetMu     *sync.Mutex

	loginCalls  atomic.Int64
	searchCalls atomic.Int64
	budgetCalls atomic.Int64
}

func (s *semanticAuthorizationProbeGRPCServer) Login(_ context.Context, req *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.loginCalls.Add(1)
	if strings.TrimSpace(req.GetUsername()) == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.Unauthenticated, "credentials are required")
	}
	return &commonv1.LoginResponse{AccessToken: s.token, AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

func (s *semanticAuthorizationProbeGRPCServer) GetSemanticProviderBudgetStatus(ctx context.Context, _ *adminv1.GetSemanticProviderBudgetStatusRequest) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
	s.budgetCalls.Add(1)
	if err := s.requireOwnToken(ctx); err != nil {
		return nil, err
	}
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	return &adminv1.GetSemanticProviderBudgetStatusResponse{Status: proto.Clone(s.budget).(*adminv1.SemanticProviderBudgetStatus)}, nil
}

func (s *semanticAuthorizationProbeGRPCServer) SemanticSearch(ctx context.Context, _ *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
	s.searchCalls.Add(1)
	if err := s.requireOwnToken(ctx); err != nil {
		return nil, err
	}
	if s.mutateBudget {
		s.budgetMu.Lock()
		s.budget.Usage.Requests++
		s.budget.Usage.Attempts++
		s.budget.HighWaterEventSequence++
		s.budgetMu.Unlock()
	}
	if s.searchCode == codes.OK {
		return &clientv1.SemanticSearchResponse{}, nil
	}
	return nil, status.Error(s.searchCode, "safe fake semantic search result")
}

func (s *semanticAuthorizationProbeGRPCServer) requireOwnToken(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || strings.TrimSpace(values[0]) != "Bearer "+s.token {
		return status.Error(codes.Unauthenticated, "token was not issued by this daemon")
	}
	return nil
}

func startSemanticAuthorizationProbeServer(t *testing.T, server *semanticAuthorizationProbeGRPCServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(grpcServer, server)
	clientv1.RegisterSemanticServiceServer(grpcServer, server)
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(grpcServer, server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String()
}

func semanticAuthorizationProbeFixture(t *testing.T, searchCode codes.Code, mutateBudget bool) (*provision.ScenarioResources, *semanticAuthorizationProbeGRPCServer, *semanticAuthorizationProbeGRPCServer) {
	t.Helper()
	budget := &adminv1.SemanticProviderBudgetStatus{
		SpaceId: "space", SemanticRuleId: "rule", EmbeddingBindingKey: "search", BudgetScopeId: "scope", CorrelationId: "run",
		Usage:                  &adminv1.SemanticProviderBudgetUsage{Requests: 4, Attempts: 4, BatchItems: 4, InputTokens: 32, EstimatedCostMicros: 1},
		HighWaterEventSequence: 8,
	}
	budgetMu := &sync.Mutex{}
	controllerServer := &semanticAuthorizationProbeGRPCServer{token: "controller-local-token", searchCode: codes.Unimplemented, budget: budget, budgetMu: budgetMu}
	outsiderServer := &semanticAuthorizationProbeGRPCServer{token: "outsider-local-token", searchCode: searchCode, mutateBudget: mutateBudget, budget: budget, budgetMu: budgetMu}
	controllerAddr := startSemanticAuthorizationProbeServer(t, controllerServer)
	outsiderAddr := startSemanticAuthorizationProbeServer(t, outsiderServer)
	resources := &provision.ScenarioResources{
		AdminAddr: controllerAddr, DaemonAddrs: []string{controllerAddr},
		Assignments: []provision.ActorAssignment{
			{ActorID: "controller-0", GroupName: "semantic-controller", DaemonAddr: controllerAddr, AccessToken: "controller-local-token"},
			// Deliberately stale/foreign cached tokens prove the negative control
			// performs a fresh direct login against outsiderAddr.
			{ActorID: "outsider-0", GroupName: "semantic-outsider", DaemonAddr: outsiderAddr, Username: "outsider", Password: "runtime-only-password", AccessToken: "foreign-node-token", RefreshToken: "foreign-refresh-token"},
		},
		Semantic: &provision.SemanticResources{
			TargetActorID: "controller-0", SpaceID: "space", DomainID: "domain", SemanticRuleID: "rule", EmbeddingBindingKey: "search", ProviderBudgetScopeID: "scope",
			ExpectedProviderBudget: &adminv1.SemanticProviderBudgetPolicy{CorrelationId: "run"},
		},
	}
	return resources, controllerServer, outsiderServer
}

func TestSemanticUnauthorizedSearchAuthenticatesNegativeActorDirectly(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Rejected || result.RejectedCode != codes.PermissionDenied.String() || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want permission denial and unchanged observed budget", result)
	}
	if result.Requests != 4 || result.Attempts != 4 || result.InputTokens != 32 || result.EstimatedCostMicros != 1 {
		t.Fatalf("result=%+v, want exact aggregate budget evidence", result)
	}
	if outsider.loginCalls.Load() != 1 || outsider.searchCalls.Load() != 1 {
		t.Fatalf("outsider login calls=%d search calls=%d, want 1 and 1", outsider.loginCalls.Load(), outsider.searchCalls.Load())
	}
	if controller.budgetCalls.Load() != 2 {
		t.Fatalf("controller budget calls=%d, want before and after snapshots", controller.budgetCalls.Load())
	}
}

func TestSemanticUnauthorizedSearchRetainsBudgetEvidenceForUnexpectedCode(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.Unauthenticated, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error=%v code=%s, want Unauthenticated", err, status.Code(err))
	}
	if result.Rejected || result.RejectedCode != codes.Unauthenticated.String() || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged || result.Requests != 4 {
		t.Fatalf("result=%+v, want safe unexpected-code and unchanged-budget evidence", result)
	}
	if outsider.loginCalls.Load() != 1 || outsider.searchCalls.Load() != 1 || controller.budgetCalls.Load() != 2 {
		t.Fatalf("login=%d search=%d budget=%d, want 1, 1, 2", outsider.loginCalls.Load(), outsider.searchCalls.Load(), controller.budgetCalls.Load())
	}
}

func TestSemanticUnauthorizedSearchFailsIfBudgetChanges(t *testing.T) {
	resources, _, _ := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("error=%v code=%s, want FailedPrecondition", err, status.Code(err))
	}
	if !result.Rejected || !result.ProviderBudgetObserved || result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want rejected request with changed budget evidence", result)
	}
}
