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
	loginToken   string
	budgetToken  string
	principalID  string
	username     string
	searchCode   codes.Code
	mutateBudget bool
	budget       *adminv1.SemanticProviderBudgetStatus
	budgetMu     *sync.Mutex

	loginCalls   atomic.Int64
	refreshCalls atomic.Int64
	whoAmICalls  atomic.Int64
	searchCalls  atomic.Int64
	budgetCalls  atomic.Int64
}

func (s *semanticAuthorizationProbeGRPCServer) Login(_ context.Context, req *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.loginCalls.Add(1)
	if strings.TrimSpace(req.GetUsername()) == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.Unauthenticated, "credentials are required")
	}
	accessToken := s.loginToken
	if accessToken == "" {
		accessToken = s.token
	}
	return &commonv1.LoginResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

func (s *semanticAuthorizationProbeGRPCServer) Refresh(_ context.Context, _ *commonv1.RefreshRequest) (*commonv1.RefreshResponse, error) {
	s.refreshCalls.Add(1)
	return nil, status.Error(codes.FailedPrecondition, "refresh is not expected")
}

func (s *semanticAuthorizationProbeGRPCServer) WhoAmI(ctx context.Context, _ *commonv1.WhoAmIRequest) (*commonv1.WhoAmIResponse, error) {
	s.whoAmICalls.Add(1)
	if err := s.requireOwnToken(ctx); err != nil {
		return nil, err
	}
	return &commonv1.WhoAmIResponse{Principal: &commonv1.AuthPrincipal{PrincipalId: s.principalID, Username: s.username}}, nil
}

func (s *semanticAuthorizationProbeGRPCServer) GetSemanticProviderBudgetStatus(ctx context.Context, _ *adminv1.GetSemanticProviderBudgetStatusRequest) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
	s.budgetCalls.Add(1)
	budgetToken := s.budgetToken
	if budgetToken == "" {
		budgetToken = s.token
	}
	if err := requireSemanticAuthorizationToken(ctx, budgetToken, codes.PermissionDenied); err != nil {
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
	return requireSemanticAuthorizationToken(ctx, s.token, codes.Unauthenticated)
}

func requireSemanticAuthorizationToken(ctx context.Context, token string, code codes.Code) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || strings.TrimSpace(values[0]) != "Bearer "+token {
		return status.Error(code, "token is not authorized for this operation")
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
	controllerServer := &semanticAuthorizationProbeGRPCServer{token: "controller-local-token", loginToken: "budget-observer-token", budgetToken: "budget-observer-token", principalID: "controller-principal", username: "controller", searchCode: codes.Unimplemented, budget: budget, budgetMu: budgetMu}
	outsiderServer := &semanticAuthorizationProbeGRPCServer{token: "outsider-local-token", principalID: "outsider-principal", username: "outsider", searchCode: searchCode, mutateBudget: mutateBudget, budget: budget, budgetMu: budgetMu}
	controllerAddr := startSemanticAuthorizationProbeServer(t, controllerServer)
	outsiderAddr := startSemanticAuthorizationProbeServer(t, outsiderServer)
	resources := &provision.ScenarioResources{
		AdminAddr: controllerAddr, AdminUsername: provision.DefaultAdminUsername, AdminPassword: provision.DefaultAdminPassword, DaemonAddrs: []string{controllerAddr},
		Assignments: []provision.ActorAssignment{
			{ActorID: "controller-0", GroupName: "semantic-controller", PrincipalID: "controller-principal", Username: "controller", DaemonAddr: controllerAddr, AccessToken: "controller-local-token"},
			{ActorID: "outsider-0", GroupName: "semantic-outsider", PrincipalID: "outsider-principal", DaemonAddr: outsiderAddr, Username: "outsider", AccessToken: "outsider-local-token"},
		},
		Semantic: &provision.SemanticResources{
			TargetActorID: "controller-0", SpaceID: "space", DomainID: "domain", SemanticRuleID: "rule", EmbeddingBindingKey: "search", ProviderBudgetScopeID: "scope",
			ExpectedProviderBudget: &adminv1.SemanticProviderBudgetPolicy{CorrelationId: "run"},
		},
	}
	return resources, controllerServer, outsiderServer
}

func TestSemanticUnauthorizedSearchVerifiesProvisionedNodeLocalSession(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage != "complete" || !result.ActorAuthenticated || !result.SearchAttempted || !result.Rejected || result.RejectedCode != codes.PermissionDenied.String() || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want authenticated permission denial and unchanged observed budget", result)
	}
	if result.Requests != 4 || result.Attempts != 4 || result.InputTokens != 32 || result.EstimatedCostMicros != 1 {
		t.Fatalf("result=%+v, want exact aggregate budget evidence", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 1 || outsider.searchCalls.Load() != 1 {
		t.Fatalf("outsider login calls=%d refresh calls=%d whoami calls=%d search calls=%d, want 0, 0, 1, 1", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load())
	}
	if controller.loginCalls.Load() != 1 || controller.refreshCalls.Load() != 0 || controller.budgetCalls.Load() != 2 {
		t.Fatalf("budget observer login calls=%d refresh calls=%d budget calls=%d, want 1, 0, 2", controller.loginCalls.Load(), controller.refreshCalls.Load(), controller.budgetCalls.Load())
	}
}

func TestSemanticUnauthorizedSearchWithoutBudgetScopeStillExercisesNodeLocalSession(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	resources.Semantic.ProviderBudgetScopeID = ""
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage != "complete" || !result.ActorAuthenticated || !result.SearchAttempted || !result.Rejected || result.RejectedCode != codes.PermissionDenied.String() || result.ProviderBudgetObserved || result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want authenticated permission denial without fabricated budget evidence", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 1 || outsider.searchCalls.Load() != 1 || controller.loginCalls.Load() != 0 || controller.budgetCalls.Load() != 0 {
		t.Fatalf("outsider login=%d refresh=%d whoami=%d search=%d observer login=%d budget=%d, want 0, 0, 1, 1, 0, 0", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load(), controller.loginCalls.Load(), controller.budgetCalls.Load())
	}
}

func TestSemanticUnauthorizedSearchRequiredBudgetFailsClosedWhenScopeMissing(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	resources.Semantic.ProviderBudgetScopeID = ""
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider", "requireProviderBudgetEvidence": true})
	if err == nil {
		t.Fatal("expected missing required budget scope to fail")
	}
	if result.Stage != "budget-before" || result.ActorAuthenticated || result.SearchAttempted || result.ProviderBudgetObserved {
		t.Fatalf("result=%+v, want budget-before failure without authentication or search", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 0 || outsider.searchCalls.Load() != 0 || controller.loginCalls.Load() != 0 || controller.budgetCalls.Load() != 0 {
		t.Fatalf("outsider login=%d refresh=%d whoami=%d search=%d observer login=%d budget=%d, want 0, 0, 0, 0, 0, 0", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load(), controller.loginCalls.Load(), controller.budgetCalls.Load())
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
	if result.Stage != "semantic-search-result" || !result.ActorAuthenticated || !result.SearchAttempted || result.Rejected || result.RejectedCode != codes.Unauthenticated.String() || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged || result.Requests != 4 {
		t.Fatalf("result=%+v, want safe unexpected-code and unchanged-budget evidence", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 1 || outsider.searchCalls.Load() != 1 || controller.loginCalls.Load() != 1 || controller.budgetCalls.Load() != 2 {
		t.Fatalf("outsider login=%d refresh=%d whoami=%d search=%d observer login=%d budget=%d, want 0, 0, 1, 1, 1, 2", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load(), controller.loginCalls.Load(), controller.budgetCalls.Load())
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
	if result.Stage != "budget-verification" || !result.ActorAuthenticated || !result.SearchAttempted || !result.Rejected || !result.ProviderBudgetObserved || result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want rejected request with changed budget evidence", result)
	}
}

func TestSemanticUnauthorizedSearchRetainsStageAndBudgetWhenNodeLocalSessionIsRejected(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	resources.Assignments[1].AccessToken = "foreign-node-token"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error=%v code=%s, want Unauthenticated", err, status.Code(err))
	}
	if result.Stage != "negative-actor-authentication" || result.ActorAuthenticated || result.SearchAttempted || result.Rejected || result.RejectedCode != codes.Unauthenticated.String() || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged || result.Requests != 4 {
		t.Fatalf("result=%+v, want safe authentication-stage and unchanged-budget evidence", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 1 || outsider.searchCalls.Load() != 0 || controller.loginCalls.Load() != 1 || controller.budgetCalls.Load() != 2 {
		t.Fatalf("outsider login=%d refresh=%d whoami=%d search=%d observer login=%d budget=%d, want 0, 0, 1, 0, 1, 2", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load(), controller.loginCalls.Load(), controller.budgetCalls.Load())
	}
}

func TestSemanticUnauthorizedSearchRejectsMismatchedNodeLocalIdentity(t *testing.T) {
	resources, controller, outsider := semanticAuthorizationProbeFixture(t, codes.PermissionDenied, false)
	outsider.principalID = "different-principal"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := executeSemanticUnauthorizedSearch(ctx, resources, map[string]any{"actorGroup": "semantic-outsider"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("error=%v code=%s, want FailedPrecondition", err, status.Code(err))
	}
	if result.Stage != "negative-actor-authentication" || result.ActorAuthenticated || result.SearchAttempted || result.Rejected || !result.ProviderBudgetObserved || !result.ProviderBudgetUnchanged {
		t.Fatalf("result=%+v, want safe identity-mismatch stage and unchanged-budget evidence", result)
	}
	if outsider.loginCalls.Load() != 0 || outsider.refreshCalls.Load() != 0 || outsider.whoAmICalls.Load() != 1 || outsider.searchCalls.Load() != 0 || controller.loginCalls.Load() != 1 || controller.budgetCalls.Load() != 2 {
		t.Fatalf("outsider login=%d refresh=%d whoami=%d search=%d observer login=%d budget=%d, want 0, 0, 1, 0, 1, 2", outsider.loginCalls.Load(), outsider.refreshCalls.Load(), outsider.whoAmICalls.Load(), outsider.searchCalls.Load(), controller.loginCalls.Load(), controller.budgetCalls.Load())
	}
}
