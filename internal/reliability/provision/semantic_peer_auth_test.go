package provision

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type semanticPeerAuthGRPCServer struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer
	token                   string
	leader                  bool
	loginCalls              atomic.Int64
	budgetCalls             atomic.Int64
	statusCalls             atomic.Int64
	remainingLeaderFailures atomic.Int64
}

func (s *semanticPeerAuthGRPCServer) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.loginCalls.Add(1)
	return &commonv1.LoginResponse{AccessToken: s.token, AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

func (s *semanticPeerAuthGRPCServer) GetSemanticProviderBudgetCapabilities(ctx context.Context, _ *adminv1.GetSemanticProviderBudgetCapabilitiesRequest) (*adminv1.GetSemanticProviderBudgetCapabilitiesResponse, error) {
	s.budgetCalls.Add(1)
	if err := s.requireOwnToken(ctx); err != nil {
		return nil, err
	}
	if !s.leader || s.remainingLeaderFailures.Add(-1) >= 0 {
		return nil, status.Error(codes.FailedPrecondition, "not system leader")
	}
	return &adminv1.GetSemanticProviderBudgetCapabilitiesResponse{ContractVersion: "semantic-provider-budget/v1"}, nil
}

func (s *semanticPeerAuthGRPCServer) GetSemanticProviderBudgetStatus(ctx context.Context, _ *adminv1.GetSemanticProviderBudgetStatusRequest) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
	s.statusCalls.Add(1)
	if err := s.requireOwnToken(ctx); err != nil {
		return nil, err
	}
	return &adminv1.GetSemanticProviderBudgetStatusResponse{}, nil
}

func (s *semanticPeerAuthGRPCServer) requireOwnToken(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || strings.TrimSpace(values[0]) != "Bearer "+s.token {
		return status.Error(codes.Unauthenticated, "token was not issued by this daemon")
	}
	return nil
}

func startSemanticPeerAuthServer(t *testing.T, server *semanticPeerAuthGRPCServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(grpcServer, server)
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

func dialSemanticPeerAuthPrimary(t *testing.T, ctx context.Context, addr string) *mycel.AdminClient {
	t.Helper()
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: addr, Username: "operator", Password: "test-password", ClientName: "semantic-peer-auth-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	return admin
}

func TestReadSemanticAdminAcrossDaemonsAuthenticatesVisitedPeerDirectly(t *testing.T) {
	primaryServer := &semanticPeerAuthGRPCServer{token: "primary-token"}
	leaderServer := &semanticPeerAuthGRPCServer{token: "leader-token", leader: true}
	leaderServer.remainingLeaderFailures.Store(2)
	primaryAddr := startSemanticPeerAuthServer(t, primaryServer)
	leaderAddr := startSemanticPeerAuthServer(t, leaderServer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	primary := dialSemanticPeerAuthPrimary(t, ctx, primaryAddr)
	scenario := &ScenarioResources{AdminAddr: primaryAddr, AdminUsername: "operator", AdminPassword: "test-password", DaemonAddrs: []string{primaryAddr, leaderAddr}, Semantic: &SemanticResources{MaintenanceTimeout: time.Second, MaintenancePollInterval: time.Millisecond}}

	result, err := ReadSemanticAdminAcrossDaemons(ctx, primary, primaryAddr, scenario, func(callCtx context.Context, client *mycel.AdminClient) (string, error) {
		_, readErr := client.SemanticMaintenance.GetSemanticProviderBudgetCapabilities(callCtx, &adminv1.GetSemanticProviderBudgetCapabilitiesRequest{})
		return "leader", readErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "leader" || scenario.AdminAddr != leaderAddr {
		t.Fatalf("result=%q adminAddr=%q, want leader and %q", result, scenario.AdminAddr, leaderAddr)
	}
	if primaryServer.loginCalls.Load() != 1 || leaderServer.loginCalls.Load() != 1 {
		t.Fatalf("login calls primary=%d leader=%d, want 1 and 1", primaryServer.loginCalls.Load(), leaderServer.loginCalls.Load())
	}
	if primaryServer.budgetCalls.Load() != 3 || leaderServer.budgetCalls.Load() != 3 {
		t.Fatalf("budget calls primary=%d leader=%d, want 3 and 3 across cached sessions", primaryServer.budgetCalls.Load(), leaderServer.budgetCalls.Load())
	}
}

func TestSemanticBudgetStatusReadersAuthenticateEachPeerOnce(t *testing.T) {
	primaryServer := &semanticPeerAuthGRPCServer{token: "primary-token", leader: true}
	peerServer := &semanticPeerAuthGRPCServer{token: "peer-token", leader: true}
	primaryAddr := startSemanticPeerAuthServer(t, primaryServer)
	peerAddr := startSemanticPeerAuthServer(t, peerServer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	primary := dialSemanticPeerAuthPrimary(t, ctx, primaryAddr)
	scenario := &ScenarioResources{AdminAddr: primaryAddr, AdminUsername: "operator", AdminPassword: "test-password", DaemonAddrs: []string{primaryAddr, peerAddr}, Semantic: &SemanticResources{SpaceID: "space", SemanticRuleID: "rule", EmbeddingBindingKey: "search"}}

	readers, closeReaders, err := semanticBudgetStatusReaders(ctx, primary, scenario)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReaders()
	if len(readers) != 2 {
		t.Fatalf("readers=%d, want 2", len(readers))
	}
	for _, reader := range readers {
		if _, err := reader.read(ctx); err != nil {
			t.Fatalf("read %s: %v", reader.addr, err)
		}
	}
	if primaryServer.loginCalls.Load() != 1 || peerServer.loginCalls.Load() != 1 {
		t.Fatalf("login calls primary=%d peer=%d, want 1 and 1", primaryServer.loginCalls.Load(), peerServer.loginCalls.Load())
	}
	if primaryServer.statusCalls.Load() != 1 || peerServer.statusCalls.Load() != 1 {
		t.Fatalf("status calls primary=%d peer=%d, want 1 and 1", primaryServer.statusCalls.Load(), peerServer.statusCalls.Load())
	}
}
