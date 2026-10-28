package events

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type semanticRuleReadinessGRPCServer struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminSemanticServiceServer
	loginCalls      atomic.Int64
	validationCalls atomic.Int64
	mutationCalls   atomic.Int64
}

func (s *semanticRuleReadinessGRPCServer) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.loginCalls.Add(1)
	return &commonv1.LoginResponse{
		AccessToken:           "integration-access-token",
		AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour)),
	}, nil
}

func (s *semanticRuleReadinessGRPCServer) ValidateSemanticRule(context.Context, *adminv1.ValidateSemanticRuleRequest) (*adminv1.ValidateSemanticRuleResponse, error) {
	if s.validationCalls.Add(1) < 3 {
		return nil, status.Error(codes.Unauthenticated, "semantic authorization is not visible yet")
	}
	return &adminv1.ValidateSemanticRuleResponse{Valid: true}, nil
}

func (s *semanticRuleReadinessGRPCServer) CreateSemanticRule(context.Context, *adminv1.CreateSemanticRuleRequest) (*adminv1.CreateSemanticRuleResponse, error) {
	s.mutationCalls.Add(1)
	return &adminv1.CreateSemanticRuleResponse{}, nil
}

func TestSemanticRuleReadinessUsesOneLoginAndOneMutation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &semanticRuleReadinessGRPCServer{}
	grpcServer := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(grpcServer, server)
	adminv1.RegisterAdminSemanticServiceServer(grpcServer, server)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		<-serveDone
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	admin, err := mycel.DialAdmin(ctx, mycel.Config{
		Addr:       listener.Addr().String(),
		Username:   "admin",
		Password:   "integration-password",
		ClientName: "semantic-rule-readiness-integration",
	})
	if err != nil {
		t.Fatalf("dial admin: %v", err)
	}
	defer admin.Close()

	attempts, mutationAttempted, err := executeSemanticRuleMutation(ctx, func(probeCtx context.Context) error {
		callCtx, callCancel := admin.AuthCallContext(probeCtx)
		defer callCancel()
		response, validationErr := admin.Semantic.ValidateSemanticRule(callCtx, &adminv1.ValidateSemanticRuleRequest{})
		if validationErr != nil {
			return validationErr
		}
		if !response.GetValid() {
			return status.Error(codes.FailedPrecondition, "semantic validation was rejected")
		}
		return nil
	}, func(mutationCtx context.Context) error {
		callCtx, callCancel := admin.AuthCallContext(mutationCtx)
		defer callCancel()
		_, mutationErr := admin.Semantic.CreateSemanticRule(callCtx, &adminv1.CreateSemanticRuleRequest{})
		return mutationErr
	})
	if err != nil {
		t.Fatalf("execute semantic mutation after rule readiness: %v", err)
	}
	if attempts != 3 || !mutationAttempted {
		t.Fatalf("readiness attempts = %d, mutation attempted = %v; want 3, true", attempts, mutationAttempted)
	}
	if got := server.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
	if got := server.validationCalls.Load(); got != 3 {
		t.Fatalf("semantic validation calls = %d, want 3", got)
	}
	if got := server.mutationCalls.Load(); got != 1 {
		t.Fatalf("semantic mutation calls = %d, want 1", got)
	}
}
