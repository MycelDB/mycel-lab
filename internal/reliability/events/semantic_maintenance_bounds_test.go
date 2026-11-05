package events

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type boundedMaintenanceProbe struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer
	block bool
}

func (*boundedMaintenanceProbe) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	return &commonv1.LoginResponse{AccessToken: "integration-work-token", AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}
func (p *boundedMaintenanceProbe) AnalyzeSemanticDirtyWork(ctx context.Context, _ *adminv1.AnalyzeSemanticDirtyWorkRequest) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	if p.block {
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, "sensitive raw provider details")
	}
	return &adminv1.AnalyzeSemanticDirtyWorkResponse{}, nil
}
func (*boundedMaintenanceProbe) ProcessSemanticDirtyWork(ctx context.Context, _ *adminv1.ProcessSemanticDirtyWorkRequest) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	return &adminv1.ProcessSemanticDirtyWorkResponse{}, nil
}
func (*boundedMaintenanceProbe) GetSemanticMaintenanceStatus(ctx context.Context, _ *adminv1.GetSemanticMaintenanceStatusRequest) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	return &adminv1.GetSemanticMaintenanceStatusResponse{Degraded: true, DegradedReason: "sensitive raw provider details"}, nil
}
func TestSemanticMaintenanceDeadlineCoversRPCAndRedactsDiagnostics(t *testing.T) {
	for _, block := range []bool{true, false} {
		t.Run(map[bool]string{true: "blocked-rpc", false: "degraded"}[block], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			probe := &boundedMaintenanceProbe{block: block}
			commonv1.RegisterAuthServiceServer(server, probe)
			adminv1.RegisterAdminSemanticMaintenanceServiceServer(server, probe)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			defer func() { server.Stop(); listener.Close(); <-done }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: listener.Addr().String(), Username: "admin", Password: "fixture-password"})
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			resources := &provision.SemanticResources{SpaceID: "space", SemanticRuleID: "rule", EmbeddingBindingKey: "search", MaintenanceTimeout: 100 * time.Millisecond, MaintenancePollInterval: time.Millisecond, MaintenanceAnalyzeLimit: 1, MaintenanceProcessLimit: 1}
			started := time.Now()
			result, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
			raw, _ := json.Marshal(result)
			if err == nil || result.Converged || strings.Contains(err.Error(), "sensitive raw") || strings.Contains(string(raw), "sensitive raw") {
				t.Fatalf("unsupported/degraded evidence passed or leaked: %+v %v", result, err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("maintenance RPC escaped its operation deadline")
			}
		})
	}
}
