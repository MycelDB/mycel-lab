package events

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordingClusterRecoveryInspector struct {
	mu         sync.Mutex
	calls      map[int]int
	closeCalls int
}

func (i *recordingClusterRecoveryInspector) Inspect(_ context.Context, ordinal int, expectedGroupIDs []string, _ time.Duration, target map[string]any) ClusterRecoveryNodeResult {
	i.mu.Lock()
	i.calls[ordinal]++
	index := uint64(i.calls[ordinal])
	i.mu.Unlock()
	groups := make([]clusterRaftGroup, 0, len(expectedGroupIDs))
	for _, groupID := range expectedGroupIDs {
		commitIndex := index
		appliedIndex := index
		groups = append(groups, clusterRaftGroup{GroupID: groupID, LeaderNodeID: 1, Health: "healthy", CommitIndex: &commitIndex, AppliedIndex: &appliedIndex, LastIndex: index})
	}
	result := ClusterRecoveryNodeResult{Ordinal: ordinal, ReadinessReady: true}
	evaluateClusterRaftGroups(&result, groups, expectedGroupIDs, boolTarget(target, "requireZeroApplyLag", false), boolTarget(target, "requireConsistentProgress", false))
	return result
}

func (i *recordingClusterRecoveryInspector) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closeCalls++
	return nil
}

func TestClusterRecoveryReusesOneEndpointSessionInspectorAcrossPolls(t *testing.T) {
	inspector := &recordingClusterRecoveryInspector{calls: map[int]int{}}
	factoryCalls := 0
	factory := func(_ context.Context, _ env.EnvironmentDriver, _ env.Environment, ordinals []int, commandTimeout time.Duration, target map[string]any) (clusterRecoveryNodeInspector, error) {
		factoryCalls++
		if !reflect.DeepEqual(ordinals, []int{0, 1, 2}) || commandTimeout != 10*time.Millisecond || !boolTarget(target, "reuseEndpointSessions", false) {
			t.Fatalf("factory ordinals=%v commandTimeout=%s target=%v", ordinals, commandTimeout, target)
		}
		return inspector, nil
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3, Raft: spec.RaftSpec{PartitionCount: 0}}}
	result, err := executeClusterRecoveryWaitEventWithSessionFactory(context.Background(), nil, env.Environment{Name: "test", Driver: "test"}, scenario, map[string]any{
		"ordinals":                  []any{0, 1, 2},
		"timeout":                   "100ms",
		"interval":                  "1ms",
		"commandTimeout":            "10ms",
		"minimumStablePolls":        3,
		"requireZeroApplyLag":       true,
		"requireConsistentProgress": true,
		"reuseEndpointSessions":     true,
	}, factory)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Ready || result.Attempts != 3 || result.StablePolls != 3 || result.InspectionMode != "reusable-endpoint-session" || result.ReusableEndpointSessions != 3 || factoryCalls != 1 {
		t.Fatalf("result=%+v factoryCalls=%d, want three reused sessions and three stable polls", result, factoryCalls)
	}
	inspector.mu.Lock()
	defer inspector.mu.Unlock()
	if !reflect.DeepEqual(inspector.calls, map[int]int{0: 3, 1: 3, 2: 3}) || inspector.closeCalls != 1 {
		t.Fatalf("calls=%v closeCalls=%d, want three inspections per reused session and one close", inspector.calls, inspector.closeCalls)
	}
}

func TestClusterRecoveryReusableSessionInitializationFailsClosed(t *testing.T) {
	factoryErr := errors.New("session initialization failed")
	factory := func(context.Context, env.EnvironmentDriver, env.Environment, []int, time.Duration, map[string]any) (clusterRecoveryNodeInspector, error) {
		return nil, factoryErr
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1, Raft: spec.RaftSpec{PartitionCount: 0}}}
	result, err := executeClusterRecoveryWaitEventWithSessionFactory(context.Background(), nil, env.Environment{Name: "test", Driver: "test"}, scenario, map[string]any{
		"timeout":               "100ms",
		"commandTimeout":        "10ms",
		"reuseEndpointSessions": true,
	}, factory)
	if err == nil || !errors.Is(err, factoryErr) || result.Ready || result.Attempts != 0 {
		t.Fatalf("result=%+v error=%v, want fail-closed initialization error", result, err)
	}
}

func TestValidateReusableClusterReadinessMatchesDimensionedContract(t *testing.T) {
	ready := &adminv1.ClusterReadiness{ClientReady: true, ProcessReady: true, MetadataApplied: true, MetadataValidated: true, MetadataReady: true, PartitionGroupsStarted: true, RaftReady: true, ReadReady: true, WriteReady: true, AuthoritativeClusterId: "cluster-a", LocalClusterId: "cluster-a"}
	if err := validateReusableClusterReadiness(ready); err != nil {
		t.Fatalf("ready contract rejected: %v", err)
	}
	ready.WriteReady = false
	if err := validateReusableClusterReadiness(ready); err == nil {
		t.Fatal("write_ready=false accepted")
	}
}

type clusterRecoveryGRPCServer struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminClusterServiceServer
	loginCalls     atomic.Int64
	statusCalls    atomic.Int64
	raftGroupCalls atomic.Int64
	committedIndex atomic.Uint64
}

func (s *clusterRecoveryGRPCServer) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	s.loginCalls.Add(1)
	return &commonv1.LoginResponse{AccessToken: "test-access-token", AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

func (s *clusterRecoveryGRPCServer) GetClusterStatus(context.Context, *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error) {
	s.statusCalls.Add(1)
	return &adminv1.GetClusterStatusResponse{Readiness: &adminv1.ClusterReadiness{
		ClientReady: true, ProcessReady: true, MetadataApplied: true, MetadataValidated: true, MetadataReady: true,
		PartitionGroupsStarted: true, RaftReady: true, ReadReady: true, WriteReady: true,
		AuthoritativeClusterId: "cluster-a", LocalClusterId: "cluster-a", ExpectedMemberCount: 3,
	}}, nil
}

func (s *clusterRecoveryGRPCServer) ListRaftGroups(context.Context, *adminv1.ListRaftGroupsRequest) (*adminv1.ListRaftGroupsResponse, error) {
	s.raftGroupCalls.Add(1)
	index := s.committedIndex.Add(1)
	return &adminv1.ListRaftGroupsResponse{Groups: []*adminv1.RaftGroupStatus{{
		GroupId: "system", LeaderNodeId: 1, Health: adminv1.RaftGroupHealth_RAFT_GROUP_HEALTH_HEALTHY,
		CommitIndex: index, AppliedIndex: index, LastIndex: index,
	}}}, nil
}

type clusterRecoveryEndpointDriver struct {
	*recordingDriver
	endpoints []env.Endpoint
}

func (d *clusterRecoveryEndpointDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return append([]env.Endpoint(nil), d.endpoints...), nil
}

func TestReusableClusterRecoverySessionsAuthenticateOncePerEndpoint(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &clusterRecoveryGRPCServer{}
	grpcServer := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(grpcServer, server)
	adminv1.RegisterAdminClusterServiceServer(grpcServer, server)
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

	baseDriver := &recordingDriver{}
	endpoints := make([]env.Endpoint, 3)
	for ordinal := range endpoints {
		endpoints[ordinal] = env.Endpoint{NodeName: fmt.Sprintf("myceld-%d", ordinal), DaemonAddr: listener.Addr().String()}
	}
	driver := &clusterRecoveryEndpointDriver{recordingDriver: baseDriver, endpoints: endpoints}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3, Raft: spec.RaftSpec{PartitionCount: 0}}}
	result, err := executeClusterRecoveryWaitEvent(context.Background(), driver, env.Environment{Name: "test", Driver: "test"}, scenario, map[string]any{
		"timeout":                   "2s",
		"interval":                  "1ms",
		"commandTimeout":            "500ms",
		"minimumStablePolls":        3,
		"requireZeroApplyLag":       true,
		"requireConsistentProgress": true,
		"reuseEndpointSessions":     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Ready || result.Attempts != 3 || result.StablePolls != 3 || result.InspectionMode != "reusable-endpoint-session" || result.ReusableEndpointSessions != 3 {
		t.Fatalf("result=%+v, want three stable direct inspections across three reused sessions", result)
	}
	if got := server.loginCalls.Load(); got != 3 {
		t.Fatalf("login calls=%d, want one per endpoint", got)
	}
	if got := server.statusCalls.Load(); got != 9 {
		t.Fatalf("status calls=%d, want three endpoints across three polls", got)
	}
	if got := server.raftGroupCalls.Load(); got != 9 {
		t.Fatalf("raft-group calls=%d, want three endpoints across three polls", got)
	}
	if len(baseDriver.execCommands) != 0 {
		t.Fatalf("exec commands=%v, want reusable direct SDK inspection", baseDriver.execCommands)
	}
}
