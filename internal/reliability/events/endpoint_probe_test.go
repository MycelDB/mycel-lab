package events

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type endpointProbeSpaceServer struct {
	clientv1.UnimplementedSpaceServiceServer
}

func (endpointProbeSpaceServer) GetSpace(_ context.Context, req *clientv1.GetSpaceRequest) (*clientv1.GetSpaceResponse, error) {
	return &clientv1.GetSpaceResponse{Space: &clientv1.Space{SpaceId: req.GetSpaceId(), Name: "probe"}}, nil
}

func TestProbeExternalGraphEndpointUsesSameExternalGRPCPath(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authorization := make(chan string, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		select {
		case authorization <- strings.Join(md.Get("authorization"), ","):
		default:
		}
		return handler(ctx, req)
	}))
	clientv1.RegisterSpaceServiceServer(server, endpointProbeSpaceServer{})
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()
	defer func() {
		server.Stop()
		<-serveDone
	}()
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", DaemonAddr: listener.Addr().String(), AccessToken: "test-token", SpaceID: "space-1"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observation, err := probeExternalGraphEndpoint(ctx, resources, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if observation.Attempts != 1 || observation.Code != "ok" || observation.Stage != "get-space" {
		t.Fatalf("probe observation=%+v", observation)
	}
	if got := <-authorization; got != "Bearer test-token" {
		t.Fatalf("authorization=%q", got)
	}
}

type recoveringEndpointProbeSpaceServer struct {
	clientv1.UnimplementedSpaceServiceServer
	remainingFailures atomic.Int32
}

func (s *recoveringEndpointProbeSpaceServer) GetSpace(_ context.Context, req *clientv1.GetSpaceRequest) (*clientv1.GetSpaceResponse, error) {
	if s.remainingFailures.Add(-1) >= 0 {
		return nil, status.Error(codes.FailedPrecondition, "recovery pending")
	}
	return &clientv1.GetSpaceResponse{Space: &clientv1.Space{SpaceId: req.GetSpaceId(), Name: "probe"}}, nil
}

func TestProbeExternalGraphEndpointWaitsForSemanticRecovery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &recoveringEndpointProbeSpaceServer{}
	service.remainingFailures.Store(2)
	server := grpc.NewServer()
	clientv1.RegisterSpaceServiceServer(server, service)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()
	defer func() {
		server.Stop()
		<-serveDone
	}()
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", DaemonAddr: listener.Addr().String(), AccessToken: "test-token", SpaceID: "space-1"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observation, err := probeExternalGraphEndpoint(ctx, resources, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if observation.Attempts != 3 || observation.Code != "ok" || observation.Stage != "get-space" {
		t.Fatalf("probe observation=%+v", observation)
	}
}

func TestEndpointReadinessRetryClassification(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted, codes.FailedPrecondition, codes.NotFound, codes.Internal, codes.Unknown, codes.ResourceExhausted} {
		if !retryableEndpointReadinessCode(code) {
			t.Fatalf("code %s must be retryable for the idempotent readiness read", code)
		}
		if canonicalProbeCode(code) == "" {
			t.Fatalf("code %s has empty canonical evidence", code)
		}
	}
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented, codes.DataLoss} {
		if retryableEndpointReadinessCode(code) {
			t.Fatalf("code %s must fail immediately", code)
		}
		if canonicalProbeCode(code) == "" {
			t.Fatalf("code %s has empty canonical evidence", code)
		}
	}
}

func TestProbeExternalGraphEndpointCannotRefreshAuthentication(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var refreshCalls atomic.Int32
	server := grpc.NewServer(grpc.UnaryInterceptor(func(_ context.Context, _ any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/Refresh") {
			refreshCalls.Add(1)
		}
		return nil, status.Error(codes.Unauthenticated, "rejected")
	}))
	clientv1.RegisterSpaceServiceServer(server, endpointProbeSpaceServer{})
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()
	defer func() {
		server.Stop()
		<-serveDone
	}()
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", DaemonAddr: listener.Addr().String(), AccessToken: "expired-token", RefreshToken: "must-not-be-used", SpaceID: "space-1"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observation, err := probeExternalGraphEndpoint(ctx, resources, listener.Addr().String())
	if err == nil {
		t.Fatal("probe unexpectedly succeeded")
	}
	if observation.Attempts != 1 || observation.Code != "unauthenticated" || observation.Stage != "get-space" {
		t.Fatalf("probe observation=%+v", observation)
	}
	if got := refreshCalls.Load(); got != 0 {
		t.Fatalf("authentication refresh calls=%d, want 0", got)
	}
}
