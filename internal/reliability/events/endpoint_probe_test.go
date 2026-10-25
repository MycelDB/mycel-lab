package events

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc"
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
	server := grpc.NewServer()
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
	attempts, err := probeExternalGraphEndpoint(ctx, resources, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("probe attempts=%d, want 1", attempts)
	}
}
