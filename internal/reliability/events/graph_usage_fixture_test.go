package events

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type graphUsageFixtureServer struct {
	clientv1.UnimplementedSessionServiceServer
	clientv1.UnimplementedTransactionServiceServer
	clientv1.UnimplementedGraphServiceServer
}

func TestGraphUsageFixtureMutatesWithoutSemanticProvisioning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, action := range []string{"create", "update", "delete"} {
		t.Run(action, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			calls := make(chan string, 10)
			server := grpc.NewServer(grpc.UnaryInterceptor(func(_ context.Context, req any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
				method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
				calls <- method
				switch r := req.(type) {
				case *clientv1.OpenSessionRequest:
					if r.GetSpaceId() != "space" || r.GetDomainId() != "domain" {
						return nil, status.Error(codes.InvalidArgument, "wrong actor scope")
					}
					return &clientv1.OpenSessionResponse{Session: &clientv1.GraphSession{SessionId: "session"}}, nil
				case *clientv1.BeginTransactionRequest:
					if r.GetSessionId() != "session" || r.GetMode() != clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE {
						return nil, status.Error(codes.InvalidArgument, "wrong session or mode")
					}
					return &clientv1.BeginTransactionResponse{Transaction: &clientv1.GraphTransaction{TransactionId: "tx"}}, nil
				case *clientv1.CreateNodeRequest:
					if r.GetTransactionId() != "tx" || r.GetNode().GetNodeId() != "node" || r.GetNode().GetPayload().GetFields()["text"].GetStringValue() != "世界" {
						return nil, status.Error(codes.InvalidArgument, "wrong node or payload for create")
					}
					return &clientv1.CreateNodeResponse{}, nil
				case *clientv1.GetNodeRequest:
					return &clientv1.GetNodeResponse{Node: &clientv1.Node{NodeId: "node"}}, nil
				case *clientv1.UpdateNodeRequest:
					if r.GetTransactionId() != "tx" || r.GetNode().GetNodeId() != "node" || !reflect.DeepEqual(r.GetUpdateMask().GetPaths(), []string{"payload"}) || r.GetNode().GetPayload().GetFields()["text"].GetStringValue() != "世界" {
						return nil, status.Error(codes.InvalidArgument, "wrong node or payload for update")
					}
					return &clientv1.UpdateNodeResponse{}, nil
				case *clientv1.DeleteNodeRequest:
					if r.GetTransactionId() != "tx" || r.GetNodeId() != "node" {
						return nil, status.Error(codes.InvalidArgument, "wrong node for delete")
					}
					return &clientv1.DeleteNodeResponse{DeletedNodeIds: []string{"node"}}, nil
				case *clientv1.CommitTransactionRequest:
					if r.GetTransactionId() != "tx" {
						return nil, status.Error(codes.InvalidArgument, "wrong transaction for commit")
					}
					return &clientv1.CommitTransactionResponse{Commit: &clientv1.TransactionCommit{CommitId: "commit", CommittedRevision: 7}}, nil
				case *clientv1.CloseSessionRequest:
					return &clientv1.CloseSessionResponse{}, nil
				default:
					return nil, status.Error(codes.Unimplemented, "unexpected fixture RPC")
				}
			}))
			fixtureServer := &graphUsageFixtureServer{}
			clientv1.RegisterSessionServiceServer(server, fixtureServer)
			clientv1.RegisterTransactionServiceServer(server, fixtureServer)
			clientv1.RegisterGraphServiceServer(server, fixtureServer)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = server.Serve(listener)
			}()
			t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
			resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{
				{GroupName: "unrelated"},
				{GroupName: "fixture", BehaviorType: "semantic-control", DaemonAddr: listener.Addr().String(), SpaceID: "space", DomainID: "domain", AccessToken: "fixture-token"},
			}}
			event := "semantic-node-" + action
			result, err := executeSemanticEvent(ctx, resources, event, map[string]any{"actorGroup": "fixture", "nodeId": "node", "text": "世界"})
			mutation, ok := result.(SemanticGraphMutationResult)
			if err != nil || !ok || mutation.Action != event || mutation.CommitID != "commit" || mutation.CommittedRevision != 7 {
				t.Fatalf("result=%+v err=%v, want committed graph mutation with no semantic resources", result, err)
			}
			want := []string{"OpenSession", "BeginTransaction"}
			switch action {
			case "create":
				want = append(want, "CreateNode")
			case "update":
				want = append(want, "GetNode", "UpdateNode")
			case "delete":
				want = append(want, "DeleteNode")
			}
			want = append(want, "CommitTransaction", "CloseSession")
			var got []string
			for len(calls) > 0 {
				got = append(got, <-calls)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("RPCs=%v, want %v", got, want)
			}
		})
	}
}

func TestGraphOnlyFixtureRejectsSemanticEventRequirements(t *testing.T) {
	resources := &provision.ScenarioResources{}
	for _, tc := range []struct {
		event  string
		target map[string]any
	}{
		{"semantic-node-create", map[string]any{"nodeId": "node"}},
		{"semantic-node-update", map[string]any{"nodeId": "node", "actorGroup": "fixture", "scope": "peer"}},
		{"semantic-node-delete", map[string]any{"nodeId": "node", "actorGroup": "fixture", "captureRevisionAs": "capture"}},
		{"semantic-search-assert", map[string]any{"actorGroup": "fixture"}},
	} {
		_, err := executeSemanticEvent(context.Background(), resources, tc.event, tc.target)
		if err == nil || !strings.Contains(err.Error(), "semantic resources are not provisioned") {
			t.Fatalf("%s %v error=%v, want semantic provisioning requirement", tc.event, tc.target, err)
		}
	}
}
