package events

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type semanticSearchProbeServer struct {
	clientv1.UnimplementedSemanticServiceServer
	search func(context.Context, *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error)
}

func (s *semanticSearchProbeServer) SemanticSearch(ctx context.Context, request *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
	return s.search(ctx, request)
}

func semanticSearchProbeFixture(t *testing.T, search func(context.Context, *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error)) *provision.ScenarioResources {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	clientv1.RegisterSemanticServiceServer(server, &semanticSearchProbeServer{search: search})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})
	return &provision.ScenarioResources{
		Assignments: []provision.ActorAssignment{{ActorID: "controller-0", DaemonAddr: listener.Addr().String(), AccessToken: "local-search-probe-token"}},
		Semantic: &provision.SemanticResources{
			TargetActorID: "controller-0", SpaceID: "space", DomainID: "domain", SemanticRuleID: "rule", EmbeddingBindingKey: "search",
		},
	}
}

func semanticSearchProbeResult(nodeID string) *clientv1.SemanticSearchResult {
	return &clientv1.SemanticSearchResult{NodeId: nodeID, RecordId: "record-" + nodeID, SemanticRuleId: "rule", EmbeddingBindingKey: "search", Score: 1}
}

func TestSemanticSearchRetriesOnlyTransientFailuresWhenConfigured(t *testing.T) {
	tests := []struct {
		name         string
		firstError   error
		timeout      string
		expectedCode string
		wantCalls    int64
		wantError    bool
	}{
		{name: "unavailable", firstError: status.Error(codes.Unavailable, "leader recovering"), timeout: "1s", wantCalls: 2},
		{name: "rpc deadline", firstError: status.Error(codes.DeadlineExceeded, "temporary RPC timeout"), timeout: "1s", wantCalls: 2},
		{name: "not ready", firstError: status.Error(codes.Internal, "semantic index not ready"), timeout: "1s", wantCalls: 2},
		{name: "maintenance unavailable", firstError: status.Error(codes.Internal, "semantic maintenance request failed"), timeout: "1s", wantCalls: 2},
		{name: "permission denied", firstError: status.Error(codes.PermissionDenied, "forbidden"), timeout: "1s", wantCalls: 1, wantError: true},
		{name: "unrelated internal error", firstError: status.Error(codes.Internal, "invalid index data"), timeout: "1s", wantCalls: 1, wantError: true},
		{name: "no retry by default", firstError: status.Error(codes.Unavailable, "leader recovering"), wantCalls: 1, wantError: true},
		{name: "expected rejection", firstError: status.Error(codes.Unavailable, "leader recovering"), timeout: "1s", expectedCode: "Unavailable", wantCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			resources := semanticSearchProbeFixture(t, func(context.Context, *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
				if calls.Add(1) == 1 {
					return nil, test.firstError
				}
				return &clientv1.SemanticSearchResponse{}, nil
			})
			target := map[string]any{"query": "axis-0", "expectedResultCount": 0, "pollInterval": "1ms"}
			if test.timeout != "" {
				target["timeout"] = test.timeout
			}
			if test.expectedCode != "" {
				target["expectFailureCode"] = test.expectedCode
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			evidence, err := executeSemanticSearchAssertion(ctx, resources, target)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v, wantError=%t", err, test.wantError)
			}
			if got := calls.Load(); got != test.wantCalls {
				t.Fatalf("search calls=%d, want %d", got, test.wantCalls)
			}
			if test.expectedCode != "" && evidence.RejectedCode != test.expectedCode {
				t.Fatalf("rejected code=%q, want %q", evidence.RejectedCode, test.expectedCode)
			}
		})
	}
}

func TestSemanticSearchRetryRestartsPaginationWithoutPartialEvidence(t *testing.T) {
	var calls atomic.Int64
	resources := semanticSearchProbeFixture(t, func(_ context.Context, request *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
		switch call := calls.Add(1); call {
		case 1:
			if request.GetPageToken() != "" {
				return nil, status.Error(codes.InvalidArgument, "first search must start at page one")
			}
			return &clientv1.SemanticSearchResponse{Results: []*clientv1.SemanticSearchResult{semanticSearchProbeResult("stale-node")}, Warnings: []string{"discarded partial observation"}, NextPageToken: "stale-next"}, nil
		case 2:
			if request.GetPageToken() != "stale-next" {
				return nil, status.Error(codes.InvalidArgument, "missing original page token")
			}
			return nil, status.Error(codes.Unavailable, "leader recovering")
		case 3:
			if request.GetPageToken() != "" {
				return nil, status.Error(codes.InvalidArgument, "retry retained a stale page token")
			}
			return &clientv1.SemanticSearchResponse{Results: []*clientv1.SemanticSearchResult{semanticSearchProbeResult("node-1")}, NextPageToken: "fresh-next"}, nil
		case 4:
			if request.GetPageToken() != "fresh-next" {
				return nil, status.Error(codes.InvalidArgument, "missing fresh page token")
			}
			return &clientv1.SemanticSearchResponse{Results: []*clientv1.SemanticSearchResult{semanticSearchProbeResult("node-2")}}, nil
		default:
			return nil, status.Error(codes.Internal, fmt.Sprintf("unexpected search call %d", call))
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	evidence, err := executeSemanticSearchAssertion(ctx, resources, map[string]any{
		"query": "axis-0", "allPages": true, "timeout": "1s", "pollInterval": "1ms",
		"expectedResultCount": 2, "expectedNodeIds": []string{"node-1", "node-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 || len(evidence.Results) != 2 || len(evidence.Warnings) != 0 {
		t.Fatalf("search calls=%d evidence=%+v, want only the complete fresh search", calls.Load(), evidence)
	}
}

func TestSemanticSearchDoesNotRetryAssertionFailures(t *testing.T) {
	var calls atomic.Int64
	resources := semanticSearchProbeFixture(t, func(context.Context, *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
		calls.Add(1)
		return &clientv1.SemanticSearchResponse{}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := executeSemanticSearchAssertion(ctx, resources, map[string]any{"query": "axis-0", "timeout": "1s", "expectedResultCount": 1})
	if err == nil || !strings.Contains(err.Error(), "want 1") || calls.Load() != 1 {
		t.Fatalf("error=%v search calls=%d, want one failed assertion without retry", err, calls.Load())
	}
}

func TestSemanticSearchTimeoutBoundsRPCsAndRetryWaits(t *testing.T) {
	for _, mode := range []string{"blocked RPC", "retry wait", "parent deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			resources := semanticSearchProbeFixture(t, func(ctx context.Context, _ *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
				calls.Add(1)
				if mode != "retry wait" {
					<-ctx.Done()
					return nil, status.FromContextError(ctx.Err()).Err()
				}
				return nil, status.Error(codes.Unavailable, "leader recovering")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			timeout := "100ms"
			if mode == "parent deadline" {
				parentCtx, parentCancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer parentCancel()
				ctx = parentCtx
				timeout = "2s"
			}
			start := time.Now()
			_, err := executeSemanticSearchAssertion(ctx, resources, map[string]any{"query": "axis-0", "timeout": timeout, "pollInterval": "2s"})
			elapsed := time.Since(start)
			if status.Code(err) != codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error=%v, want deadline exceeded", err)
			}
			if elapsed > time.Second || calls.Load() != 1 {
				t.Fatalf("elapsed=%s search calls=%d, want a single call bounded by the earliest deadline", elapsed, calls.Load())
			}
		})
	}
}
