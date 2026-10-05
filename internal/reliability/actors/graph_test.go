package actors

import (
	"context"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGraphOperationGenerationIsDeterministic(t *testing.T) {
	profile := testGraphProfile()
	state := KnownState{Nodes: []string{"n1", "n2"}, Edges: map[string]bool{}}
	first := GenerateGraphTransaction("actor-1", 99, 7, profile, state)
	second := GenerateGraphTransaction("actor-1", 99, 7, profile, state)
	if len(first.Operations) == 0 {
		t.Fatal("generated no operations")
	}
	if len(first.Operations) != len(second.Operations) {
		t.Fatalf("operation count changed: %d != %d", len(first.Operations), len(second.Operations))
	}
	for i := range first.Operations {
		if first.Operations[i].Type != second.Operations[i].Type || first.Operations[i].NodeID != second.Operations[i].NodeID || first.Operations[i].FromID != second.Operations[i].FromID || first.Operations[i].ToID != second.Operations[i].ToID {
			t.Fatalf("operation %d changed: %+v != %+v", i, first.Operations[i], second.Operations[i])
		}
	}
}

func TestGraphOperationMixRespectsWeights(t *testing.T) {
	profile := testGraphProfile()
	state := KnownState{Nodes: []string{"n1", "n2", "n3", "n4"}, Edges: map[string]bool{}}
	counts := map[oracle.OperationType]int{}
	for seq := int64(1); seq <= 500; seq++ {
		tx := GenerateGraphTransaction("actor-1", 1234, seq, profile, state)
		for _, op := range tx.Operations {
			counts[op.Type]++
		}
	}
	if counts[oracle.OperationCreateNode] <= counts[oracle.OperationUpdateNode] || counts[oracle.OperationCreateNode] <= counts[oracle.OperationCreateEdge] {
		t.Fatalf("createNode should dominate broad weighted sample: %+v", counts)
	}
	if counts[oracle.OperationUpdateNode] == 0 || counts[oracle.OperationCreateEdge] == 0 {
		t.Fatalf("expected all weighted operation classes to appear: %+v", counts)
	}
}

func TestImpossibleCreateEdgeFallsBackToCreateNode(t *testing.T) {
	profile := DefaultGraphProfile()
	profile.Transaction.OperationsPerTransaction = OperationBudget{Min: 1, Max: 1}
	profile.Transaction.OperationMix = map[string]OperationProfile{"createEdge": {Weight: 1, RequiresExistingNodes: 2}}
	profile.Transaction.FallbackOperation = "createNode"
	tx := GenerateGraphTransaction("actor-1", 1, 1, profile, KnownState{Edges: map[string]bool{}})
	if len(tx.Operations) != 1 || tx.Operations[0].Type != oracle.OperationCreateNode {
		t.Fatalf("operation=%+v, want createNode fallback", tx.Operations)
	}
}

func TestGraphGeneratorHonorsMaxPerTransaction(t *testing.T) {
	profile := DefaultGraphProfile()
	profile.Transaction.OperationsPerTransaction = OperationBudget{Min: 5, Max: 5}
	profile.Transaction.OperationMix = map[string]OperationProfile{"createNode": {Weight: 1, MaxPerTransaction: 2}}
	profile.Transaction.FallbackOperation = "createNode"
	tx := GenerateGraphTransaction("actor-1", 1, 1, profile, KnownState{Edges: map[string]bool{}})
	if len(tx.Operations) != 2 {
		t.Fatalf("operations=%d, want constrained max 2", len(tx.Operations))
	}
}

func TestGraphGeneratorDoesNotEmitDeletes(t *testing.T) {
	profile := testGraphProfile()
	state := KnownState{Nodes: []string{"n1", "n2"}, Edges: map[string]bool{}}
	for seq := int64(1); seq <= 100; seq++ {
		tx := GenerateGraphTransaction("actor-1", 77, seq, profile, state)
		for _, op := range tx.Operations {
			if op.Type == "deleteNode" || op.Type == "deleteEdge" {
				t.Fatalf("delete operation generated: %+v", op)
			}
		}
	}
}

func TestAcknowledgedTransactionsUpdateExpectedCounters(t *testing.T) {
	state := KnownState{Edges: map[string]bool{}}
	tx := oracle.Transaction{Acknowledged: true, Operations: []oracle.Operation{{Type: oracle.OperationCreateNode, NodeID: "n1"}, {Type: oracle.OperationCreateNode, NodeID: "n2"}, {Type: oracle.OperationCreateEdge, FromID: "n1", ToID: "n2"}}}
	ApplyAcknowledged(&state, tx)
	if len(state.Nodes) != 2 || len(state.Edges) != 1 {
		t.Fatalf("state=%+v, want 2 nodes 1 edge", state)
	}
	counts := oracle.ExpectedCounts([]oracle.Transaction{tx})
	if counts.Nodes != 2 || counts.Edges != 1 {
		t.Fatalf("counts=%+v, want 2 nodes 1 edge", counts)
	}
}

func TestGraphActorRecordsAcknowledgedTransactions(t *testing.T) {
	group := spec.ResolvedActorGroup{ActorGroupSpec: spec.ActorGroupSpec{Name: "writers"}, Profile: spec.ActorProfile{Behavior: map[string]any{"type": "graph-transaction", "transaction": map[string]any{"operationsPerTransaction": map[string]any{"min": 1, "max": 1}, "operationMix": map[string]any{"createNode": map[string]any{"weight": 1}}}}}}
	recorder := &actorRecorder{}
	actor, err := NewGraphActor(group, 0, 5, spec.RateSpec{Mode: "group-total", CommitsPerSecond: 50}, recorder)
	if err != nil {
		t.Fatalf("NewGraphActor() error=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := actor.Start(ctx); err != nil {
		t.Fatalf("Start() error=%v", err)
	}
	<-ctx.Done()
	_ = actor.Stop(context.Background())
	if len(actor.Transactions()) == 0 {
		t.Fatal("graph actor recorded no transactions")
	}
	if recorder.acknowledged == 0 {
		t.Fatal("recorder saw no acknowledged transaction events")
	}
}

func TestClassifyErrorTreatsUnauthenticatedAsAuthentication(t *testing.T) {
	err := status.Error(codes.Unauthenticated, "authorization token is invalid")
	if got := ClassifyError(err); got != "authentication" {
		t.Fatalf("ClassifyError()=%q, want authentication", got)
	}
}

func TestClientConfigForAssignmentForcesPasswordLogin(t *testing.T) {
	assignment := provision.ActorAssignment{DaemonAddr: "127.0.0.1:19091", Username: "user", Password: "pass", AccessToken: "old-access", RefreshToken: "old-refresh"}
	tokenCfg := clientConfigForAssignment(assignment, false)
	if tokenCfg.AccessToken != "old-access" || tokenCfg.RefreshToken != "old-refresh" || tokenCfg.Username != "" || tokenCfg.Password != "" {
		t.Fatalf("token config=%+v, want token-only auth", tokenCfg)
	}
	loginCfg := clientConfigForAssignment(assignment, true)
	if loginCfg.AccessToken != "" || loginCfg.RefreshToken != "" || loginCfg.Username != "user" || loginCfg.Password != "pass" {
		t.Fatalf("forced login config=%+v, want username/password auth", loginCfg)
	}
}

func TestShouldReauthenticateRequiresCredentials(t *testing.T) {
	err := status.Error(codes.Unauthenticated, "authorization token is invalid")
	assignment := provision.ActorAssignment{DaemonAddr: "127.0.0.1:19091", Username: "user", Password: "pass"}
	if !shouldReauthenticate(err, assignment) {
		t.Fatal("shouldReauthenticate()=false, want true")
	}
	assignment.Password = ""
	if shouldReauthenticate(err, assignment) {
		t.Fatal("shouldReauthenticate()=true without password, want false")
	}
}

func testGraphProfile() GraphProfile {
	profile := DefaultGraphProfile()
	profile.Transaction.OperationsPerTransaction = OperationBudget{Min: 1, Max: 1}
	profile.Transaction.OperationMix = map[string]OperationProfile{
		"createNode": {Weight: 60, MaxPerTransaction: 1},
		"updateNode": {Weight: 20, MaxPerTransaction: 1, RequiresExistingNode: true},
		"createEdge": {Weight: 20, MaxPerTransaction: 1, RequiresExistingNodes: 2},
	}
	profile.Transaction.DataModel.NodeLabels = []string{"Note"}
	profile.Transaction.DataModel.EdgeTypes = []string{"LINKS_TO"}
	return profile
}

type actorRecorder struct{ acknowledged int }

func (r *actorRecorder) RecordActorEvent(_ context.Context, event ActorEvent) error {
	if event.Type == "graph-transaction-acknowledged" {
		r.acknowledged++
	}
	return nil
}
