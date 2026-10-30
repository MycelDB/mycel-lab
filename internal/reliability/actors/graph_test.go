package actors

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
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

func TestGraphCommitRetriesCommittedResponseLossWithSameIdentity(t *testing.T) {
	profile := testGraphProfile()
	profile.Retry = RetryProfile{
		MaxAttempts:    4,
		InitialBackoff: spec.Duration{Duration: time.Nanosecond},
		MaxBackoff:     spec.Duration{Duration: 2 * time.Nanosecond},
		RetryOn:        []string{"transient", "unavailable"},
	}
	recorder := &actorRecorder{}
	actor := &GraphActor{ID: "graph-writers-1", GroupName: "graph-writers", Profile: profile, Recorder: recorder}
	client := &ambiguousCommitGraphClient{}
	assignment := provision.ActorAssignment{SpaceID: "space-1", DomainID: "domain-1"}
	tx := oracle.Transaction{
		Sequence: 210,
		Operations: []oracle.Operation{{
			Type:       oracle.OperationCreateNode,
			NodeID:     "node-210",
			Label:      "Note",
			Properties: map[string]any{"ordinal": float64(210)},
		}},
	}

	if err := actor.executeGraphTransaction(context.Background(), client, assignment, "session-1", tx); err != nil {
		t.Fatalf("executeGraphTransaction() error=%v", err)
	}
	if client.beginCalls != 1 || client.createNodeCalls != 1 || client.commitCalls != 2 {
		t.Fatalf("begin=%d create=%d commit=%d, want 1, 1, 2", client.beginCalls, client.createNodeCalls, client.commitCalls)
	}
	if client.retryPolicy.MaxAttempts != 4 {
		t.Fatalf("commit retry max attempts=%d, want 4", client.retryPolicy.MaxAttempts)
	}
	if client.closeCalls != 0 {
		t.Fatalf("close calls=%d, want 0 after idempotent replay acknowledgment", client.closeCalls)
	}
	if !client.serverApplied {
		t.Fatal("simulated server never applied the transaction")
	}
	if len(client.commitIdentities) != 2 || client.commitIdentities[0] != client.commitIdentities[1] {
		t.Fatalf("commit identities=%v, want the same transaction object on both attempts", client.commitIdentities)
	}
	wantKey := graphCommitIdempotencyKey(actor.ID, tx.Sequence)
	if client.idempotencyKey != wantKey {
		t.Fatalf("idempotency key=%q, want %q", client.idempotencyKey, wantKey)
	}
	if len(recorder.events) != 1 || recorder.events[0].Type != "graph-transaction-commit-retry" {
		t.Fatalf("events=%+v, want one graph-transaction-commit-retry", recorder.events)
	}
}

func TestGraphCommitExhaustionRecordsUnresolvedTransaction(t *testing.T) {
	profile := testGraphProfile()
	profile.Retry = RetryProfile{MaxAttempts: 2, RetryOn: []string{"unavailable"}}
	recorder := &actorRecorder{}
	actor := &GraphActor{ID: "graph-writers-1", GroupName: "graph-writers", Profile: profile, Recorder: recorder}
	client := &ambiguousCommitGraphClient{exhaust: true}
	tx := oracle.Transaction{Sequence: 211, Operations: []oracle.Operation{{Type: oracle.OperationCreateNode, NodeID: "node-211", Label: "Note"}}}
	err := actor.executeGraphTransaction(context.Background(), client, provision.ActorAssignment{SpaceID: "space-1", DomainID: "domain-1"}, "session-1", tx)
	if err == nil {
		t.Fatal("executeGraphTransaction() error=nil, want unresolved ambiguous commit")
	}
	if recorder.unresolved != 1 || client.createNodeCalls != 1 || client.commitCalls != 2 {
		t.Fatalf("unresolved=%d creates=%d commits=%d, want 1, 1, 2", recorder.unresolved, client.createNodeCalls, client.commitCalls)
	}
}

func TestGraphCommitRetryFailsClosedForUnconfiguredError(t *testing.T) {
	profile := RetryProfile{MaxAttempts: 4, RetryOn: []string{"unavailable"}}
	err := status.Error(codes.Internal, "ambiguous but not allowlisted")
	if graphCommitRetryable(profile, err) {
		t.Fatal("graphCommitRetryable()=true for a non-allowlisted category")
	}
}

func TestGraphCommitRetryAcceptsPlainLostEOF(t *testing.T) {
	profile := RetryProfile{MaxAttempts: 2, RetryOn: []string{"unavailable"}}
	if !graphCommitRetryable(profile, io.EOF) {
		t.Fatal("graphCommitRetryable(io.EOF)=false, want true")
	}
}

func TestGraphCommitBackoffIsCapped(t *testing.T) {
	profile := RetryProfile{
		InitialBackoff: spec.Duration{Duration: time.Nanosecond},
		MaxBackoff:     spec.Duration{Duration: 2 * time.Nanosecond},
	}
	want := []time.Duration{time.Nanosecond, 2 * time.Nanosecond, 2 * time.Nanosecond}
	for index, expected := range want {
		if got := graphCommitRetryDelay(profile, index+1); got != expected {
			t.Fatalf("graphCommitRetryDelay(attempt=%d)=%s, want %s", index+1, got, expected)
		}
	}
	profile.InitialBackoff.Duration = time.Duration(1 << 62)
	profile.MaxBackoff.Duration = 0
	if got := graphCommitRetryDelay(profile, 2); got <= 0 {
		t.Fatalf("overflow boundary delay=%s, want positive saturation", got)
	}
}

func TestGraphCommitIdempotencyKeyIsStablePerActorSequence(t *testing.T) {
	first := graphCommitIdempotencyKey("graph-writers-1", 210)
	second := graphCommitIdempotencyKey("graph-writers-1", 210)
	other := graphCommitIdempotencyKey("graph-writers-1", 211)
	if first != second {
		t.Fatalf("same actor sequence produced different keys: %q != %q", first, second)
	}
	if first == other {
		t.Fatalf("different sequence produced same key: %q", first)
	}
	if len(first) > 128 {
		t.Fatalf("idempotency key length=%d, want <=128", len(first))
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

type actorRecorder struct {
	acknowledged int
	unresolved   int
	events       []ActorEvent
}

func (r *actorRecorder) RecordActorEvent(_ context.Context, event ActorEvent) error {
	if event.Type == "graph-transaction-acknowledged" {
		r.acknowledged++
	}
	if event.Type == "graph-transaction-commit-retry" {
		r.events = append(r.events, event)
	}
	if event.Type == "graph-transaction-commit-unresolved" {
		r.unresolved++
	}
	return nil
}

type ambiguousCommitGraphClient struct {
	beginCalls       int
	createNodeCalls  int
	commitCalls      int
	closeCalls       int
	serverApplied    bool
	idempotencyKey   string
	commitIdentities []*mycel.IdempotentTransaction
	retryPolicy      mycel.IdempotentCommitRetryPolicy
	exhaust          bool
}

func (c *ambiguousCommitGraphClient) BeginIdempotentReadWriteTransaction(_ context.Context, _ string, spaceID, domainID, key string) (*mycel.IdempotentTransaction, error) {
	c.beginCalls++
	c.idempotencyKey = key
	return &mycel.IdempotentTransaction{
		Transaction: &clientv1.GraphTransaction{TransactionId: "tx-210"},
		CommitOptions: mycel.CommitTransactionOptions{
			IdempotencyKey: key,
			SpaceID:        spaceID,
			DomainID:       domainID,
		},
	}, nil
}

func (c *ambiguousCommitGraphClient) CloseTransaction(context.Context, string) error {
	c.closeCalls++
	return nil
}

func (c *ambiguousCommitGraphClient) CreateNode(_ context.Context, _ string, _ *clientv1.NodeCreate) (*clientv1.Node, error) {
	c.createNodeCalls++
	return &clientv1.Node{}, nil
}

func (c *ambiguousCommitGraphClient) UpdateNodeContent(context.Context, string, string, string) (*clientv1.Node, error) {
	return &clientv1.Node{}, nil
}

func (c *ambiguousCommitGraphClient) CreateEdge(context.Context, string, *clientv1.EdgeCreate) (*clientv1.Edge, error) {
	return &clientv1.Edge{}, nil
}

func (c *ambiguousCommitGraphClient) CommitIdempotentTransactionWithRetry(_ context.Context, tx *mycel.IdempotentTransaction, policy mycel.IdempotentCommitRetryPolicy) (*clientv1.CommitTransactionResponse, error) {
	c.retryPolicy = policy
	lostResponse := io.EOF
	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c.commitCalls++
		c.commitIdentities = append(c.commitIdentities, tx)
		c.serverApplied = true
		if attempt > 1 && !c.exhaust {
			return &clientv1.CommitTransactionResponse{}, nil
		}
		if attempt == maxAttempts || policy.Retryable == nil || !policy.Retryable(lostResponse) {
			return nil, lostResponse
		}
		if policy.Backoff != nil {
			_ = policy.Backoff(attempt)
		}
	}
	return nil, lostResponse
}
