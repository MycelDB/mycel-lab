package actors

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type EventRecorder interface {
	RecordActorEvent(ctx context.Context, event ActorEvent) error
}

type ActorEvent struct {
	Type        string             `json:"type"`
	ActorID     string             `json:"actorId"`
	GroupName   string             `json:"groupName"`
	Sequence    int64              `json:"sequence,omitempty"`
	Transaction oracle.Transaction `json:"transaction,omitempty"`
	Payload     map[string]any     `json:"payload,omitempty"`
}

type GraphProfile struct {
	Transaction GraphTransactionProfile `json:"transaction"`
	Retry       RetryProfile            `json:"retry"`
	Checks      GraphChecksProfile      `json:"checks"`
}

type GraphTransactionProfile struct {
	OperationsPerTransaction OperationBudget             `json:"operationsPerTransaction"`
	OperationMix             map[string]OperationProfile `json:"operationMix"`
	FallbackOperation        string                      `json:"fallbackOperation"`
	DataModel                DataModelProfile            `json:"dataModel"`
}

type OperationBudget struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type OperationProfile struct {
	Weight                int  `json:"weight"`
	MaxPerTransaction     int  `json:"maxPerTransaction"`
	RequiresExistingNode  bool `json:"requiresExistingNode"`
	RequiresExistingNodes int  `json:"requiresExistingNodes"`
}

type DataModelProfile struct {
	NodeLabels []string `json:"nodeLabels"`
	EdgeTypes  []string `json:"edgeTypes"`
}

type RetryProfile struct {
	MaxAttempts    int           `json:"maxAttempts"`
	InitialBackoff spec.Duration `json:"initialBackoff"`
	MaxBackoff     spec.Duration `json:"maxBackoff"`
	RetryOn        []string      `json:"retryOn"`
}

type GraphChecksProfile struct {
	ReadAfterWrite ReadAfterWriteProfile `json:"readAfterWrite"`
}

type ReadAfterWriteProfile struct {
	Enabled            bool          `json:"enabled"`
	SampleEveryCommits int           `json:"sampleEveryCommits"`
	MaxAttempts        int           `json:"maxAttempts"`
	Backoff            spec.Duration `json:"backoff"`
}

type KnownState struct {
	Nodes []string        `json:"nodes"`
	Edges map[string]bool `json:"edges"`
}

func DefaultGraphProfile() GraphProfile {
	return GraphProfile{Transaction: GraphTransactionProfile{OperationsPerTransaction: OperationBudget{Min: 1, Max: 1}, OperationMix: map[string]OperationProfile{"createNode": {Weight: 1, MaxPerTransaction: 1}}, FallbackOperation: "createNode", DataModel: DataModelProfile{NodeLabels: []string{"Node"}, EdgeTypes: []string{"LINKS_TO"}}}, Retry: RetryProfile{MaxAttempts: 1}}
}

func GraphProfileFromBehavior(behavior map[string]any) (GraphProfile, error) {
	profile := DefaultGraphProfile()
	raw, err := json.Marshal(behavior)
	if err != nil {
		return GraphProfile{}, err
	}
	if err := json.Unmarshal(raw, &profile); err != nil {
		return GraphProfile{}, err
	}
	if profile.Transaction.OperationsPerTransaction.Min <= 0 {
		profile.Transaction.OperationsPerTransaction.Min = 1
	}
	if profile.Transaction.OperationsPerTransaction.Max < profile.Transaction.OperationsPerTransaction.Min {
		profile.Transaction.OperationsPerTransaction.Max = profile.Transaction.OperationsPerTransaction.Min
	}
	if len(profile.Transaction.OperationMix) == 0 {
		profile.Transaction.OperationMix = map[string]OperationProfile{"createNode": {Weight: 1, MaxPerTransaction: profile.Transaction.OperationsPerTransaction.Max}}
	}
	if profile.Transaction.FallbackOperation == "" {
		profile.Transaction.FallbackOperation = "createNode"
	}
	if len(profile.Transaction.DataModel.NodeLabels) == 0 {
		profile.Transaction.DataModel.NodeLabels = []string{"Node"}
	}
	if len(profile.Transaction.DataModel.EdgeTypes) == 0 {
		profile.Transaction.DataModel.EdgeTypes = []string{"LINKS_TO"}
	}
	return profile, nil
}

func GenerateGraphTransaction(actorID string, seed int64, sequence int64, profile GraphProfile, state KnownState) oracle.Transaction {
	rng := rand.New(rand.NewSource(seed + sequence*7919))
	budget := profile.Transaction.OperationsPerTransaction.Min
	if max := profile.Transaction.OperationsPerTransaction.Max; max > budget {
		budget += rng.Intn(max - budget + 1)
	}
	working := cloneKnownState(state)
	perType := map[oracle.OperationType]int{}
	tx := oracle.Transaction{ActorID: actorID, Sequence: sequence, Acknowledged: true}
	for len(tx.Operations) < budget {
		selected, ok := selectPossibleOperation(rng, profile, working, perType)
		if !ok {
			break
		}
		op := buildOperation(rng, actorID, sequence, len(tx.Operations), selected, profile, &working)
		tx.Operations = append(tx.Operations, op)
		perType[selected]++
	}
	return tx
}

func ApplyAcknowledged(state *KnownState, tx oracle.Transaction) {
	if !tx.Acknowledged {
		return
	}
	if state.Edges == nil {
		state.Edges = map[string]bool{}
	}
	for _, op := range tx.Operations {
		switch op.Type {
		case oracle.OperationCreateNode:
			state.Nodes = append(state.Nodes, op.NodeID)
		case oracle.OperationCreateEdge:
			state.Edges[op.FromID+"->"+op.ToID] = true
		}
	}
}

func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case containsAny(msg, "unavailable", "connection refused", "deadline exceeded", "server preface", "context canceled", "canceled"):
		return "unavailable"
	case containsAny(msg, "transient", "timeout", "temporarily"):
		return "transient"
	default:
		return "permanent"
	}
}

type GraphActor struct {
	GroupName  string
	Index      int
	ID         string
	Seed       int64
	Rate       spec.RateSpec
	Profile    GraphProfile
	Recorder   EventRecorder
	Assignment provision.ActorAssignment

	mu           sync.Mutex
	state        KnownState
	sequence     int64
	transactions []oracle.Transaction
	cancel       context.CancelFunc
	done         chan struct{}
}

func NewGraphActor(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec, recorder EventRecorder) (*GraphActor, error) {
	profile, err := GraphProfileFromBehavior(group.Profile.Behavior)
	if err != nil {
		return nil, err
	}
	return &GraphActor{GroupName: group.Name, Index: index, ID: fmt.Sprintf("%s-%d", group.Name, index), Seed: seed, Rate: rate, Profile: profile, Recorder: recorder, state: KnownState{Edges: map[string]bool{}}}, nil
}

func (a *GraphActor) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.done != nil {
		a.mu.Unlock()
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.mu.Unlock()
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-started", ActorID: a.ID, GroupName: a.GroupName})
	}
	go a.loop(child)
	return nil
}

func (a *GraphActor) UpdateRate(ctx context.Context, rate spec.RateSpec) error {
	a.mu.Lock()
	a.Rate = rate
	a.mu.Unlock()
	if a.Recorder != nil {
		return a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-rate-updated", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"commitsPerSecond": rate.CommitsPerSecond, "queriesPerSecond": rate.QueriesPerSecond}})
	}
	return nil
}

func (a *GraphActor) Stop(ctx context.Context) error {
	a.mu.Lock()
	cancel := a.cancel
	done := a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if a.Recorder != nil {
		return a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-stopped", ActorID: a.ID, GroupName: a.GroupName})
	}
	return nil
}

func (a *GraphActor) Transactions() []oracle.Transaction {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]oracle.Transaction(nil), a.transactions...)
}

func (a *GraphActor) loop(ctx context.Context) {
	defer close(a.done)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	lastCommit := time.Now()
	lastRead := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			commitInterval, readInterval := a.intervals()
			if commitInterval > 0 && now.Sub(lastCommit) >= commitInterval {
				opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				a.commit(opCtx)
				cancel()
				lastCommit = now
			}
			if readInterval > 0 && now.Sub(lastRead) >= readInterval {
				opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				a.read(opCtx)
				cancel()
				lastRead = now
			}
		}
	}
}

func (a *GraphActor) intervals() (time.Duration, time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var commitInterval time.Duration
	if a.Rate.CommitsPerSecond > 0 {
		commitInterval = time.Duration(float64(time.Second) / a.Rate.CommitsPerSecond)
	}
	var readInterval time.Duration
	if a.Rate.QueriesPerSecond > 0 {
		readInterval = time.Duration(float64(time.Second) / a.Rate.QueriesPerSecond)
	}
	return commitInterval, readInterval
}

func (a *GraphActor) commit(ctx context.Context) {
	a.mu.Lock()
	a.sequence++
	tx := GenerateGraphTransaction(a.ID, a.Seed, a.sequence, a.Profile, a.state)
	a.mu.Unlock()
	if err := a.executeRealTransaction(ctx, tx); err != nil {
		tx.Acknowledged = false
		tx.ErrorClass = ClassifyError(err)
		if a.Recorder != nil {
			_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-transaction-failed", ActorID: a.ID, GroupName: a.GroupName, Sequence: tx.Sequence, Transaction: tx, Payload: map[string]any{"error": err.Error(), "class": tx.ErrorClass}})
		}
		return
	}
	a.mu.Lock()
	ApplyAcknowledged(&a.state, tx)
	a.transactions = append(a.transactions, tx)
	a.mu.Unlock()
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-transaction-acknowledged", ActorID: a.ID, GroupName: a.GroupName, Sequence: tx.Sequence, Transaction: tx, Payload: map[string]any{"spaceId": a.Assignment.SpaceID, "domainId": a.Assignment.DomainID, "daemonAddr": a.Assignment.DaemonAddr}})
	}
}

func (a *GraphActor) read(ctx context.Context) {
	if a.Assignment.DaemonAddr == "" || a.Assignment.SpaceID == "" || a.Assignment.DomainID == "" || (a.Assignment.AccessToken == "" && (a.Assignment.Username == "" || a.Assignment.Password == "")) {
		if a.Recorder != nil {
			_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"ok": true, "simulated": true}})
		}
		return
	}
	client, err := a.dialClient(ctx)
	if err == nil {
		_, err = client.QueryGQLReadOnly(ctx, a.Assignment.SpaceID, a.Assignment.DomainID, "MATCH (n) RETURN count(n)", 10)
		_ = client.Close()
	}
	payload := map[string]any{"ok": err == nil, "spaceId": a.Assignment.SpaceID, "domainId": a.Assignment.DomainID, "daemonAddr": a.Assignment.DaemonAddr, "targetActorId": a.ID}
	if err != nil {
		payload["error"] = err.Error()
		payload["class"] = ClassifyError(err)
	}
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: payload})
	}
}

func (a *GraphActor) executeRealTransaction(ctx context.Context, tx oracle.Transaction) error {
	if a.Assignment.DaemonAddr == "" || a.Assignment.SpaceID == "" || a.Assignment.DomainID == "" || (a.Assignment.AccessToken == "" && (a.Assignment.Username == "" || a.Assignment.Password == "")) {
		return nil
	}
	client, err := a.dialClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	sessionID, err := client.OpenSession(ctx, a.Assignment.SpaceID, a.Assignment.DomainID)
	if err != nil {
		return err
	}
	defer func() { _ = client.CloseSession(context.Background(), sessionID) }()
	graphTx, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = client.CloseTransaction(context.Background(), graphTx)
		}
	}()
	for _, op := range tx.Operations {
		props, _ := structpb.NewStruct(op.Properties)
		switch op.Type {
		case oracle.OperationCreateNode:
			nodeID := op.NodeID
			payload, _ := structpb.NewStruct(map[string]any{"text": fmt.Sprintf("%s %d", a.ID, tx.Sequence)})
			if _, err := client.CreateNode(ctx, graphTx, &clientv1.NodeCreate{NodeId: &nodeID, Labels: []string{op.Label}, Properties: props, Payload: payload}); err != nil {
				return err
			}
		case oracle.OperationUpdateNode:
			if _, err := client.UpdateNodeContent(ctx, graphTx, op.NodeID, fmt.Sprintf("updated by %s seq %d", a.ID, tx.Sequence)); err != nil {
				return err
			}
		case oracle.OperationCreateEdge:
			if _, err := client.CreateEdge(ctx, graphTx, &clientv1.EdgeCreate{FromNodeId: op.FromID, ToNodeId: op.ToID, Labels: []string{op.EdgeType}, Properties: props}); err != nil {
				return err
			}
		}
	}
	if err := client.CommitTransaction(ctx, graphTx); err != nil {
		return err
	}
	committed = true
	return nil
}

func (a *GraphActor) dialClient(ctx context.Context) (*mycel.Client, error) {
	cfg := mycel.Config{Addr: a.Assignment.DaemonAddr, Username: a.Assignment.Username, Password: a.Assignment.Password, ClientName: "mycel-lab"}
	if a.Assignment.AccessToken != "" {
		cfg.Username = ""
		cfg.Password = ""
		cfg.AccessToken = a.Assignment.AccessToken
		cfg.RefreshToken = a.Assignment.RefreshToken
		cfg.AccessTokenExpireTime = a.Assignment.TokenExpires
	}
	return mycel.Dial(ctx, cfg)
}

func selectPossibleOperation(rng *rand.Rand, profile GraphProfile, state KnownState, perType map[oracle.OperationType]int) (oracle.OperationType, bool) {
	mix := profile.Transaction.OperationMix
	selected := weightedOperation(rng, mix)
	if operationPossible(selected, mix[string(selected)], state, perType) {
		return selected, true
	}
	fallback := oracle.OperationType(profile.Transaction.FallbackOperation)
	if operationPossible(fallback, mix[string(fallback)], state, perType) {
		return fallback, true
	}
	keys := make([]string, 0, len(mix))
	for typ := range mix {
		keys = append(keys, typ)
	}
	sort.Strings(keys)
	for _, typ := range keys {
		candidate := oracle.OperationType(typ)
		if operationPossible(candidate, mix[typ], state, perType) {
			return candidate, true
		}
	}
	return "", false
}

func weightedOperation(rng *rand.Rand, mix map[string]OperationProfile) oracle.OperationType {
	keys := make([]string, 0, len(mix))
	for typ := range mix {
		keys = append(keys, typ)
	}
	sort.Strings(keys)
	total := 0
	for _, typ := range keys {
		profile := mix[typ]
		if isV1Operation(oracle.OperationType(typ)) && profile.Weight > 0 {
			total += profile.Weight
		}
	}
	if total <= 0 {
		return oracle.OperationCreateNode
	}
	pick := rng.Intn(total)
	for _, typ := range keys {
		profile := mix[typ]
		if !isV1Operation(oracle.OperationType(typ)) || profile.Weight <= 0 {
			continue
		}
		if pick < profile.Weight {
			return oracle.OperationType(typ)
		}
		pick -= profile.Weight
	}
	return oracle.OperationCreateNode
}

func operationPossible(typ oracle.OperationType, profile OperationProfile, state KnownState, perType map[oracle.OperationType]int) bool {
	if !isV1Operation(typ) {
		return false
	}
	if profile.MaxPerTransaction > 0 && perType[typ] >= profile.MaxPerTransaction {
		return false
	}
	switch typ {
	case oracle.OperationCreateNode:
		return true
	case oracle.OperationUpdateNode:
		return len(state.Nodes) > 0 && (!profile.RequiresExistingNode || len(state.Nodes) >= 1)
	case oracle.OperationCreateEdge:
		required := profile.RequiresExistingNodes
		if required == 0 {
			required = 2
		}
		return len(state.Nodes) >= required
	default:
		return false
	}
}

func buildOperation(rng *rand.Rand, actorID string, sequence int64, index int, typ oracle.OperationType, profile GraphProfile, state *KnownState) oracle.Operation {
	switch typ {
	case oracle.OperationUpdateNode:
		nodeID := state.Nodes[rng.Intn(len(state.Nodes))]
		return oracle.Operation{Type: typ, NodeID: nodeID, Properties: map[string]any{"updatedBy": actorID, "transactionSeq": sequence}}
	case oracle.OperationCreateEdge:
		from := state.Nodes[rng.Intn(len(state.Nodes))]
		to := state.Nodes[rng.Intn(len(state.Nodes))]
		if len(state.Nodes) > 1 {
			for to == from {
				to = state.Nodes[rng.Intn(len(state.Nodes))]
			}
		}
		edgeType := profile.Transaction.DataModel.EdgeTypes[rng.Intn(len(profile.Transaction.DataModel.EdgeTypes))]
		return oracle.Operation{Type: typ, FromID: from, ToID: to, EdgeType: edgeType, Properties: map[string]any{"createdBy": actorID, "transactionSeq": sequence}}
	default:
		nodeID := stableUUID(fmt.Sprintf("%s-node-%d-%d", actorID, sequence, index))
		label := profile.Transaction.DataModel.NodeLabels[rng.Intn(len(profile.Transaction.DataModel.NodeLabels))]
		state.Nodes = append(state.Nodes, nodeID)
		return oracle.Operation{Type: oracle.OperationCreateNode, NodeID: nodeID, Label: label, Properties: map[string]any{"createdBy": actorID, "transactionSeq": sequence}}
	}
}

func stableUUID(value string) string {
	sum := sha1.Sum([]byte(value))
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func cloneKnownState(in KnownState) KnownState {
	out := KnownState{Nodes: append([]string(nil), in.Nodes...), Edges: map[string]bool{}}
	for key, value := range in.Edges {
		out.Edges[key] = value
	}
	return out
}

func isV1Operation(typ oracle.OperationType) bool {
	return typ == oracle.OperationCreateNode || typ == oracle.OperationUpdateNode || typ == oracle.OperationCreateEdge
}

func containsAny(s string, needles ...string) bool {
	for _, needle := range needles {
		if len(needle) > 0 && contains(s, needle) {
			return true
		}
	}
	return false
}

func contains(s, needle string) bool {
	for i := 0; i+len(needle) <= len(s); i++ {
		if s[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
