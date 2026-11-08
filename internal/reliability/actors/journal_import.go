package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// JournalImportProfile describes the graph shape emitted by the Commonfolio
// importer without depending on source files or credentials. It intentionally
// uses the daemon's BeginReadWriteTransaction + ApplyGraphOperations +
// CommitTransaction API rather than the generic import stream: that is the
// request shape used by the importer being investigated.
type JournalImportProfile struct {
	Transaction JournalImportTransactionProfile `json:"transaction"`
}

type JournalImportTransactionProfile struct {
	JournalsPerTransaction   int    `json:"journalsPerTransaction"`
	EntriesPerJournal        int    `json:"entriesPerJournal"`
	LinksPerEntry            int    `json:"linksPerEntry"`
	ExistingPageCount        int    `json:"existingPageCount"`
	SameTransactionPageCount int    `json:"sameTransactionPageCount"`
	PageTargetMode           string `json:"pageTargetMode"`
	JournalPayloadBytes      int    `json:"journalPayloadBytes"`
	EntryPayloadBytes        int    `json:"entryPayloadBytes"`
	PagePayloadBytes         int    `json:"pagePayloadBytes"`
	IndexedProperties        bool   `json:"indexedProperties"`
	ApplyChunkOperations     int    `json:"applyChunkOperations"`
	Atomic                   bool   `json:"atomic"`
}

const (
	journalImportMaxChunkProtoSize     = 1 << 20
	journalImportTargetExistingReused  = "existing-reused"
	journalImportTargetExistingUnique  = "existing-unique"
	journalImportTargetSameTransaction = "same-transaction"
	journalImportTargetMixed           = "mixed"
)

func DefaultJournalImportProfile() JournalImportProfile {
	return JournalImportProfile{Transaction: JournalImportTransactionProfile{
		JournalsPerTransaction:   1,
		EntriesPerJournal:        256,
		LinksPerEntry:            2,
		ExistingPageCount:        64,
		SameTransactionPageCount: 64,
		PageTargetMode:           journalImportTargetExistingReused,
		EntryPayloadBytes:        3072,
		PagePayloadBytes:         0,
		IndexedProperties:        true,
		ApplyChunkOperations:     100,
		Atomic:                   true,
	}}
}

func JournalImportProfileFromBehavior(behavior map[string]any) (JournalImportProfile, error) {
	profile := DefaultJournalImportProfile()
	raw, err := json.Marshal(behavior)
	if err != nil {
		return JournalImportProfile{}, err
	}
	if err := json.Unmarshal(raw, &profile); err != nil {
		return JournalImportProfile{}, err
	}
	if profile.Transaction.JournalsPerTransaction <= 0 || profile.Transaction.EntriesPerJournal <= 0 {
		return JournalImportProfile{}, fmt.Errorf("transaction journalsPerTransaction and entriesPerJournal must be positive")
	}
	if profile.Transaction.LinksPerEntry < 0 {
		return JournalImportProfile{}, fmt.Errorf("transaction.linksPerEntry must not be negative")
	}
	for name, value := range map[string]int{
		"journalPayloadBytes": profile.Transaction.JournalPayloadBytes,
		"entryPayloadBytes":   profile.Transaction.EntryPayloadBytes,
		"pagePayloadBytes":    profile.Transaction.PagePayloadBytes,
	} {
		if value < 0 || value > 1<<20 {
			return JournalImportProfile{}, fmt.Errorf("transaction.%s must be between 0 and %d", name, 1<<20)
		}
	}
	if profile.Transaction.ExistingPageCount < 0 || profile.Transaction.SameTransactionPageCount < 0 {
		return JournalImportProfile{}, fmt.Errorf("transaction page counts must not be negative")
	}
	if profile.Transaction.ApplyChunkOperations <= 0 {
		profile.Transaction.ApplyChunkOperations = 100
	}
	if !profile.Transaction.Atomic {
		return JournalImportProfile{}, fmt.Errorf("transaction.atomic must be true; this profile never silently splits an import")
	}
	requiredTargets := profile.Transaction.JournalsPerTransaction * profile.Transaction.EntriesPerJournal * profile.Transaction.LinksPerEntry
	switch profile.Transaction.PageTargetMode {
	case journalImportTargetExistingReused:
		if profile.Transaction.ExistingPageCount == 0 {
			return JournalImportProfile{}, fmt.Errorf("transaction.existingPageCount must be positive for pageTargetMode %q", profile.Transaction.PageTargetMode)
		}
	case journalImportTargetExistingUnique:
		if profile.Transaction.ExistingPageCount < requiredTargets {
			return JournalImportProfile{}, fmt.Errorf("transaction.existingPageCount=%d is too small for %d unique page targets", profile.Transaction.ExistingPageCount, requiredTargets)
		}
	case journalImportTargetSameTransaction:
		if profile.Transaction.SameTransactionPageCount < requiredTargets {
			return JournalImportProfile{}, fmt.Errorf("transaction.sameTransactionPageCount=%d is too small for %d unique page targets", profile.Transaction.SameTransactionPageCount, requiredTargets)
		}
	case journalImportTargetMixed:
		existingTargets := (requiredTargets + 1) / 2
		sameTargets := requiredTargets / 2
		if profile.Transaction.ExistingPageCount < existingTargets || profile.Transaction.SameTransactionPageCount < sameTargets {
			return JournalImportProfile{}, fmt.Errorf("transaction page populations are too small for mixed unique targets")
		}
	default:
		return JournalImportProfile{}, fmt.Errorf("transaction.pageTargetMode %q is unsupported", profile.Transaction.PageTargetMode)
	}
	samePages := 0
	if profile.Transaction.PageTargetMode == journalImportTargetSameTransaction || profile.Transaction.PageTargetMode == journalImportTargetMixed {
		samePages = profile.Transaction.SameTransactionPageCount
	}
	referenceEdgesPerEntry := profile.Transaction.LinksPerEntry
	if profile.Transaction.PageTargetMode == journalImportTargetExistingReused && referenceEdgesPerEntry > 0 {
		referenceEdgesPerEntry = 1
	}
	estimatedOperations := profile.Transaction.JournalsPerTransaction*(1+profile.Transaction.EntriesPerJournal) + samePages + profile.Transaction.JournalsPerTransaction*profile.Transaction.EntriesPerJournal*(1+referenceEdgesPerEntry)
	if estimatedOperations > 1000 {
		return JournalImportProfile{}, fmt.Errorf("transaction shape estimates %d operations; reduce it below the importer atomic threshold of 1000", estimatedOperations)
	}
	return profile, nil
}

type JournalImportActor struct {
	GroupName  string
	Index      int
	ID         string
	Seed       int64
	Rate       spec.RateSpec
	Profile    JournalImportProfile
	Recorder   EventRecorder
	Assignment provision.ActorAssignment

	mu            sync.Mutex
	existingPages []string
	sequence      int64
	transactions  []oracle.Transaction
	cancel        context.CancelFunc
	done          chan struct{}
}

func NewJournalImportActor(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec, recorder EventRecorder) (*JournalImportActor, error) {
	profile, err := JournalImportProfileFromBehavior(group.Profile.Behavior)
	if err != nil {
		return nil, err
	}
	return &JournalImportActor{
		GroupName: group.Name,
		Index:     index,
		ID:        fmt.Sprintf("%s-%d", group.Name, index),
		Seed:      seed,
		Rate:      rate,
		Profile:   profile,
		Recorder:  recorder,
	}, nil
}

func (a *JournalImportActor) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.done != nil {
		a.mu.Unlock()
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.mu.Unlock()

	a.initializeExistingPages()
	if err := a.bootstrap(child); err != nil {
		cancel()
		close(a.done)
		return err
	}
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-started", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"behavior": "import-journal", "importAPI": "ApplyGraphOperations"}})
	}
	go a.loop(child)
	return nil
}

func (a *JournalImportActor) UpdateRate(ctx context.Context, rate spec.RateSpec) error {
	a.mu.Lock()
	a.Rate = rate
	a.mu.Unlock()
	if a.Recorder != nil {
		return a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-rate-updated", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"commitsPerSecond": rate.CommitsPerSecond, "queriesPerSecond": rate.QueriesPerSecond}})
	}
	return nil
}

func (a *JournalImportActor) Stop(ctx context.Context) error {
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

func (a *JournalImportActor) Transactions() []oracle.Transaction {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]oracle.Transaction(nil), a.transactions...)
}

func (a *JournalImportActor) initializeExistingPages() {
	count := a.Profile.Transaction.ExistingPageCount
	pages := make([]string, 0, count)
	for index := 0; index < count; index++ {
		pages = append(pages, stableUUID(fmt.Sprintf("%s-existing-page-%d", a.ID, index)))
	}
	a.mu.Lock()
	a.existingPages = pages
	a.mu.Unlock()
}

func (a *JournalImportActor) loop(ctx context.Context) {
	defer close(a.done)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	lastCommit := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			interval := a.commitInterval()
			if interval > 0 && now.Sub(lastCommit) >= interval {
				opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				a.commit(opCtx)
				cancel()
				lastCommit = now
			}
		}
	}
}

func (a *JournalImportActor) commitInterval() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Rate.CommitsPerSecond <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / a.Rate.CommitsPerSecond)
}

func (a *JournalImportActor) bootstrap(ctx context.Context) error {
	if len(a.existingPages) == 0 {
		return nil
	}
	nodes := make([]*clientv1.GraphOperation, 0, len(a.existingPages))
	transaction := oracle.Transaction{ActorID: a.ID, Sequence: 0, Acknowledged: true}
	for index, pageID := range a.existingPages {
		props := map[string]any{
			"title":                fmt.Sprintf("page-%04d", index),
			"normalized_title":     fmt.Sprintf("page-%04d", index),
			"source_type":          "logseq",
			"tags":                 []any{},
			"page_refs":            []any{},
			"properties":           map[string]any{},
			"ai_processing_policy": "excluded",
		}
		op, err := journalCreateNodeOperation(pageID, []string{"pkm.page"}, props, journalImportPayloadText(a.ID, 0, index, a.Profile.Transaction.PagePayloadBytes))
		if err != nil {
			return err
		}
		nodes = append(nodes, op)
		transaction.Operations = append(transaction.Operations, oracle.Operation{Type: oracle.OperationCreateNode, NodeID: pageID, Label: "pkm.page"})
	}
	if err := a.executeImport(ctx, nodes); err != nil {
		transaction.Acknowledged = false
		transaction.ErrorClass = ClassifyError(err)
		return fmt.Errorf("bootstrap existing pages: %w", err)
	}
	a.mu.Lock()
	a.transactions = append(a.transactions, transaction)
	a.mu.Unlock()
	a.recordAcknowledged(ctx, transaction, "bootstrap", len(nodes), 0, 0)
	return nil
}

func (a *JournalImportActor) commit(ctx context.Context) {
	operationCtx, release, err := reserveActorOperation(ctx, a.Recorder, a.ID, "graph-commit")
	if err != nil {
		return
	}
	defer release()
	a.mu.Lock()
	a.sequence++
	sequence := a.sequence
	a.mu.Unlock()
	transaction, operations, nodeCount, edgeCount, err := a.buildTransaction(sequence)
	if err != nil {
		a.recordFailure(ctx, transaction, err, nodeCount, edgeCount, 0)
		return
	}
	requestBytes := graphOperationProtoBytes(operations)
	start := time.Now()
	if err := a.executeImport(operationCtx, operations); err != nil {
		transaction.Acknowledged = false
		transaction.ErrorClass = ClassifyError(err)
		a.recordFailure(ctx, transaction, err, nodeCount, edgeCount, requestBytes)
		return
	}
	a.mu.Lock()
	a.transactions = append(a.transactions, transaction)
	a.mu.Unlock()
	a.recordAcknowledged(ctx, transaction, "workload", nodeCount, edgeCount, requestBytes, float64(time.Since(start).Microseconds())/1000)
}

func (a *JournalImportActor) buildTransaction(sequence int64) (oracle.Transaction, []*clientv1.GraphOperation, int, int, error) {
	cfg := a.Profile.Transaction
	transaction := oracle.Transaction{ActorID: a.ID, Sequence: sequence, Acknowledged: true}
	nodeOps := make([]*clientv1.GraphOperation, 0, cfg.JournalsPerTransaction*(cfg.EntriesPerJournal+1)+cfg.SameTransactionPageCount)
	samePageOps := make([]*clientv1.GraphOperation, 0, cfg.SameTransactionPageCount)
	edgeOps := make([]*clientv1.GraphOperation, 0, cfg.JournalsPerTransaction*cfg.EntriesPerJournal*(1+cfg.LinksPerEntry))
	pageIDs := make([]string, 0, cfg.SameTransactionPageCount)
	pageNames := make([]string, 0, cfg.SameTransactionPageCount)
	if cfg.PageTargetMode == journalImportTargetSameTransaction || cfg.PageTargetMode == journalImportTargetMixed {
		for index := 0; index < cfg.SameTransactionPageCount; index++ {
			pageID := stableUUID(fmt.Sprintf("%s-seq-%d-page-%d", a.ID, sequence, index))
			pageIDs = append(pageIDs, pageID)
			pageNames = append(pageNames, fmt.Sprintf("page-%d-%04d", sequence, index))
			props := map[string]any{"title": pageNames[index], "normalized_title": pageNames[index], "source_type": "logseq", "tags": []any{}, "page_refs": []any{}, "properties": map[string]any{}, "ai_processing_policy": "excluded"}
			if !cfg.IndexedProperties {
				delete(props, "normalized_title")
			}
			op, err := journalCreateNodeOperation(pageID, []string{"pkm.page"}, props, journalImportPayloadText(a.ID, sequence, index, cfg.PagePayloadBytes))
			if err != nil {
				return transaction, nil, 0, 0, err
			}
			samePageOps = append(samePageOps, op)
		}
	}
	for journalIndex := 0; journalIndex < cfg.JournalsPerTransaction; journalIndex++ {
		journalDate := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(sequence*int64(cfg.JournalsPerTransaction))+journalIndex)
		journalDay := float64(journalDate.Year()*10000 + int(journalDate.Month())*100 + journalDate.Day())
		journalID := stableUUID(fmt.Sprintf("%s-seq-%d-journal-%d", a.ID, sequence, journalIndex))
		journalProps := map[string]any{"source_type": "logseq", "tags": []any{}, "page_refs": []any{}, "properties": map[string]any{}, "ai_processing_policy": "excluded"}
		if cfg.IndexedProperties {
			journalProps["journal_date"] = journalDate.Format("2006-01-02")
			journalProps["journal_day"] = journalDay
		}
		journalOp, err := journalCreateNodeOperation(journalID, []string{"pkm.journal"}, journalProps, journalImportPayloadText(a.ID, sequence, journalIndex, cfg.JournalPayloadBytes))
		if err != nil {
			return transaction, nil, 0, 0, err
		}
		nodeOps = append(nodeOps, journalOp)
		for entryIndex := 0; entryIndex < cfg.EntriesPerJournal; entryIndex++ {
			entryID := stableUUID(fmt.Sprintf("%s-seq-%d-journal-%d-entry-%d", a.ID, sequence, journalIndex, entryIndex))
			refs := make([]any, 0, cfg.LinksPerEntry)
			refIDs := make([]string, 0, cfg.LinksPerEntry)
			for linkIndex := 0; linkIndex < cfg.LinksPerEntry; linkIndex++ {
				pageID, pageName := a.pageTarget(cfg.PageTargetMode, pageIDs, pageNames, sequence, journalIndex, entryIndex, linkIndex)
				if pageID == "" {
					return transaction, nil, 0, 0, fmt.Errorf("no page target available for journal entry")
				}
				refs = append(refs, pageName)
				refIDs = append(refIDs, pageID)
			}
			entryProps := map[string]any{"tags": []any{}, "page_refs": refs, "block_refs": []any{}, "properties": map[string]any{}, "source_type": "logseq", "logseq_id": fmt.Sprintf("journal-%d-entry-%d", sequence, entryIndex)}
			if cfg.IndexedProperties {
				entryProps["journal_date"] = journalDate.Format("2006-01-02")
				entryProps["journal_day"] = journalDay
			}
			entryOp, err := journalCreateNodeOperation(entryID, []string{"pkm.journal_entry"}, entryProps, journalImportPayloadText(a.ID, sequence, entryIndex, cfg.EntryPayloadBytes))
			if err != nil {
				return transaction, nil, 0, 0, err
			}
			nodeOps = append(nodeOps, entryOp)
			edge, err := journalCreateEdgeOperation(journalID, entryID, "contains", map[string]any{"order": float64(entryIndex)})
			if err != nil {
				return transaction, nil, 0, 0, err
			}
			edgeOps = append(edgeOps, edge)
			refOrder := make([]string, 0, len(refIDs))
			refNames := map[string]string{}
			refCounts := map[string]int{}
			for linkIndex, pageID := range refIDs {
				if _, seen := refCounts[pageID]; !seen {
					refOrder = append(refOrder, pageID)
				}
				refCounts[pageID]++
				refNames[pageID] = refs[linkIndex].(string)
			}
			for _, pageID := range refOrder {
				refName := refNames[pageID]
				edgeProps := map[string]any{"ref_type": "page", "source": "logseq", "raw": "[[" + refName + "]]", "target": refName, "count": float64(refCounts[pageID])}
				edge, err := journalCreateEdgeOperation(entryID, pageID, "references", edgeProps)
				if err != nil {
					return transaction, nil, 0, 0, err
				}
				edgeOps = append(edgeOps, edge)
			}
		}
	}
	nodeOps = append(nodeOps, samePageOps...)
	operations := append(nodeOps, edgeOps...)
	for _, operation := range operations {
		switch value := operation.GetOperation().(type) {
		case *clientv1.GraphOperation_CreateNode:
			transaction.Operations = append(transaction.Operations, oracle.Operation{Type: oracle.OperationCreateNode, NodeID: value.CreateNode.GetNodeId(), Label: firstLabel(value.CreateNode.GetLabels())})
		case *clientv1.GraphOperation_CreateEdge:
			transaction.Operations = append(transaction.Operations, oracle.Operation{Type: oracle.OperationCreateEdge, FromID: value.CreateEdge.GetFromNodeId(), ToID: value.CreateEdge.GetToNodeId(), EdgeType: firstLabel(value.CreateEdge.GetLabels())})
		}
	}
	return transaction, operations, len(nodeOps), len(edgeOps), nil
}

func (a *JournalImportActor) pageTarget(mode string, sameIDs, sameNames []string, sequence int64, journalIndex, entryIndex, linkIndex int) (string, string) {
	position := journalIndex*a.Profile.Transaction.EntriesPerJournal*a.Profile.Transaction.LinksPerEntry + entryIndex*a.Profile.Transaction.LinksPerEntry + linkIndex
	a.mu.Lock()
	existing := append([]string(nil), a.existingPages...)
	a.mu.Unlock()
	existingName := func(index int) string { return fmt.Sprintf("page-%04d", index) }
	same := func(index int) (string, string) { index %= len(sameIDs); return sameIDs[index], sameNames[index] }
	switch mode {
	case journalImportTargetExistingReused:
		return existing[0], existingName(0)
	case journalImportTargetExistingUnique:
		index := position % len(existing)
		return existing[index], existingName(index)
	case journalImportTargetSameTransaction:
		return same(position)
	case journalImportTargetMixed:
		if position%2 == 0 {
			index := (position / 2) % len(existing)
			return existing[index], existingName(index)
		}
		return same(position / 2)
	default:
		return "", ""
	}
}

func (a *JournalImportActor) executeImport(ctx context.Context, operations []*clientv1.GraphOperation) error {
	if !assignmentCanUseRealClient(a.currentAssignment()) {
		return nil
	}
	assignment := a.currentAssignment()
	client, err := mycel.Dial(ctx, clientConfigForAssignment(assignment, false))
	if err != nil {
		return err
	}
	defer client.Close()
	a.captureClientTokens(client)
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return err
	}
	defer func() { _ = client.CloseSession(context.Background(), sessionID) }()
	if err := readImporterSnapshot(ctx, client, sessionID); err != nil {
		return err
	}
	txID, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = client.CloseTransaction(context.Background(), txID)
		}
	}()
	chunkSize := a.Profile.Transaction.ApplyChunkOperations
	for len(operations) > 0 {
		count := 0
		encodedBytes := 0
		for count < len(operations) && count < chunkSize {
			size := proto.Size(operations[count])
			if count > 0 && encodedBytes+size > journalImportMaxChunkProtoSize {
				break
			}
			encodedBytes += size
			count++
		}
		if count == 0 {
			count = 1
		}
		if _, err := client.ApplyGraphOperations(ctx, txID, operations[:count]); err != nil {
			return err
		}
		operations = operations[count:]
	}
	if err := client.CommitTransaction(ctx, txID); err != nil {
		return err
	}
	committed = true
	return nil
}

func readImporterSnapshot(ctx context.Context, client *mycel.Client, sessionID string) error {
	txID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return err
	}
	defer func() { _ = client.CloseTransaction(context.Background(), txID) }()
	pageToken := ""
	for {
		response, err := client.ListNodes(ctx, txID, 100, pageToken)
		if err != nil {
			return err
		}
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	pageToken = ""
	for {
		callCtx, cancel := client.AuthCallContext(ctx)
		response, err := client.Graph.ListEdges(callCtx, &clientv1.ListEdgesRequest{TransactionId: txID, PageSize: 100, PageToken: pageToken})
		cancel()
		if err != nil {
			return err
		}
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil
}

func (a *JournalImportActor) currentAssignment() provision.ActorAssignment {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Assignment
}

func (a *JournalImportActor) captureClientTokens(client *mycel.Client) {
	a.mu.Lock()
	defer a.mu.Unlock()
	captureClientTokens(&a.Assignment, client)
}

func (a *JournalImportActor) recordAcknowledged(ctx context.Context, transaction oracle.Transaction, phase string, nodeCount, edgeCount int, requestBytes int, latency ...float64) {
	payload := a.transactionPayload(phase, nodeCount, edgeCount, requestBytes)
	if len(latency) > 0 {
		payload["transactionLatencyMs"] = latency[0]
	}
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-transaction-acknowledged", ActorID: a.ID, GroupName: a.GroupName, Sequence: transaction.Sequence, Transaction: transaction, Payload: payload})
	}
}

func (a *JournalImportActor) recordFailure(ctx context.Context, transaction oracle.Transaction, err error, nodeCount, edgeCount, requestBytes int) {
	if a.Recorder != nil {
		payload := a.transactionPayload("workload", nodeCount, edgeCount, requestBytes)
		payload["error"] = err.Error()
		payload["class"] = transaction.ErrorClass
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-transaction-failed", ActorID: a.ID, GroupName: a.GroupName, Sequence: transaction.Sequence, Transaction: transaction, Payload: payload})
	}
}

func (a *JournalImportActor) transactionPayload(phase string, nodeCount, edgeCount, requestBytes int) map[string]any {
	assignment := a.currentAssignment()
	return map[string]any{
		"importAPI":            "ApplyGraphOperations",
		"transactionBoundary":  "single-read-write-transaction",
		"atomic":               true,
		"phase":                phase,
		"nodeCount":            nodeCount,
		"edgeCount":            edgeCount,
		"operationCount":       nodeCount + edgeCount,
		"operationProtoBytes":  requestBytes,
		"applyChunkOperations": a.Profile.Transaction.ApplyChunkOperations,
		"pageTargetMode":       a.Profile.Transaction.PageTargetMode,
		"indexedProperties":    a.Profile.Transaction.IndexedProperties,
		"spaceId":              assignment.SpaceID,
		"domainId":             assignment.DomainID,
		"daemonAddr":           assignment.DaemonAddr,
		"payloadSizesRedacted": true,
	}
}

func journalCreateNodeOperation(id string, labels []string, properties map[string]any, content string) (*clientv1.GraphOperation, error) {
	props, err := structpb.NewStruct(properties)
	if err != nil {
		return nil, err
	}
	payload, err := structpb.NewStruct(map[string]any{"text": content})
	if err != nil {
		return nil, err
	}
	return &clientv1.GraphOperation{Operation: &clientv1.GraphOperation_CreateNode{CreateNode: &clientv1.NodeCreate{NodeId: stringPointer(id), Labels: labels, Properties: props, Payload: payload}}}, nil
}

func journalCreateEdgeOperation(from, to, label string, properties map[string]any) (*clientv1.GraphOperation, error) {
	props, err := structpb.NewStruct(properties)
	if err != nil {
		return nil, err
	}
	return &clientv1.GraphOperation{Operation: &clientv1.GraphOperation_CreateEdge{CreateEdge: &clientv1.EdgeCreate{FromNodeId: from, ToNodeId: to, Labels: []string{label}, Properties: props}}}, nil
}

func journalImportPayloadText(actorID string, sequence int64, index, targetBytes int) string {
	if targetBytes <= 0 {
		return ""
	}
	base := fmt.Sprintf("logseq journal entry %s %d %d", actorID, sequence, index)
	if targetBytes > len(base) {
		base += strings.Repeat("x", targetBytes-len(base))
	}
	return base
}

func graphOperationProtoBytes(operations []*clientv1.GraphOperation) int {
	total := 0
	for _, operation := range operations {
		total += proto.Size(operation)
	}
	return total
}

func firstLabel(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return labels[0]
}

func stringPointer(value string) *string { return &value }
