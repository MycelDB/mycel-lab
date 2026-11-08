package actors

import (
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestJournalImportProfileUsesAtomicDefaults(t *testing.T) {
	profile, err := JournalImportProfileFromBehavior(map[string]any{"type": "import-journal"})
	if err != nil {
		t.Fatalf("JournalImportProfileFromBehavior() error=%v", err)
	}
	if !profile.Transaction.Atomic || profile.Transaction.PageTargetMode != "existing-reused" {
		t.Fatalf("profile=%+v, want atomic existing-reused defaults", profile.Transaction)
	}
}

func TestJournalImportProfileRequiresAtomicTransactions(t *testing.T) {
	_, err := JournalImportProfileFromBehavior(map[string]any{
		"transaction": map[string]any{
			"atomic": false,
		},
	})
	if err == nil {
		t.Fatal("JournalImportProfileFromBehavior() error=nil, want non-atomic profile rejection")
	}
}

func TestJournalImportBuildsCommonfolioJournalShape(t *testing.T) {
	group := spec.ResolvedActorGroup{ActorGroupSpec: spec.ActorGroupSpec{Name: "importer"}, Profile: spec.ActorProfile{Behavior: map[string]any{
		"type": "import-journal",
		"transaction": map[string]any{
			"journalsPerTransaction":   1,
			"entriesPerJournal":        2,
			"linksPerEntry":            2,
			"existingPageCount":        2,
			"sameTransactionPageCount": 2,
			"pageTargetMode":           "existing-reused",
			"entryPayloadBytes":        32,
			"indexedProperties":        true,
			"applyChunkOperations":     2,
			"atomic":                   true,
		},
	}}}
	actor, err := NewJournalImportActor(group, 0, 313, spec.RateSpec{}, nil)
	if err != nil {
		t.Fatalf("NewJournalImportActor() error=%v", err)
	}
	actor.initializeExistingPages()
	transaction, operations, nodes, edges, err := actor.buildTransaction(1)
	if err != nil {
		t.Fatalf("buildTransaction() error=%v", err)
	}
	if nodes != 3 || edges != 4 {
		t.Fatalf("nodes=%d edges=%d, want 3 nodes and 4 deduplicated edges", nodes, edges)
	}
	if len(operations) != nodes+edges || len(transaction.Operations) != nodes+edges {
		t.Fatalf("operations=%d transactionOperations=%d, want %d", len(operations), len(transaction.Operations), nodes+edges)
	}
	if got := countImportOperationType(transaction, oracle.OperationCreateNode); got != 3 {
		t.Fatalf("create nodes=%d, want 3", got)
	}
	if got := countImportOperationType(transaction, oracle.OperationCreateEdge); got != 4 {
		t.Fatalf("create edges=%d, want 4", got)
	}
	for _, operation := range operations {
		if operation.GetCreateNode() != nil && operation.GetCreateNode().GetPayload() == nil {
			t.Fatal("import node payload is missing")
		}
	}
}

func TestJournalImportSameTransactionPagesAreIncludedBeforeEdges(t *testing.T) {
	group := spec.ResolvedActorGroup{ActorGroupSpec: spec.ActorGroupSpec{Name: "importer"}, Profile: spec.ActorProfile{Behavior: map[string]any{
		"type": "import-journal",
		"transaction": map[string]any{
			"entriesPerJournal":        1,
			"linksPerEntry":            1,
			"sameTransactionPageCount": 1,
			"pageTargetMode":           "same-transaction",
			"atomic":                   true,
		},
	}}}
	actor, err := NewJournalImportActor(group, 0, 1, spec.RateSpec{}, nil)
	if err != nil {
		t.Fatalf("NewJournalImportActor() error=%v", err)
	}
	transaction, operations, _, _, err := actor.buildTransaction(1)
	if err != nil {
		t.Fatalf("buildTransaction() error=%v", err)
	}
	seenEdge := false
	for _, operation := range operations {
		if operation.GetCreateEdge() != nil {
			seenEdge = true
		}
		if seenEdge && operation.GetCreateNode() != nil {
			t.Fatal("node operation appeared after edge operation")
		}
	}
	if len(transaction.Operations) == 0 {
		t.Fatal("transaction has no operations")
	}
}

func countImportOperationType(transaction oracle.Transaction, typ oracle.OperationType) int {
	count := 0
	for _, operation := range transaction.Operations {
		if operation.Type == typ {
			count++
		}
	}
	return count
}
