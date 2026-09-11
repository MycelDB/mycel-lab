package oracle

import (
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestExpectedCountsDeriveFromAcknowledgedOperationLog(t *testing.T) {
	counts := ExpectedCounts([]Transaction{
		{Acknowledged: true, Operations: []Operation{{Type: OperationCreateNode}, {Type: OperationCreateEdge}}},
		{Acknowledged: false, Operations: []Operation{{Type: OperationCreateNode}, {Type: OperationCreateEdge}}},
		{Acknowledged: true, Operations: []Operation{{Type: OperationUpdateNode}, {Type: OperationCreateNode}}},
	})
	if counts.Nodes != 2 || counts.Edges != 1 {
		t.Fatalf("counts=%+v, want nodes=2 edges=1", counts)
	}
}

func TestReadCheckFailureClassification(t *testing.T) {
	phase := spec.PhaseSpec{Name: "outage", Outcome: map[string]string{"availability": "expected-degradation"}}
	transient := ClassifyReadCheck("actor-1", phase, false, "unavailable")
	if !transient.ExpectedDegradation {
		t.Fatalf("transient outage read check not marked expected degradation: %+v", transient)
	}
	correctness := ClassifyReadCheck("actor-1", phase, false, "correctness")
	if correctness.ExpectedDegradation {
		t.Fatalf("correctness failure was masked: %+v", correctness)
	}
	normal := ClassifyReadCheck("actor-1", spec.PhaseSpec{Name: "normal"}, false, "unavailable")
	if normal.ExpectedDegradation {
		t.Fatalf("normal phase unavailable failure was masked: %+v", normal)
	}
}

func TestConvergenceFailuresIncludeDetails(t *testing.T) {
	failures := CheckConvergence(map[string]Counts{"actor-1": {Nodes: 3, Edges: 2}}, []Observation{{ActorID: "actor-1", Endpoint: "pod-0", Counts: Counts{Nodes: 2, Edges: 2}}}, "recovery")
	if len(failures) != 1 {
		t.Fatalf("failures=%d, want 1", len(failures))
	}
	failure := failures[0]
	if failure.ExpectedNodes != 3 || failure.ObservedNodes != 2 || failure.Endpoint != "pod-0" || failure.Phase != "recovery" {
		t.Fatalf("failure details incomplete: %+v", failure)
	}
}

func TestAssertNoAcknowledgedLoss(t *testing.T) {
	if err := AssertNoAcknowledgedLoss(Counts{Nodes: 1}, Counts{Nodes: 1}); err != nil {
		t.Fatalf("AssertNoAcknowledgedLoss(equal) error=%v", err)
	}
	if err := AssertNoAcknowledgedLoss(Counts{Nodes: 1}, Counts{}); err == nil {
		t.Fatal("AssertNoAcknowledgedLoss(mismatch) error=nil, want error")
	}
}
