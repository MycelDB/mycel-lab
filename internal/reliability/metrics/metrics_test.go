package metrics

import (
	"testing"
	"time"
)

func TestAggregateComputesRatesAndPercentiles(t *testing.T) {
	summary := Aggregate([]Sample{
		{Name: "commits", Value: 120},
		{Name: "transaction_latency_ms", Value: 10},
		{Name: "transaction_latency_ms", Value: 20},
		{Name: "transaction_latency_ms", Value: 30},
		{Name: "read_check_latency_ms", Value: 5},
		{Name: "read_check_latency_ms", Value: 15},
		{Name: "errors", Value: 2, Labels: map[string]string{"class": "transient"}},
		{Name: "errors", Value: 1, Labels: map[string]string{"class": "permanent"}},
		{Name: "expected_degradation", Value: 3},
		{Name: "pod_restarts", Value: 4},
		{Name: "leader_changes", Value: 1},
	}, time.Minute)
	if summary.CommitsPerSecond != 2 {
		t.Fatalf("commits/sec=%v, want 2", summary.CommitsPerSecond)
	}
	if summary.TransactionLatencyP50MS != 20 || summary.TransactionLatencyP95MS != 30 || summary.TransactionLatencyP99MS != 30 {
		t.Fatalf("transaction percentiles unexpected: %+v", summary)
	}
	if summary.ReadCheckLatencyP50MS != 5 || summary.ReadCheckLatencyP95MS != 15 {
		t.Fatalf("read percentiles unexpected: %+v", summary)
	}
	if summary.TransientErrors != 2 || summary.PermanentErrors != 1 || summary.ExpectedDegradationEvents != 3 || summary.PodRestartEvents != 4 || summary.LeaderChanges != 1 {
		t.Fatalf("counters unexpected: %+v", summary)
	}
}

func TestCompareComputesDeltas(t *testing.T) {
	cmp := Compare(Summary{RunID: "a", CommitsPerSecond: 1, TransientErrors: 2}, Summary{RunID: "b", CommitsPerSecond: 3.5, TransientErrors: 5})
	if cmp.Delta.CommitsPerSecond != 2.5 || cmp.Delta.TransientErrors != 3 {
		t.Fatalf("delta unexpected: %+v", cmp.Delta)
	}
}
