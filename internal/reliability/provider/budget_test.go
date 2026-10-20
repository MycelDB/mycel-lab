package provider

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLedgerConcurrentReservationsNeverExceedLimit(t *testing.T) {
	budget := testBudget()
	budget.MaxOperations = 1000
	budget.MaxConcurrentOperations = 4
	ledger, err := NewLedger(budget)
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int64
	var maximum atomic.Int64
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			reservation, err := ledger.Reserve(context.Background(), Demand{Operations: 1})
			if err != nil {
				return
			}
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			reservation.Release()
		}()
	}
	close(start)
	group.Wait()
	if got := maximum.Load(); got > budget.MaxConcurrentOperations {
		t.Fatalf("maximum concurrency = %d, limit %d", got, budget.MaxConcurrentOperations)
	}
	if snapshot := ledger.Snapshot(); snapshot.ConcurrentOperations != 0 {
		t.Fatalf("leaked reservation: %#v", snapshot)
	}
}

func TestLedgerExhaustionStopsNewWork(t *testing.T) {
	budget := testBudget()
	budget.MaxOperations = 2
	ledger, err := NewLedger(budget)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		reservation, err := ledger.Reserve(context.Background(), Demand{Operations: 1})
		if err != nil {
			t.Fatalf("Reserve(%d) error = %v", i, err)
		}
		reservation.Release()
	}
	if _, err := ledger.Reserve(context.Background(), Demand{Operations: 1}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("exhausting Reserve() error = %v", err)
	}
	if _, err := ledger.Reserve(context.Background(), Demand{Operations: 1}); !errors.Is(err, ErrBudgetClosed) {
		t.Fatalf("post-exhaustion Reserve() error = %v", err)
	}
}

func TestLedgerAccountsProviderDimensionsAtomically(t *testing.T) {
	budget := testBudget()
	budget.MaxRequests = 2
	budget.MaxConcurrentRequests = 2
	budget.MaxRetriesPerRequest = 1
	budget.MaxInputTokens = 10
	budget.MaxOutputTokens = 5
	budget.MaxEstimatedCostUSD = 0.5
	ledger, err := NewLedger(budget)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := ledger.Reserve(context.Background(), Demand{Operations: 1, ProviderRequests: 2, Retries: 1, InputTokens: 10, OutputTokens: 5, EstimatedCostUSD: 0.5})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	reservation.Release()
	snapshot := ledger.Snapshot()
	if snapshot.ProviderRequests != 2 || snapshot.Retries != 1 || snapshot.InputTokens != 10 || snapshot.OutputTokens != 5 || snapshot.EstimatedCostUSD != 0.5 {
		t.Fatalf("unexpected accounting: %#v", snapshot)
	}
}

func TestLedgerHonorsCancellationBeforeReservation(t *testing.T) {
	ledger, err := NewLedger(testBudget())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ledger.Reserve(ctx, Demand{Operations: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reserve() error = %v", err)
	}
	if snapshot := ledger.Snapshot(); snapshot.Operations != 0 {
		t.Fatalf("canceled reservation consumed budget: %#v", snapshot)
	}
}

func TestLedgerRequestContextUsesBoundedDeadline(t *testing.T) {
	ledger, err := NewLedger(testBudget())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := ledger.RequestContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("request context has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 2*time.Second {
		t.Fatalf("unexpected deadline remaining %s", remaining)
	}
}

func testBudget() Budget {
	return Budget{
		Enforcement:             EnforcementRequired,
		MaxOperations:           100,
		MaxConcurrentOperations: 8,
		RequestTimeout:          Duration{Duration: time.Second},
		TotalTimeout:            Duration{Duration: 10 * time.Second},
	}
}
