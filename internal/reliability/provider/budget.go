package provider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrBudgetExhausted = errors.New("provider orchestration budget exhausted")
	ErrBudgetClosed    = errors.New("provider orchestration budget closed")
)

type Demand struct {
	Operations       int64
	ProviderRequests int64
	Retries          int64
	InputTokens      int64
	OutputTokens     int64
	EstimatedCostUSD float64
}

type BudgetSnapshot struct {
	StartedAt                  time.Time `json:"startedAt"`
	Deadline                   time.Time `json:"deadline"`
	Closed                     bool      `json:"closed"`
	StopReason                 string    `json:"stopReason,omitempty"`
	Operations                 int64     `json:"operations"`
	ProviderRequests           int64     `json:"providerRequests"`
	Retries                    int64     `json:"retries"`
	InputTokens                int64     `json:"inputTokens"`
	OutputTokens               int64     `json:"outputTokens"`
	EstimatedCostUSD           float64   `json:"estimatedCostUSD"`
	ConcurrentOperations       int64     `json:"concurrentOperations"`
	ConcurrentProviderRequests int64     `json:"concurrentProviderRequests"`
}

type Ledger struct {
	mu      sync.Mutex
	budget  Budget
	started time.Time
	clock   func() time.Time
	state   BudgetSnapshot
}

type Reservation struct {
	ledger           *Ledger
	operations       int64
	providerRequests int64
	once             sync.Once
}

func NewLedger(budget Budget) (*Ledger, error) {
	if err := budget.Validate(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	ledger := &Ledger{budget: budget, started: now, clock: func() time.Time { return time.Now().UTC() }}
	ledger.state.StartedAt = now
	ledger.state.Deadline = now.Add(budget.TotalTimeout.Duration)
	return ledger, nil
}

func (l *Ledger) Reserve(ctx context.Context, demand Demand) (*Reservation, error) {
	if err := validateDemand(demand); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state.Closed {
		return nil, fmt.Errorf("%w: %s", ErrBudgetClosed, l.state.StopReason)
	}
	if !l.clock().Before(l.state.Deadline) {
		l.closeLocked("total timeout reached")
		return nil, fmt.Errorf("%w: total timeout reached", ErrBudgetExhausted)
	}
	if demand.Retries > l.budget.MaxRetriesPerRequest {
		return nil, l.exhaustLocked("maxRetriesPerRequest")
	}
	checks := []struct {
		name    string
		current int64
		add     int64
		max     int64
	}{
		{name: "maxOperations", current: l.state.Operations, add: demand.Operations, max: l.budget.MaxOperations},
		{name: "maxRequests", current: l.state.ProviderRequests, add: demand.ProviderRequests, max: l.budget.MaxRequests},
		{name: "maxInputTokens", current: l.state.InputTokens, add: demand.InputTokens, max: l.budget.MaxInputTokens},
		{name: "maxOutputTokens", current: l.state.OutputTokens, add: demand.OutputTokens, max: l.budget.MaxOutputTokens},
		{name: "maxConcurrentOperations", current: l.state.ConcurrentOperations, add: demand.Operations, max: l.budget.MaxConcurrentOperations},
		{name: "maxConcurrentRequests", current: l.state.ConcurrentProviderRequests, add: demand.ProviderRequests, max: l.budget.MaxConcurrentRequests},
	}
	for _, check := range checks {
		if check.add > 0 && (check.max == 0 || exceedsInt64(check.current, check.add, check.max)) {
			return nil, l.exhaustLocked(check.name)
		}
	}
	if demand.EstimatedCostUSD > 0 && (l.budget.MaxEstimatedCostUSD == 0 || l.state.EstimatedCostUSD+demand.EstimatedCostUSD > l.budget.MaxEstimatedCostUSD) {
		return nil, l.exhaustLocked("maxEstimatedCostUSD")
	}
	l.state.Operations += demand.Operations
	l.state.ProviderRequests += demand.ProviderRequests
	l.state.Retries += demand.Retries
	l.state.InputTokens += demand.InputTokens
	l.state.OutputTokens += demand.OutputTokens
	l.state.EstimatedCostUSD += demand.EstimatedCostUSD
	l.state.ConcurrentOperations += demand.Operations
	l.state.ConcurrentProviderRequests += demand.ProviderRequests
	return &Reservation{ledger: l, operations: demand.Operations, providerRequests: demand.ProviderRequests}, nil
}

func (r *Reservation) Release() {
	if r == nil || r.ledger == nil {
		return
	}
	r.once.Do(func() {
		r.ledger.mu.Lock()
		defer r.ledger.mu.Unlock()
		r.ledger.state.ConcurrentOperations -= r.operations
		r.ledger.state.ConcurrentProviderRequests -= r.providerRequests
	})
}

func (l *Ledger) RequestContext(parent context.Context) (context.Context, context.CancelFunc) {
	l.mu.Lock()
	deadline := l.state.Deadline
	requestTimeout := l.budget.RequestTimeout.Duration
	l.mu.Unlock()
	requestDeadline := l.clock().Add(requestTimeout)
	if deadline.Before(requestDeadline) {
		requestDeadline = deadline
	}
	return context.WithDeadline(parent, requestDeadline)
}

func (l *Ledger) Close(reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeLocked(reason)
}

func (l *Ledger) Snapshot() BudgetSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

func (l *Ledger) closeLocked(reason string) {
	l.state.Closed = true
	if l.state.StopReason == "" {
		l.state.StopReason = reason
	}
}

func (l *Ledger) exhaustLocked(limit string) error {
	reason := "limit " + limit + " reached"
	l.closeLocked(reason)
	return fmt.Errorf("%w: %s", ErrBudgetExhausted, reason)
}

func validateDemand(demand Demand) error {
	if demand.Operations <= 0 {
		return errors.New("budget demand operations must be positive")
	}
	integers := []struct {
		name  string
		value int64
	}{
		{name: "providerRequests", value: demand.ProviderRequests},
		{name: "retries", value: demand.Retries},
		{name: "inputTokens", value: demand.InputTokens},
		{name: "outputTokens", value: demand.OutputTokens},
	}
	for _, item := range integers {
		if item.value < 0 {
			return fmt.Errorf("budget demand %s must not be negative", item.name)
		}
	}
	if demand.EstimatedCostUSD < 0 || math.IsNaN(demand.EstimatedCostUSD) || math.IsInf(demand.EstimatedCostUSD, 0) {
		return errors.New("budget demand estimatedCostUSD must be finite and non-negative")
	}
	return nil
}

func exceedsInt64(current, add, maximum int64) bool {
	return add > maximum || current > maximum-add
}
