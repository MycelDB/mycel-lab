package events

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGraphUsageCheckSeparatesPendingBackfillAndExactReads(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		calls []string
	}{
		{spec.GraphUsageLegacy, []string{"space count", "domain count", "space reconcile", "space logical", "domain logical"}},
		{spec.GraphUsagePending, []string{"space count", "domain count", "space logical", "space strict", "domain logical", "domain strict"}},
		{spec.GraphUsageBackfill, []string{"space count", "domain count", "space reconcile", "space strict", "domain strict"}},
		{spec.GraphUsageExact, []string{"space count", "domain count", "space strict", "space physical", "domain strict", "domain physical", "space reconcile"}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			client := &graphUsageTestClient{t: t, mode: tc.mode}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := checkGraphUsageNode(ctx, client, "myceld-1", graphUsageTestScopes(), spec.GraphUsageTarget{Mode: tc.mode, PayloadBytes: []uint64{1, 14, 3}, LogicalBytes: 18, Diagnostic: tc.mode == spec.GraphUsageExact})
			if err != nil || !reflect.DeepEqual(client.calls, tc.calls) {
				t.Fatalf("mode %s: check error=%v calls=%v, want %v", tc.mode, err, client.calls, tc.calls)
			}
			for _, evidence := range result.Scopes {
				if evidence.NodeCount == nil || evidence.Logical == nil || (tc.mode == spec.GraphUsagePending && evidence.StrictCode != codes.FailedPrecondition.String()) || (tc.mode == spec.GraphUsageExact && evidence.Diagnostic == nil) {
					t.Fatalf("missing per-scope evidence: %+v", evidence)
				}
			}
		})
	}
}

func TestGraphUsageCheckRejectsFalseZeroAndWrongStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*graphUsageTestClient)
		want string
	}{
		{"reported zero", func(c *graphUsageTestClient) { c.showZero = true }, "must be withheld"},
		{"unregistered metric on upgraded daemon", func(c *graphUsageTestClient) { c.notRegistered = true }, "missing USAGE_STATUS_REASON_COUNTERS_REBUILDING"},
		{"strict request unexpectedly passes", func(c *graphUsageTestClient) { c.strictCode = codes.OK; c.skipDefaultCode = true }, "strict graph byte read returned OK"},
		{"strict request has unrelated error", func(c *graphUsageTestClient) { c.strictCode = codes.Unavailable }, "strict graph byte read returned Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &graphUsageTestClient{t: t, mode: spec.GraphUsagePending}
			tc.edit(client)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := checkGraphUsageNode(ctx, client, "myceld-0", graphUsageTestScopes(), spec.GraphUsageTarget{Mode: spec.GraphUsagePending, PayloadBytes: []uint64{1, 14, 3}, LogicalBytes: 18})
			if err == nil || !strings.Contains(err.Error(), tc.want) || result.Scopes[0].Logical == nil || result.Health != nil {
				t.Fatalf("check error=%v, want %q with snapshot and no reconciliation", err, tc.want)
			}
		})
	}
}

func TestGraphUsageCheckRejectsIncorrectQuotaSafeBytesAndDrift(t *testing.T) {
	snapshot := &adminv1.UsageSnapshot{QuotaSafeComplete: true, AccountingState: adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY, Metrics: []*adminv1.UsageMetric{graphUsageTestMetric(graphLogicalBytesMetric, 17)}}
	if err := expectGraphUsageMetric(snapshot, graphLogicalBytesMetric, 18, adminv1.UsageUnit_USAGE_UNIT_BYTES, true); err == nil || !strings.Contains(err.Error(), "was 17, want 18") {
		t.Fatalf("byte assertion error=%v, want exact-value rejection", err)
	}
	snapshot.Metrics[0].Value = 18
	snapshot.Metrics[0].HardLimitEligible = true
	if err := expectGraphUsageMetric(snapshot, graphLogicalBytesMetric, 18, adminv1.UsageUnit_USAGE_UNIT_BYTES, true); err == nil {
		t.Fatal("accepted a hard-limit-eligible graph byte metric")
	}
	client := &graphUsageTestClient{t: t, mode: spec.GraphUsageExact, drift: 1}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := checkGraphUsageNode(ctx, client, "myceld-0", graphUsageTestScopes(), spec.GraphUsageTarget{Mode: spec.GraphUsageExact, PayloadBytes: []uint64{1, 14, 3}, LogicalBytes: 18})
	if err == nil || !strings.Contains(err.Error(), "driftScopes=1") || result.Health.GetDriftScopeCount() != 1 {
		t.Fatalf("check error=%v, health=%v, want reconciliation drift failure with evidence", err, result.Health)
	}
}

func graphUsageTestScopes() [2]*adminv1.UsageScope {
	return [2]*adminv1.UsageScope{
		{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space"}}},
		{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: "space", DomainId: "domain"}}},
	}
}

func graphUsageTestMetric(name string, value uint64) *adminv1.UsageMetric {
	metric := &adminv1.UsageMetric{Name: name, MetricVersion: 1, Value: value, Unit: adminv1.UsageUnit_USAGE_UNIT_BYTES, Authority: adminv1.UsageAuthority_USAGE_AUTHORITY_RAFT_COMMITTED_EXACT, QuotaSafe: true}
	switch name {
	case graphNodeCountMetric:
		metric.Unit = adminv1.UsageUnit_USAGE_UNIT_COUNT
		metric.HardLimitEligible = true
	case graphPhysicalBytesMetric:
		metric.Authority = adminv1.UsageAuthority_USAGE_AUTHORITY_OBSERVED_APPROXIMATE
		metric.QuotaSafe = false
	}
	return metric
}

type graphUsageTestClient struct {
	t               *testing.T
	mode            string
	calls           []string
	drift           uint64
	strictCode      codes.Code
	skipDefaultCode bool
	showZero        bool
	notRegistered   bool
}

func (c *graphUsageTestClient) GetUsage(_ context.Context, req *adminv1.GetUsageRequest) (*adminv1.GetUsageResponse, error) {
	c.t.Helper()
	scope := usageScopeName(req.GetScope())
	snapshot := &adminv1.UsageSnapshot{Scope: req.GetScope(), QuotaSafeComplete: true, AccountingState: adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY}
	if len(req.GetMetricNames()) != 1 {
		c.t.Fatalf("invalid request metrics: %v", req.GetMetricNames())
	}
	switch req.GetMetricNames()[0] {
	case graphNodeCountMetric:
		c.calls = append(c.calls, scope+" count")
		snapshot.Metrics = []*adminv1.UsageMetric{graphUsageTestMetric(graphNodeCountMetric, 3)}
	case graphPhysicalBytesMetric:
		c.calls = append(c.calls, scope+" physical")
		if !req.GetIncludeDiagnosticPhysical() || req.GetRequireQuotaSafe() {
			c.t.Fatalf("physical metric was not requested separately: %v", req)
		}
		snapshot.Metrics = []*adminv1.UsageMetric{graphUsageTestMetric(graphPhysicalBytesMetric, 900)}
	case graphLogicalBytesMetric:
		if req.GetIncludeDiagnosticPhysical() {
			c.t.Fatal("logical byte check included diagnostic physical data")
		}
		if req.GetRequireQuotaSafe() {
			c.calls = append(c.calls, scope+" strict")
		} else {
			c.calls = append(c.calls, scope+" logical")
		}
		if c.mode != spec.GraphUsageLegacy && c.mode != spec.GraphUsagePending {
			snapshot.Metrics = []*adminv1.UsageMetric{graphUsageTestMetric(graphLogicalBytesMetric, 18)}
			break
		}
		if req.GetRequireQuotaSafe() {
			code := c.strictCode
			if !c.skipDefaultCode && code == codes.OK {
				code = codes.FailedPrecondition
			}
			return nil, status.Error(code, "not quota-safe")
		}
		snapshot.QuotaSafeComplete = false
		reason := adminv1.UsageStatusReason_USAGE_STATUS_REASON_COUNTERS_REBUILDING
		if c.mode == spec.GraphUsageLegacy || c.notRegistered {
			reason = adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_UNAVAILABLE
		}
		snapshot.Blockers = []*adminv1.UsageStatus{{Reason: reason, MetricName: graphLogicalBytesMetric, Severity: adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_BLOCKER}}
		if c.showZero {
			snapshot.Metrics = []*adminv1.UsageMetric{graphUsageTestMetric(graphLogicalBytesMetric, 0)}
		}
	default:
		c.t.Fatalf("unexpected graph usage request: %v", req)
	}
	return &adminv1.GetUsageResponse{Snapshot: snapshot}, nil
}

func (c *graphUsageTestClient) GetUsageAccountingHealth(_ context.Context, req *adminv1.GetUsageAccountingHealthRequest) (*adminv1.GetUsageAccountingHealthResponse, error) {
	c.t.Helper()
	if !req.GetIncludeReconciliation() || req.GetIncludeDiagnosticCollectors() || req.GetScope().GetSpace() == nil {
		c.t.Fatalf("invalid reconciliation request: %v", req)
	}
	c.calls = append(c.calls, "space reconcile")
	return &adminv1.GetUsageAccountingHealthResponse{Health: &adminv1.UsageAccountingHealth{Scope: req.GetScope(), State: adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY, QuotaSafeReady: true, DriftScopeCount: c.drift}}, nil
}
