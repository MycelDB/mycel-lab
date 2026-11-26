package events

import (
	"context"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	graphNodeCountMetric     = "graph.node_count"
	graphLogicalBytesMetric  = "graph.logical_bytes"
	graphPhysicalBytesMetric = "diagnostic.local_graph_store_bytes"
)

type graphUsageEvidence struct {
	Expected spec.GraphUsageTarget    `json:"expected"`
	Nodes    []graphUsageNodeEvidence `json:"nodes"`
}

type graphUsageNodeEvidence struct {
	Node   string                         `json:"node"`
	Scopes [2]graphUsageScopeEvidence     `json:"scopes"`
	Health *adminv1.UsageAccountingHealth `json:"reconciliation,omitempty"`
}

type graphUsageScopeEvidence struct {
	Scope      *adminv1.UsageScope    `json:"scope"`
	NodeCount  *adminv1.UsageSnapshot `json:"nodeCountSnapshot,omitempty"`
	Logical    *adminv1.UsageSnapshot `json:"logicalSnapshot,omitempty"`
	Diagnostic *adminv1.UsageSnapshot `json:"diagnosticSnapshot,omitempty"`
	StrictCode string                 `json:"strictCode,omitempty"`
}

type usageCheckClient interface {
	GetUsage(context.Context, *adminv1.GetUsageRequest) (*adminv1.GetUsageResponse, error)
	GetUsageAccountingHealth(context.Context, *adminv1.GetUsageAccountingHealthRequest) (*adminv1.GetUsageAccountingHealthResponse, error)
}

func executeGraphUsageCheck(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, resources *provision.ScenarioResources, target map[string]any) (graphUsageEvidence, error) {
	var evidence graphUsageEvidence
	expected, err := spec.ParseGraphUsageTarget(target)
	if err != nil {
		return evidence, err
	}
	evidence.Expected = expected
	if resources == nil || scenario.Cluster.Nodes < 1 {
		return evidence, fmt.Errorf("graph usage check requires a provisioned cluster")
	}
	var fixture provision.ActorAssignment
	matched := 0
	for _, actor := range resources.Assignments {
		if actor.GroupName == expected.ActorGroup {
			matched++
			fixture = actor
		}
	}
	if matched != 1 || fixture.BehaviorType != "semantic-control" || fixture.SpaceID == "" || fixture.DomainID == "" {
		return evidence, fmt.Errorf("graph usage check requires one provisioned, event-only graph actor in %q", expected.ActorGroup)
	}
	endpoints, err := driver.Endpoints(ctx, environment)
	if err != nil {
		return evidence, err
	}
	if len(endpoints) != scenario.Cluster.Nodes {
		return evidence, fmt.Errorf("graph usage check found %d endpoints, want %d", len(endpoints), scenario.Cluster.Nodes)
	}
	seenAddresses, seenNodes := map[string]bool{}, map[string]bool{}
	for _, endpoint := range endpoints {
		if endpoint.NodeName == "" || endpoint.DaemonAddr == "" || seenNodes[endpoint.NodeName] || seenAddresses[endpoint.DaemonAddr] {
			return evidence, fmt.Errorf("graph usage check requires distinct named per-node endpoints")
		}
		seenAddresses[endpoint.DaemonAddr], seenNodes[endpoint.NodeName] = true, true
	}
	scopes := [2]*adminv1.UsageScope{
		{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: fixture.SpaceID}}},
		{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: fixture.SpaceID, DomainId: fixture.DomainID}}},
	}
	for _, endpoint := range endpoints {
		node, err := func() (graphUsageNodeEvidence, error) {
			ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: endpoint.DaemonAddr, Username: firstNonEmpty(resources.AdminUsername, provision.DefaultAdminUsername), Password: firstNonEmpty(resources.AdminPassword, provision.DefaultAdminPassword), ClientName: "mycel-lab-graph-usage-check"})
			if err != nil {
				return graphUsageNodeEvidence{Node: endpoint.NodeName}, err
			}
			defer admin.Close()
			return checkGraphUsageNode(ctx, admin, endpoint.NodeName, scopes, expected)
		}()
		evidence.Nodes = append(evidence.Nodes, node)
		if err != nil {
			return evidence, fmt.Errorf("graph usage %s on %s: %w", expected.Mode, endpoint.NodeName, err)
		}
	}
	return evidence, nil
}

func checkGraphUsageNode(ctx context.Context, admin usageCheckClient, node string, scopes [2]*adminv1.UsageScope, expected spec.GraphUsageTarget) (graphUsageNodeEvidence, error) {
	evidence := graphUsageNodeEvidence{Node: node}
	for i, scope := range scopes {
		evidence.Scopes[i].Scope = scope
		// Wait for follower apply using the legacy counter, irrespective of whether
		// graph bytes are served. No reconciliation may precede the pending check.
		req := &adminv1.GetUsageRequest{Scope: scope, MetricNames: []string{graphNodeCountMetric}}
		snapshot, err := awaitGraphUsageSnapshot(ctx, admin, req, func(snapshot *adminv1.UsageSnapshot) error {
			return expectGraphUsageMetric(snapshot, graphNodeCountMetric, uint64(len(expected.PayloadBytes)), adminv1.UsageUnit_USAGE_UNIT_COUNT, expected.Mode != spec.GraphUsageLegacy)
		})
		evidence.Scopes[i].NodeCount = snapshot
		if err != nil {
			return evidence, fmt.Errorf("wait for node count in %s: %w", usageScopeName(scope), err)
		}
	}
	if expected.Mode == spec.GraphUsageLegacy || expected.Mode == spec.GraphUsageBackfill {
		var err error
		evidence.Health, err = reconcileGraphUsage(ctx, admin, scopes[0])
		if err != nil {
			return evidence, err
		}
	}
	for i, scope := range scopes {
		if err := checkGraphLogicalSnapshot(ctx, admin, scope, expected, &evidence.Scopes[i]); err != nil {
			return evidence, fmt.Errorf("%s: %w", usageScopeName(scope), err)
		}
	}
	if expected.Mode == spec.GraphUsageExact {
		// Assert the persisted/read path first. A successful reconciliation must
		// not hide a missing counter after restart or unexpected byte drift.
		var err error
		evidence.Health, err = reconcileGraphUsage(ctx, admin, scopes[0])
		if err != nil {
			return evidence, err
		}
	}
	return evidence, nil
}

func checkGraphLogicalSnapshot(ctx context.Context, admin usageCheckClient, scope *adminv1.UsageScope, expected spec.GraphUsageTarget, evidence *graphUsageScopeEvidence) error {
	request := &adminv1.GetUsageRequest{Scope: scope, MetricNames: []string{graphLogicalBytesMetric}}
	if expected.Mode == spec.GraphUsageLegacy || expected.Mode == spec.GraphUsagePending {
		response, err := admin.GetUsage(ctx, request)
		evidence.Logical = response.GetSnapshot()
		if err != nil {
			return err
		}
		reason := adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_UNAVAILABLE
		if expected.Mode == spec.GraphUsagePending {
			reason = adminv1.UsageStatusReason_USAGE_STATUS_REASON_COUNTERS_REBUILDING
		}
		if err := expectUnavailableGraphBytes(evidence.Logical, scope, reason); err != nil {
			return err
		}
		if expected.Mode == spec.GraphUsagePending {
			request.RequireQuotaSafe = true
			_, err := admin.GetUsage(ctx, request)
			code := status.Code(err)
			evidence.StrictCode = code.String()
			if code != codes.FailedPrecondition {
				return fmt.Errorf("strict graph byte read returned %s, want FailedPrecondition", code)
			}
		}
		return nil
	}
	request.RequireQuotaSafe = true
	snapshot, err := awaitGraphUsageSnapshot(ctx, admin, request, func(snapshot *adminv1.UsageSnapshot) error {
		return expectGraphUsageMetric(snapshot, graphLogicalBytesMetric, expected.LogicalBytes, adminv1.UsageUnit_USAGE_UNIT_BYTES, true)
	})
	evidence.Logical = snapshot
	if err != nil || !expected.Diagnostic {
		return err
	}
	response, err := admin.GetUsage(ctx, &adminv1.GetUsageRequest{Scope: scope, MetricNames: []string{graphPhysicalBytesMetric}, IncludeDiagnosticPhysical: true})
	evidence.Diagnostic = response.GetSnapshot()
	if err != nil {
		return err
	}
	metrics := evidence.Diagnostic.GetMetrics()
	if !proto.Equal(evidence.Diagnostic.GetScope(), scope) || len(metrics) != 1 || metrics[0].GetName() != graphPhysicalBytesMetric || metrics[0].GetUnit() != adminv1.UsageUnit_USAGE_UNIT_BYTES || metrics[0].GetAuthority() != adminv1.UsageAuthority_USAGE_AUTHORITY_OBSERVED_APPROXIMATE || metrics[0].GetQuotaSafe() || metrics[0].GetHardLimitEligible() {
		return fmt.Errorf("missing or misclassified local physical graph observation")
	}
	return nil
}

func expectUnavailableGraphBytes(snapshot *adminv1.UsageSnapshot, scope *adminv1.UsageScope, reason adminv1.UsageStatusReason) error {
	if snapshot == nil || !proto.Equal(snapshot.GetScope(), scope) || snapshot.GetQuotaSafeComplete() || len(snapshot.GetMetrics()) != 0 {
		return fmt.Errorf("graph logical bytes must be withheld, not served as a partial value or zero")
	}
	for _, blocker := range snapshot.GetBlockers() {
		if blocker.GetMetricName() == graphLogicalBytesMetric && blocker.GetReason() == reason && blocker.GetSeverity() == adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_BLOCKER {
			return nil
		}
	}
	return fmt.Errorf("missing %s blocker for %s", reason, graphLogicalBytesMetric)
}

func expectGraphUsageMetric(snapshot *adminv1.UsageSnapshot, name string, value uint64, unit adminv1.UsageUnit, requireComplete bool) error {
	metrics := snapshot.GetMetrics()
	if len(metrics) != 1 || metrics[0].GetName() != name {
		return fmt.Errorf("metric %s missing or returned alongside unexpected metrics", name)
	}
	metric := metrics[0]
	if metric.GetMetricVersion() != 1 || !metric.GetQuotaSafe() || metric.GetUnit() != unit || metric.GetAuthority() != adminv1.UsageAuthority_USAGE_AUTHORITY_RAFT_COMMITTED_EXACT || metric.GetHardLimitEligible() != (name == graphNodeCountMetric) {
		return fmt.Errorf("invalid version, authority, units, or quota metadata for %s", name)
	}
	if requireComplete && (!snapshot.GetQuotaSafeComplete() || len(snapshot.GetBlockers()) != 0 || snapshot.GetAccountingState() != adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY) {
		return fmt.Errorf("metric %s is not quota-safe complete", name)
	}
	if metric.GetValue() != value {
		return fmt.Errorf("metric %s was %d, want %d", name, metric.GetValue(), value)
	}
	return nil
}

func awaitGraphUsageSnapshot(ctx context.Context, admin usageCheckClient, request *adminv1.GetUsageRequest, validate func(*adminv1.UsageSnapshot) error) (*adminv1.UsageSnapshot, error) {
	var last *adminv1.UsageSnapshot
	for {
		response, err := admin.GetUsage(ctx, request)
		if err == nil {
			last = response.GetSnapshot()
			if !proto.Equal(last.GetScope(), request.GetScope()) {
				return last, fmt.Errorf("usage snapshot returned a different scope")
			}
			err = validate(last)
			if err == nil {
				return last, nil
			}
		} else {
			switch status.Code(err) {
			case codes.Unavailable, codes.DeadlineExceeded, codes.FailedPrecondition:
			default:
				return last, err
			}
		}
		if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
			return last, fmt.Errorf("usage did not converge (%v): %w", err, waitErr)
		}
	}
}

func reconcileGraphUsage(ctx context.Context, admin usageCheckClient, scope *adminv1.UsageScope) (*adminv1.UsageAccountingHealth, error) {
	response, err := admin.GetUsageAccountingHealth(ctx, &adminv1.GetUsageAccountingHealthRequest{Scope: scope, IncludeReconciliation: true})
	health := response.GetHealth()
	if err != nil {
		return health, err
	}
	if !proto.Equal(health.GetScope(), scope) || !health.GetQuotaSafeReady() || health.GetState() != adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY || health.GetDriftScopeCount() != 0 || len(health.GetBlockers()) != 0 {
		return health, fmt.Errorf("space reconciliation not ready: state=%s driftScopes=%d blockers=%d", health.GetState(), health.GetDriftScopeCount(), len(health.GetBlockers()))
	}
	return health, nil
}

func usageScopeName(scope *adminv1.UsageScope) string {
	if scope.GetDomain() != nil {
		return "domain"
	}
	return "space"
}
