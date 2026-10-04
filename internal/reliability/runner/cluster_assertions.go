package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type clusterAssertionOptions struct {
	RequireHealthyCluster        bool `json:"requireHealthyCluster"`
	RequireSharedClusterIdentity bool `json:"requireSharedClusterIdentity"`
}

type clusterAssertionReport struct {
	Skipped       bool                         `json:"skipped,omitempty"`
	Reason        string                       `json:"reason,omitempty"`
	Required      clusterAssertionOptions      `json:"required"`
	ExpectedNodes int                          `json:"expectedNodes"`
	OK            bool                         `json:"ok"`
	ClusterID     string                       `json:"clusterId,omitempty"`
	Nodes         []clusterAssertionNodeReport `json:"nodes,omitempty"`
	Errors        []string                     `json:"errors,omitempty"`
}

type clusterAssertionNodeReport struct {
	NodeName         string   `json:"nodeName"`
	Resource         string   `json:"resource,omitempty"`
	ClusterID        string   `json:"clusterId,omitempty"`
	ClusterName      string   `json:"clusterName,omitempty"`
	Mode             string   `json:"mode,omitempty"`
	NodeID           string   `json:"nodeId,omitempty"`
	State            string   `json:"state,omitempty"`
	Admitted         bool     `json:"admitted"`
	PeerCount        int      `json:"peerCount"`
	HealthStatus     string   `json:"healthStatus,omitempty"`
	ActiveMembers    int      `json:"activeMembers"`
	PendingMembers   int      `json:"pendingMembers"`
	UnreachablePeers int      `json:"unreachablePeers"`
	Warnings         []string `json:"warnings,omitempty"`
	OK               bool     `json:"ok"`
	Errors           []string `json:"errors,omitempty"`
}

func runFinalClusterAssertions(ctx context.Context, sink *artifacts.Sink, scenario spec.ResolvedScenario, driver env.EnvironmentDriver, environment env.Environment, nodes []env.Node, live bool) error {
	required := finalClusterAssertionOptions(scenario.Assertions)
	if !required.RequireHealthyCluster && !required.RequireSharedClusterIdentity {
		return nil
	}
	report := clusterAssertionReport{Required: required, ExpectedNodes: scenario.Cluster.Nodes}
	defer func() {
		_, _ = sink.WriteJSON("oracle/cluster-report.json", report)
		_, _ = sink.WriteText("oracle/cluster-report.md", formatClusterAssertionReport(report))
	}()
	if !live {
		report.Skipped = true
		report.Reason = "live environment checks are disabled"
		report.OK = true
		return nil
	}
	if len(nodes) == 0 {
		report.Errors = append(report.Errors, "no environment nodes discovered")
		report.OK = false
		return fmt.Errorf("cluster assertions failed: no environment nodes discovered")
	}
	if report.ExpectedNodes <= 0 {
		report.ExpectedNodes = len(nodes)
	}
	clusterIDs := map[string]struct{}{}
	for _, node := range nodes {
		nodeReport := runClusterNodeAssertion(ctx, driver, environment, node, report.ExpectedNodes)
		report.Nodes = append(report.Nodes, nodeReport)
		if nodeReport.ClusterID != "" {
			clusterIDs[nodeReport.ClusterID] = struct{}{}
		}
		for _, err := range nodeReport.Errors {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", node.Name, err))
		}
	}
	if required.RequireSharedClusterIdentity || required.RequireHealthyCluster {
		if len(clusterIDs) != 1 {
			report.Errors = append(report.Errors, fmt.Sprintf("cluster IDs are not one shared non-empty value: %v", sortedStringKeys(clusterIDs)))
		} else {
			for id := range clusterIDs {
				report.ClusterID = id
			}
		}
	}
	report.OK = len(report.Errors) == 0
	if !report.OK {
		return fmt.Errorf("cluster assertions failed: %s", strings.Join(report.Errors, "; "))
	}
	return nil
}

func finalClusterAssertionOptions(assertions map[string]any) clusterAssertionOptions {
	final, _ := assertions["final"].(map[string]any)
	return clusterAssertionOptions{RequireHealthyCluster: boolFromMap(final, "requireHealthyCluster"), RequireSharedClusterIdentity: boolFromMap(final, "requireSharedClusterIdentity")}
}

func boolFromMap(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	switch v := values[key].(type) {
	case bool:
		return v
	case string:
		parsed, _ := strconv.ParseBool(v)
		return parsed
	default:
		return false
	}
}

func runClusterNodeAssertion(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.Node, expectedNodes int) clusterAssertionNodeReport {
	report := clusterAssertionNodeReport{NodeName: node.Name, Resource: node.Resource}
	statusOut, err := driver.Exec(ctx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", provision.DefaultAdminPassword, "--output", "json", "cluster", "status"}})
	if err != nil {
		report.Errors = append(report.Errors, "cluster status command failed: "+err.Error())
		return report
	}
	if err := parseClusterStatus(statusOut.Stdout, &report); err != nil {
		report.Errors = append(report.Errors, "parse cluster status: "+err.Error())
		return report
	}
	healthOut, err := driver.Exec(ctx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", provision.DefaultAdminPassword, "--output", "json", "cluster", "health"}})
	if err != nil {
		report.Errors = append(report.Errors, "cluster health command failed: "+err.Error())
		return report
	}
	if err := parseClusterHealth(healthOut.Stdout, &report); err != nil {
		report.Errors = append(report.Errors, "parse cluster health: "+err.Error())
		return report
	}
	if report.ClusterID == "" {
		report.Errors = append(report.Errors, "cluster ID is empty")
	}
	if report.Mode != "clustered" || report.State != "clustered" || !report.Admitted {
		report.Errors = append(report.Errors, fmt.Sprintf("node is not clustered/admitted: mode=%s state=%s admitted=%t", report.Mode, report.State, report.Admitted))
	}
	if report.PeerCount < expectedNodes {
		report.Errors = append(report.Errors, fmt.Sprintf("peer count %d below expected %d", report.PeerCount, expectedNodes))
	}
	if report.HealthStatus != "healthy" {
		report.Errors = append(report.Errors, fmt.Sprintf("health status is %s", report.HealthStatus))
	}
	if report.ActiveMembers < expectedNodes {
		report.Errors = append(report.Errors, fmt.Sprintf("active member count %d below expected %d", report.ActiveMembers, expectedNodes))
	}
	if report.PendingMembers != 0 || report.UnreachablePeers != 0 {
		report.Errors = append(report.Errors, fmt.Sprintf("pending/unreachable members: pending=%d unreachable=%d", report.PendingMembers, report.UnreachablePeers))
	}
	report.OK = len(report.Errors) == 0
	return report
}

func parseClusterStatus(raw string, report *clusterAssertionNodeReport) error {
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return err
	}
	cluster := mapValue(data, "cluster")
	node := mapValue(data, "node")
	report.ClusterID = stringValue(cluster, "cluster_id")
	report.ClusterName = stringValue(cluster, "cluster_name")
	report.Mode = stringValue(cluster, "mode")
	report.NodeID = stringValue(node, "node_id")
	report.State = stringValue(node, "state")
	report.Admitted = boolValue(node, "admitted")
	if peers, ok := data["peers"].([]any); ok {
		report.PeerCount = len(peers)
	}
	return nil
}

func parseClusterHealth(raw string, report *clusterAssertionNodeReport) error {
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return err
	}
	report.HealthStatus = stringValue(data, "status")
	report.ActiveMembers = intValue(data, "active_members")
	report.PendingMembers = intValue(data, "pending_members")
	report.UnreachablePeers = intValue(data, "unreachable_peers")
	if warnings, ok := data["warnings"].([]any); ok {
		for _, warning := range warnings {
			if s, ok := warning.(string); ok {
				report.Warnings = append(report.Warnings, s)
			}
		}
	}
	return nil
}

func mapValue(values map[string]any, key string) map[string]any {
	if child, ok := values[key].(map[string]any); ok {
		return child
	}
	return nil
}

func stringValue(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func boolValue(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	if value, ok := values[key].(bool); ok {
		return value
	}
	return false
}

func intValue(values map[string]any, key string) int {
	if values == nil {
		return 0
	}
	switch value := values[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		n, _ := value.Int64()
		return int(n)
	default:
		return 0
	}
}

func sortedStringKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func formatClusterAssertionReport(report clusterAssertionReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster assertion report\n\n")
	if report.Skipped {
		fmt.Fprintf(&b, "Skipped: %s\n", report.Reason)
		return b.String()
	}
	fmt.Fprintf(&b, "- OK: %t\n", report.OK)
	fmt.Fprintf(&b, "- Expected nodes: %d\n", report.ExpectedNodes)
	if report.ClusterID != "" {
		fmt.Fprintf(&b, "- Cluster ID: `%s`\n", report.ClusterID)
	}
	if len(report.Errors) > 0 {
		fmt.Fprintf(&b, "\n## Errors\n\n")
		for _, err := range report.Errors {
			fmt.Fprintf(&b, "- %s\n", err)
		}
	}
	if len(report.Nodes) > 0 {
		fmt.Fprintf(&b, "\n## Nodes\n\n")
		for _, node := range report.Nodes {
			fmt.Fprintf(&b, "- `%s`: ok=%t cluster=%s state=%s health=%s active=%d pending=%d unreachable=%d\n", node.NodeName, node.OK, node.ClusterID, node.State, node.HealthStatus, node.ActiveMembers, node.PendingMembers, node.UnreachablePeers)
		}
	}
	return b.String()
}
