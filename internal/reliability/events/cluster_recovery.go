package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type ClusterRecoveryWaitResult struct {
	Attempts       int                         `json:"attempts"`
	Timeout        string                      `json:"timeout"`
	Interval       string                      `json:"interval"`
	ExpectedGroups int                         `json:"expectedGroups"`
	CommandTimeout string                      `json:"commandTimeout"`
	Ready          bool                        `json:"ready"`
	Nodes          []ClusterRecoveryNodeResult `json:"nodes"`
}

type ClusterRecoveryNodeResult struct {
	Ordinal             int      `json:"ordinal"`
	ReadinessReady      bool     `json:"readinessReady"`
	RaftGroupsReady     bool     `json:"raftGroupsReady"`
	GroupCount          int      `json:"groupCount"`
	MissingGroups       []string `json:"missingGroups,omitempty"`
	DuplicateGroups     []string `json:"duplicateGroups,omitempty"`
	UnexpectedGroups    []string `json:"unexpectedGroups,omitempty"`
	GroupsWithoutLeader []string `json:"groupsWithoutLeader,omitempty"`
	UnhealthyGroups     []string `json:"unhealthyGroups,omitempty"`
	ReadinessStdout     string   `json:"readinessStdout,omitempty"`
	ReadinessError      string   `json:"readinessError,omitempty"`
	RaftGroupsStdout    string   `json:"raftGroupsStdout,omitempty"`
	RaftGroupsError     string   `json:"raftGroupsError,omitempty"`
}

type clusterRaftGroup struct {
	GroupID      string `json:"group_id"`
	LeaderNodeID uint64 `json:"leader_node_id"`
	Health       string `json:"health"`
}

type clusterRaftGroupsResponse struct {
	Groups []clusterRaftGroup `json:"groups"`
}

func executeClusterRecoveryWaitEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any) (ClusterRecoveryWaitResult, error) {
	timeout, err := targetDuration(target, "timeout")
	if err != nil {
		return ClusterRecoveryWaitResult{}, err
	}
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	interval, err := targetDuration(target, "interval")
	if err != nil {
		return ClusterRecoveryWaitResult{}, err
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	commandTimeout, err := targetDuration(target, "commandTimeout")
	if err != nil {
		return ClusterRecoveryWaitResult{}, err
	}
	if commandTimeout <= 0 {
		commandTimeout = 10 * time.Second
	}
	if commandTimeout > timeout {
		commandTimeout = timeout
	}
	ordinals := nodeTargetOrdinalSequence(target)
	if len(ordinals) == 0 {
		for ordinal := 0; ordinal < scenario.Cluster.Nodes; ordinal++ {
			ordinals = append(ordinals, ordinal)
		}
	}
	expectedGroupIDs := requiredRaftGroupIDs(scenario.Cluster.Raft.PartitionCount)
	expectedGroups := len(expectedGroupIDs)
	if configured := intTarget(target, "expectedGroups", expectedGroups); configured != expectedGroups {
		return ClusterRecoveryWaitResult{}, fmt.Errorf("cluster-recovery-wait expectedGroups=%d does not match system plus %d configured partitions (%d groups)", configured, scenario.Cluster.Raft.PartitionCount, expectedGroups)
	}
	result := ClusterRecoveryWaitResult{Timeout: timeout.String(), Interval: interval.String(), ExpectedGroups: expectedGroups, CommandTimeout: commandTimeout.String()}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	timeoutError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("cluster did not recover readiness and exact Raft groups %v on ordinals %v within %s", expectedGroupIDs, ordinals, timeout)
	}
	for {
		result.Attempts++
		result.Nodes = make([]ClusterRecoveryNodeResult, len(ordinals))
		var inspections sync.WaitGroup
		for i, ordinal := range ordinals {
			inspections.Add(1)
			go func(index, nodeOrdinal int) {
				defer inspections.Done()
				result.Nodes[index] = inspectClusterRecoveryNode(waitCtx, driver, environment, nodeOrdinal, expectedGroupIDs, commandTimeout, target)
			}(i, ordinal)
		}
		inspections.Wait()
		allReady := true
		for _, nodeResult := range result.Nodes {
			if !nodeResult.ReadinessReady || !nodeResult.RaftGroupsReady {
				allReady = false
			}
		}
		if allReady {
			result.Ready = true
			return result, nil
		}
		if waitCtx.Err() != nil {
			return result, timeoutError()
		}
		if err := sleepContext(waitCtx, interval); err != nil {
			if waitCtx.Err() != nil {
				return result, timeoutError()
			}
			return result, err
		}
	}
}

func requiredRaftGroupIDs(partitionCount int) []string {
	if partitionCount < 0 {
		partitionCount = 0
	}
	ids := make([]string, 0, partitionCount+1)
	ids = append(ids, "system")
	for partition := 0; partition < partitionCount; partition++ {
		ids = append(ids, fmt.Sprintf("space-partition-%d", partition))
	}
	return ids
}

func inspectClusterRecoveryNode(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, ordinal int, expectedGroupIDs []string, commandTimeout time.Duration, target map[string]any) ClusterRecoveryNodeResult {
	result := ClusterRecoveryNodeResult{Ordinal: ordinal}
	base := []string{
		"mycel",
		"--daemon-addr", firstNonEmpty(stringTarget(target, "daemonAddr"), "127.0.0.1:9091"),
		"--username", firstNonEmpty(stringTarget(target, "username"), provision.DefaultAdminUsername),
		"--password", firstNonEmpty(stringTarget(target, "password"), provision.DefaultAdminPassword),
		"--output", "json",
	}
	readinessCommand := append(append([]string(nil), base...), "cluster", "readiness", "check")
	readinessCtx, readinessCancel := context.WithTimeout(ctx, commandTimeout)
	readiness, readinessErr := driver.Exec(readinessCtx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: readinessCommand})
	readinessCancel()
	result.ReadinessStdout = bounded(readiness.Stdout, 8192)
	if readinessErr != nil {
		result.ReadinessError = bounded(strings.TrimSpace(readiness.Stderr+"\n"+readinessErr.Error()), 4096)
	} else {
		result.ReadinessReady = true
	}

	raftCommand := append(append([]string(nil), base...), "cluster", "raft-groups")
	raftCtx, raftCancel := context.WithTimeout(ctx, commandTimeout)
	raftGroups, raftErr := driver.Exec(raftCtx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: raftCommand})
	raftCancel()
	result.RaftGroupsStdout = bounded(raftGroups.Stdout, 16384)
	if raftErr != nil {
		result.RaftGroupsError = bounded(strings.TrimSpace(raftGroups.Stderr+"\n"+raftErr.Error()), 4096)
		return result
	}
	var response clusterRaftGroupsResponse
	if err := json.Unmarshal([]byte(raftGroups.Stdout), &response); err != nil {
		result.RaftGroupsError = "decode raft groups: " + err.Error()
		return result
	}
	evaluateClusterRaftGroups(&result, response.Groups, expectedGroupIDs)
	return result
}

func evaluateClusterRaftGroups(result *ClusterRecoveryNodeResult, groups []clusterRaftGroup, expectedGroupIDs []string) {
	result.GroupCount = len(groups)
	expected := make(map[string]struct{}, len(expectedGroupIDs))
	for _, groupID := range expectedGroupIDs {
		expected[groupID] = struct{}{}
	}
	counts := make(map[string]int, len(groups))
	for _, group := range groups {
		groupID := strings.TrimSpace(group.GroupID)
		counts[groupID]++
		if group.LeaderNodeID == 0 {
			result.GroupsWithoutLeader = append(result.GroupsWithoutLeader, groupID)
		}
		if !strings.EqualFold(strings.TrimSpace(group.Health), "healthy") {
			result.UnhealthyGroups = append(result.UnhealthyGroups, groupID+":"+group.Health)
		}
	}
	for _, groupID := range expectedGroupIDs {
		switch counts[groupID] {
		case 0:
			result.MissingGroups = append(result.MissingGroups, groupID)
		case 1:
		default:
			result.DuplicateGroups = append(result.DuplicateGroups, groupID)
		}
	}
	for groupID, count := range counts {
		if _, ok := expected[groupID]; !ok {
			result.UnexpectedGroups = append(result.UnexpectedGroups, groupID)
		}
		if count > 1 {
			if _, ok := expected[groupID]; !ok {
				result.DuplicateGroups = append(result.DuplicateGroups, groupID)
			}
		}
	}
	sort.Strings(result.MissingGroups)
	sort.Strings(result.DuplicateGroups)
	sort.Strings(result.UnexpectedGroups)
	sort.Strings(result.GroupsWithoutLeader)
	sort.Strings(result.UnhealthyGroups)
	result.RaftGroupsReady = result.GroupCount == len(expectedGroupIDs) && len(result.MissingGroups) == 0 && len(result.DuplicateGroups) == 0 && len(result.UnexpectedGroups) == 0 && len(result.GroupsWithoutLeader) == 0 && len(result.UnhealthyGroups) == 0
	if result.RaftGroupsReady {
		return
	}
	var reasons []string
	if result.GroupCount != len(expectedGroupIDs) {
		reasons = append(reasons, fmt.Sprintf("reported %d Raft groups, want exactly %d", result.GroupCount, len(expectedGroupIDs)))
	}
	if len(result.MissingGroups) > 0 {
		reasons = append(reasons, "missing groups: "+strings.Join(result.MissingGroups, ", "))
	}
	if len(result.DuplicateGroups) > 0 {
		reasons = append(reasons, "duplicate groups: "+strings.Join(result.DuplicateGroups, ", "))
	}
	if len(result.UnexpectedGroups) > 0 {
		reasons = append(reasons, "unexpected groups: "+strings.Join(result.UnexpectedGroups, ", "))
	}
	if len(result.GroupsWithoutLeader) > 0 {
		reasons = append(reasons, "groups without leader: "+strings.Join(result.GroupsWithoutLeader, ", "))
	}
	if len(result.UnhealthyGroups) > 0 {
		reasons = append(reasons, "unhealthy groups: "+strings.Join(result.UnhealthyGroups, ", "))
	}
	result.RaftGroupsError = strings.Join(reasons, "; ")
}
