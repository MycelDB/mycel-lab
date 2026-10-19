package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	Ready          bool                        `json:"ready"`
	Nodes          []ClusterRecoveryNodeResult `json:"nodes"`
}

type ClusterRecoveryNodeResult struct {
	Ordinal             int      `json:"ordinal"`
	ReadinessReady      bool     `json:"readinessReady"`
	RaftGroupsReady     bool     `json:"raftGroupsReady"`
	GroupCount          int      `json:"groupCount"`
	GroupsWithoutLeader []string `json:"groupsWithoutLeader,omitempty"`
	UnhealthyGroups     []string `json:"unhealthyGroups,omitempty"`
	ReadinessStdout     string   `json:"readinessStdout,omitempty"`
	ReadinessError      string   `json:"readinessError,omitempty"`
	RaftGroupsStdout    string   `json:"raftGroupsStdout,omitempty"`
	RaftGroupsError     string   `json:"raftGroupsError,omitempty"`
}

type clusterRaftGroupsResponse struct {
	Groups []struct {
		GroupID      string `json:"group_id"`
		LeaderNodeID uint64 `json:"leader_node_id"`
		Health       string `json:"health"`
	} `json:"groups"`
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
	ordinals := nodeTargetOrdinalSequence(target)
	if len(ordinals) == 0 {
		for ordinal := 0; ordinal < scenario.Cluster.Nodes; ordinal++ {
			ordinals = append(ordinals, ordinal)
		}
	}
	expectedGroups := intTarget(target, "expectedGroups", scenario.Cluster.Raft.PartitionCount+1)
	if expectedGroups <= 0 {
		expectedGroups = scenario.Cluster.Raft.PartitionCount + 1
	}
	result := ClusterRecoveryWaitResult{Timeout: timeout.String(), Interval: interval.String(), ExpectedGroups: expectedGroups}
	deadline := time.Now().Add(timeout)
	for {
		result.Attempts++
		result.Nodes = result.Nodes[:0]
		allReady := true
		for _, ordinal := range ordinals {
			nodeResult := inspectClusterRecoveryNode(ctx, driver, environment, ordinal, expectedGroups, target)
			result.Nodes = append(result.Nodes, nodeResult)
			if !nodeResult.ReadinessReady || !nodeResult.RaftGroupsReady {
				allReady = false
			}
		}
		if allReady {
			result.Ready = true
			return result, nil
		}
		if !time.Now().Before(deadline) {
			return result, fmt.Errorf("cluster did not recover readiness and %d Raft groups on ordinals %v within %s", expectedGroups, ordinals, timeout)
		}
		if err := sleepContext(ctx, interval); err != nil {
			return result, err
		}
	}
}

func inspectClusterRecoveryNode(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, ordinal, expectedGroups int, target map[string]any) ClusterRecoveryNodeResult {
	result := ClusterRecoveryNodeResult{Ordinal: ordinal}
	base := []string{
		"mycel",
		"--daemon-addr", firstNonEmpty(stringTarget(target, "daemonAddr"), "127.0.0.1:9091"),
		"--username", firstNonEmpty(stringTarget(target, "username"), provision.DefaultAdminUsername),
		"--password", firstNonEmpty(stringTarget(target, "password"), provision.DefaultAdminPassword),
		"--output", "json",
	}
	readinessCommand := append(append([]string(nil), base...), "cluster", "readiness", "check")
	readiness, readinessErr := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: readinessCommand})
	result.ReadinessStdout = bounded(readiness.Stdout, 8192)
	if readinessErr != nil {
		result.ReadinessError = bounded(strings.TrimSpace(readiness.Stderr+"\n"+readinessErr.Error()), 4096)
	} else {
		result.ReadinessReady = true
	}

	raftCommand := append(append([]string(nil), base...), "cluster", "raft-groups")
	raftGroups, raftErr := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: raftCommand})
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
	result.GroupCount = len(response.Groups)
	for _, group := range response.Groups {
		if group.LeaderNodeID == 0 {
			result.GroupsWithoutLeader = append(result.GroupsWithoutLeader, group.GroupID)
		}
		if !strings.EqualFold(strings.TrimSpace(group.Health), "healthy") {
			result.UnhealthyGroups = append(result.UnhealthyGroups, group.GroupID+":"+group.Health)
		}
	}
	result.RaftGroupsReady = result.GroupCount >= expectedGroups && len(result.GroupsWithoutLeader) == 0 && len(result.UnhealthyGroups) == 0
	if result.GroupCount < expectedGroups {
		result.RaftGroupsError = fmt.Sprintf("reported %d Raft groups, want at least %d", result.GroupCount, expectedGroups)
	}
	return result
}
