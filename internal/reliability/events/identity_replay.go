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

type IdentityDuplicatePrincipalReplayResult struct {
	Operation         string                      `json:"operation"`
	SnapshotOrdinal   int                         `json:"snapshotOrdinal"`
	LeaderNodeID      uint64                      `json:"leaderNodeId,omitempty"`
	DuplicateOrdinal  int                         `json:"duplicateOrdinal"`
	RestartOrdinal    int                         `json:"restartOrdinal"`
	DuplicateUsername string                      `json:"duplicateUsername"`
	Snapshot          *identityRaftSnapshotResult `json:"snapshot,omitempty"`
	DuplicateCommand  string                      `json:"duplicateCommand,omitempty"`
	DuplicateStdout   string                      `json:"duplicateStdout,omitempty"`
	DuplicateStderr   string                      `json:"duplicateStderr,omitempty"`
}

func executeIdentityReplayEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any) (IdentityDuplicatePrincipalReplayResult, error) {
	operation := "identity-duplicate-principal-replay"
	username := stringTarget(target, "username")
	if username == "" {
		username = provision.DefaultAdminUsername
	}
	password := stringTarget(target, "password")
	if password == "" {
		password = provision.DefaultAdminPassword
	}
	newPassword := stringTarget(target, "newPassword")
	if newPassword == "" {
		newPassword = "mycel-lab-duplicate-admin-password"
	}
	nodeCount := scenario.Cluster.Nodes
	if nodeCount <= 0 {
		nodeCount = intTarget(target, "nodeCount", 3)
	}
	compact := boolTarget(target, "compact", true)

	leaderOrdinal, leaderNodeID, snapshot, err := snapshotSystemRaftLeader(ctx, driver, environment, nodeCount, compact, password)
	result := IdentityDuplicatePrincipalReplayResult{Operation: operation, SnapshotOrdinal: leaderOrdinal, LeaderNodeID: leaderNodeID, DuplicateOrdinal: leaderOrdinal, RestartOrdinal: leaderOrdinal, DuplicateUsername: username, Snapshot: snapshot}
	if err != nil {
		return result, err
	}

	cmd := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", password, "--output", "json", "user", "add", "--principal-username", username, "--new-password", newPassword}
	duplicate, dupErr := driver.Exec(ctx, environment, env.NodeRef{Ordinal: leaderOrdinal}, env.ExecRequest{Command: cmd})
	result.DuplicateCommand = duplicate.Command
	result.DuplicateStdout = duplicate.Stdout
	result.DuplicateStderr = duplicate.Stderr
	combined := duplicate.Stdout + "\n" + duplicate.Stderr + "\n"
	if dupErr == nil {
		return result, fmt.Errorf("duplicate principal command unexpectedly succeeded on ordinal %d", leaderOrdinal)
	}
	if !strings.Contains(strings.ToLower(combined+dupErr.Error()), "principal already exists") && !strings.Contains(strings.ToLower(combined+dupErr.Error()), "alreadyexists") && !strings.Contains(strings.ToLower(combined+dupErr.Error()), "already exists") {
		return result, fmt.Errorf("duplicate principal command failed without expected duplicate-principal error: %w", dupErr)
	}

	settle := durationTarget(target, "settle", 2*time.Second)
	if settle > 0 {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(settle):
		}
	}
	if err := driver.RestartNode(ctx, environment, env.NodeRef{Ordinal: leaderOrdinal}); err != nil {
		return result, fmt.Errorf("restart ordinal %d after duplicate principal tail: %w", leaderOrdinal, err)
	}
	return result, nil
}

func snapshotSystemRaftLeader(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, nodeCount int, compact bool, password string) (int, uint64, *identityRaftSnapshotResult, error) {
	if nodeCount <= 0 {
		nodeCount = 1
	}
	for ordinal := 0; ordinal < nodeCount; ordinal++ {
		cmd := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", password, "--output", "json", "cluster", "raft-snapshot", "create", "--group-id", "system"}
		if !compact {
			cmd = append(cmd, "--compact=false")
		}
		res, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: cmd})
		if err != nil {
			return ordinal, 0, nil, fmt.Errorf("create system raft snapshot on ordinal %d: %w", ordinal, err)
		}
		var out identityRaftSnapshotOutput
		if err := json.Unmarshal([]byte(res.Stdout), &out); err != nil {
			return ordinal, 0, nil, fmt.Errorf("parse raft snapshot output from ordinal %d: %w; stdout=%s", ordinal, err, res.Stdout)
		}
		for _, snapshot := range out.Results {
			if snapshot.GroupID != "system" || snapshot.Before == nil {
				continue
			}
			if snapshot.Before.LocalNodeID != 0 && snapshot.Before.LocalNodeID == snapshot.Before.LeaderNodeID {
				copySnapshot := snapshot
				return ordinal, snapshot.Before.LeaderNodeID, &copySnapshot, nil
			}
		}
	}
	return -1, 0, nil, fmt.Errorf("could not identify local system raft leader across %d node(s)", nodeCount)
}

type identityRaftSnapshotOutput struct {
	Results []identityRaftSnapshotResult `json:"results"`
}

type identityRaftSnapshotResult struct {
	GroupID       string                  `json:"group_id"`
	SnapshotIndex uint64                  `json:"snapshot_index,omitempty"`
	Compacted     bool                    `json:"compacted"`
	Error         string                  `json:"error,omitempty"`
	Before        *identityRaftGroupState `json:"before,omitempty"`
}

type identityRaftGroupState struct {
	GroupID      string `json:"group_id"`
	LocalNodeID  uint64 `json:"local_node_id"`
	LeaderNodeID uint64 `json:"leader_node_id,omitempty"`
}
