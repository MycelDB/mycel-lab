package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type RaftSnapshotOperationResult struct {
	Operation    string                        `json:"operation"`
	StatefulSet  string                        `json:"statefulSet,omitempty"`
	Ordinal      int                           `json:"ordinal,omitempty"`
	Replicas     int                           `json:"replicas,omitempty"`
	PVCName      string                        `json:"pvcName,omitempty"`
	OldPVCUID    string                        `json:"oldPvcUid,omitempty"`
	NewPVCUID    string                        `json:"newPvcUid,omitempty"`
	Nodes        []RaftSnapshotNodeResult      `json:"nodes,omitempty"`
	RejoinedNode *RaftSnapshotNodeVerification `json:"rejoinedNode,omitempty"`
}

type RaftSnapshotNodeResult struct {
	NodeName string               `json:"nodeName"`
	Ordinal  int                  `json:"ordinal"`
	Results  []RaftSnapshotResult `json:"results,omitempty"`
}

type RaftSnapshotResult struct {
	GroupID       string `json:"group_id"`
	SnapshotIndex uint64 `json:"snapshot_index,omitempty"`
	Compacted     bool   `json:"compacted"`
	Error         string `json:"error,omitempty"`
}

type RaftSnapshotNodeVerification struct {
	NodeName string                  `json:"nodeName"`
	Ordinal  int                     `json:"ordinal"`
	Groups   []RaftSnapshotGroupInfo `json:"groups"`
}

type RaftSnapshotGroupInfo struct {
	GroupID       string `json:"group_id"`
	Kind          string `json:"kind"`
	Health        string `json:"health"`
	SnapshotIndex uint64 `json:"snapshot_index,omitempty"`
	CommitIndex   uint64 `json:"commit_index,omitempty"`
	AppliedIndex  uint64 `json:"applied_index,omitempty"`
	LastIndex     uint64 `json:"last_index,omitempty"`
}

func executeRaftRejoinEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, eventType string, target map[string]any) (RaftSnapshotOperationResult, error) {
	switch eventType {
	case "raft-snapshot-create":
		return executeRaftSnapshotCreate(ctx, driver, environment, target)
	case "raft-pvc-replace-node":
		return executeRaftPVCReplaceNode(ctx, driver, environment, scenario, target)
	case "raft-snapshot-verify-rejoined":
		return executeRaftSnapshotVerifyRejoined(ctx, driver, environment, scenario, target)
	default:
		return RaftSnapshotOperationResult{Operation: eventType}, fmt.Errorf("unsupported raft rejoin event type %q", eventType)
	}
}

func executeRaftSnapshotCreate(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (RaftSnapshotOperationResult, error) {
	ordinals := intSliceTarget(target, "ordinals")
	if len(ordinals) == 0 {
		count := intTarget(target, "count", 0)
		if count <= 0 {
			count = 1
		}
		start := intTarget(target, "startOrdinal", 0)
		for i := 0; i < count; i++ {
			ordinals = append(ordinals, start+i)
		}
	}
	compact := boolTarget(target, "compact", true)
	cmd := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", "admin", "--password", "admin-password", "--output", "json", "cluster", "raft-snapshot", "create"}
	if !compact {
		cmd = append(cmd, "--compact=false")
	}
	for _, groupID := range stringSliceTarget(target, "groupIds") {
		cmd = append(cmd, "--group-id", groupID)
	}
	out := RaftSnapshotOperationResult{Operation: "raft-snapshot-create"}
	for _, ordinal := range ordinals {
		node := env.NodeRef{Ordinal: ordinal}
		res, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd})
		if err != nil {
			return out, err
		}
		parsed, err := parseRaftSnapshotOutput(res.Stdout)
		if err != nil {
			return out, fmt.Errorf("parse raft snapshot output for ordinal %d: %w", ordinal, err)
		}
		for _, item := range parsed {
			if item.Error != "" {
				return out, fmt.Errorf("raft snapshot on ordinal %d group %s failed: %s", ordinal, item.GroupID, item.Error)
			}
			if item.SnapshotIndex == 0 {
				return out, fmt.Errorf("raft snapshot on ordinal %d group %s returned zero snapshot index", ordinal, item.GroupID)
			}
		}
		out.Nodes = append(out.Nodes, RaftSnapshotNodeResult{NodeName: fmt.Sprintf("myceld-%d", ordinal), Ordinal: ordinal, Results: parsed})
	}
	return out, nil
}

func executeRaftPVCReplaceNode(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any) (RaftSnapshotOperationResult, error) {
	vr, ok := driver.(env.VolumeReplacementDriver)
	if !ok {
		return RaftSnapshotOperationResult{Operation: "raft-pvc-replace-node"}, fmt.Errorf("driver %q does not support volume replacement", driver.Name())
	}
	statefulSet := firstNonEmpty(stringTarget(target, "statefulSet"), "myceld")
	nodeCount := scenario.Cluster.Nodes
	if nodeCount <= 0 {
		nodeCount = intTarget(target, "nodeCount", 0)
	}
	if nodeCount <= 0 {
		return RaftSnapshotOperationResult{Operation: "raft-pvc-replace-node", StatefulSet: statefulSet}, fmt.Errorf("node count is required")
	}
	ordinal := intTarget(target, "ordinal", nodeCount-1)
	if ordinal != nodeCount-1 {
		return RaftSnapshotOperationResult{Operation: "raft-pvc-replace-node", StatefulSet: statefulSet, Ordinal: ordinal}, fmt.Errorf("raft-pvc-replace-node currently supports only the highest StatefulSet ordinal %d, got %d", nodeCount-1, ordinal)
	}
	pvcName := firstNonEmpty(stringTarget(target, "pvc"), fmt.Sprintf("data-%s-%d", statefulSet, ordinal))
	oldUIDs, err := vr.PVCUIDs(ctx, environment, statefulSet, nodeCount)
	if err != nil {
		return RaftSnapshotOperationResult{}, err
	}
	out := RaftSnapshotOperationResult{Operation: "raft-pvc-replace-node", StatefulSet: statefulSet, Ordinal: ordinal, Replicas: nodeCount, PVCName: pvcName, OldPVCUID: oldUIDs[pvcName]}
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, nodeCount-1); err != nil {
		return out, err
	}
	if err := vr.DeletePVC(ctx, environment, pvcName); err != nil {
		return out, err
	}
	if snapshotOrdinals := intSliceTarget(target, "snapshotOrdinals"); len(snapshotOrdinals) > 0 {
		snapshotTarget := map[string]any{"ordinals": snapshotOrdinals, "compact": boolTarget(target, "compact", true)}
		if groupIDs := stringSliceTarget(target, "groupIds"); len(groupIDs) > 0 {
			snapshotTarget["groupIds"] = groupIDs
		}
		snapshots, err := executeRaftSnapshotCreate(ctx, driver, environment, snapshotTarget)
		if err != nil {
			return out, fmt.Errorf("create active quorum snapshots before rejoin: %w", err)
		}
		out.Nodes = snapshots.Nodes
	}
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, nodeCount); err != nil {
		return out, err
	}
	if err := vr.WaitPVCs(ctx, environment, statefulSet, nodeCount); err != nil {
		return out, err
	}
	newUIDs, err := vr.PVCUIDs(ctx, environment, statefulSet, nodeCount)
	if err != nil {
		return out, err
	}
	out.NewPVCUID = newUIDs[pvcName]
	if out.OldPVCUID == "" || out.NewPVCUID == "" {
		return out, fmt.Errorf("missing PVC UID evidence for %s old=%q new=%q", pvcName, out.OldPVCUID, out.NewPVCUID)
	}
	if out.OldPVCUID == out.NewPVCUID {
		return out, fmt.Errorf("PVC %s was not replaced; UID remained %s", pvcName, out.OldPVCUID)
	}
	return out, nil
}

func executeRaftSnapshotVerifyRejoined(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any) (RaftSnapshotOperationResult, error) {
	nodeCount := scenario.Cluster.Nodes
	if nodeCount <= 0 {
		nodeCount = intTarget(target, "nodeCount", 0)
	}
	ordinal := intTarget(target, "ordinal", nodeCount-1)
	if ordinal < 0 {
		return RaftSnapshotOperationResult{Operation: "raft-snapshot-verify-rejoined"}, fmt.Errorf("ordinal is required")
	}
	verifyTimeout := durationTarget(target, "verifyTimeout", 5*time.Minute)
	verifyInterval := durationTarget(target, "verifyInterval", 5*time.Second)
	execTimeout := durationTarget(target, "execTimeout", 30*time.Second)
	if verifyInterval <= 0 {
		verifyInterval = time.Second
	}
	cmd := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", "admin", "--password", "admin-password", "--output", "json", "cluster", "raft-groups"}
	started := time.Now()
	var lastOut RaftSnapshotOperationResult
	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptCtx := ctx
		cancel := func() {}
		if execTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, execTimeout)
		}
		res, err := driver.Exec(attemptCtx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: cmd})
		cancel()
		out, verifyErr := verifyRaftSnapshotGroups(ordinal, res.Stdout)
		lastOut = out
		if err == nil && verifyErr == nil {
			return out, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = verifyErr
		}
		if verifyTimeout <= 0 || time.Since(started) >= verifyTimeout {
			return lastOut, fmt.Errorf("rejoined node myceld-%d did not report nonzero snapshot indexes after %d attempt(s) within %s: %w", ordinal, attempt, verifyTimeout, lastErr)
		}
		if err := sleepContext(ctx, verifyInterval); err != nil {
			return lastOut, err
		}
	}
}

func verifyRaftSnapshotGroups(ordinal int, raw string) (RaftSnapshotOperationResult, error) {
	out := RaftSnapshotOperationResult{Operation: "raft-snapshot-verify-rejoined", Ordinal: ordinal}
	groups, err := parseRaftGroupsOutput(raw)
	if err != nil {
		return out, err
	}
	if len(groups) == 0 {
		return out, fmt.Errorf("no raft groups reported by ordinal %d", ordinal)
	}
	missing := []string{}
	for _, group := range groups {
		if group.SnapshotIndex == 0 {
			missing = append(missing, group.GroupID)
		}
	}
	verification := RaftSnapshotNodeVerification{NodeName: fmt.Sprintf("myceld-%d", ordinal), Ordinal: ordinal, Groups: groups}
	out.RejoinedNode = &verification
	if len(missing) > 0 {
		return out, fmt.Errorf("rejoined node myceld-%d has zero snapshot_index for raft groups: %s", ordinal, strings.Join(missing, ", "))
	}
	return out, nil
}

func parseRaftSnapshotOutput(raw string) ([]RaftSnapshotResult, error) {
	var doc struct {
		Results []RaftSnapshotResult `json:"results"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, err
	}
	return doc.Results, nil
}

func parseRaftGroupsOutput(raw string) ([]RaftSnapshotGroupInfo, error) {
	var doc struct {
		Groups []RaftSnapshotGroupInfo `json:"groups"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, err
	}
	return doc.Groups, nil
}

func intSliceTarget(target map[string]any, key string) []int {
	values := stringSliceTarget(target, key)
	out := make([]int, 0, len(values))
	for _, value := range values {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil {
			out = append(out, parsed)
		}
	}
	return out
}

func stringSliceTarget(target map[string]any, key string) []string {
	if target == nil {
		return nil
	}
	value, ok := target[key]
	if !ok || value == nil {
		return nil
	}
	switch v := value.(type) {
	case []string:
		return append([]string(nil), v...)
	case []int:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, strconv.Itoa(item))
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, strings.TrimSpace(fmt.Sprint(item)))
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return []string{strings.TrimSpace(fmt.Sprint(v))}
	}
}
