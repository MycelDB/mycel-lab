package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
)

type ClusterRecoveryWaitResult struct {
	Attempts                  int                            `json:"attempts"`
	Timeout                   string                         `json:"timeout"`
	Interval                  string                         `json:"interval"`
	ExpectedGroups            int                            `json:"expectedGroups"`
	CommandTimeout            string                         `json:"commandTimeout"`
	MinimumStablePolls        int                            `json:"minimumStablePolls"`
	StablePolls               int                            `json:"stablePolls"`
	RequireZeroApplyLag       bool                           `json:"requireZeroApplyLag"`
	RequireConsistentProgress bool                           `json:"requireConsistentProgress"`
	InspectionMode            string                         `json:"inspectionMode"`
	ReusableEndpointSessions  int                            `json:"reusableEndpointSessions"`
	RejectedPolls             int                            `json:"rejectedPolls"`
	RejectionCounts           ClusterRecoveryRejectionCounts `json:"rejectionCounts"`
	LastRejectedPoll          *ClusterRecoveryRejectedPoll   `json:"lastRejectedPoll,omitempty"`
	Ready                     bool                           `json:"ready"`
	Nodes                     []ClusterRecoveryNodeResult    `json:"nodes"`
}

type ClusterRecoveryRejectionCounts struct {
	Readiness        int `json:"readiness"`
	RaftInspection   int `json:"raftInspection"`
	Topology         int `json:"topology"`
	Leadership       int `json:"leadership"`
	Health           int `json:"health"`
	ApplyLag         int `json:"applyLag"`
	ProgressMissing  int `json:"progressMissing"`
	ProgressRollback int `json:"progressRollback"`
}

type ClusterRecoveryRejectedPoll struct {
	Attempt           int                           `json:"attempt"`
	StablePollsBefore int                           `json:"stablePollsBefore"`
	RejectedNodeCount int                           `json:"rejectedNodeCount"`
	NodesTruncated    bool                          `json:"nodesTruncated"`
	Nodes             []ClusterRecoveryRejectedNode `json:"nodes"`
}

type ClusterRecoveryRejectedNode struct {
	Ordinal             int                                 `json:"ordinal"`
	Reasons             []string                            `json:"reasons"`
	LaggingGroups       []string                            `json:"laggingGroups,omitempty"`
	ProgressRegressions []ClusterRecoveryProgressRegression `json:"progressRegressions,omitempty"`
}

type ClusterRecoveryProgressRegression struct {
	GroupID               string `json:"groupId"`
	ObservedCommitIndex   string `json:"observedCommitIndex"`
	HighWaterCommitIndex  string `json:"highWaterCommitIndex"`
	ObservedAppliedIndex  string `json:"observedAppliedIndex"`
	HighWaterAppliedIndex string `json:"highWaterAppliedIndex"`
}

type ClusterRecoveryNodeResult struct {
	Ordinal               int                                 `json:"ordinal"`
	ReadinessReady        bool                                `json:"readinessReady"`
	RaftGroupsReady       bool                                `json:"raftGroupsReady"`
	GroupCount            int                                 `json:"groupCount"`
	MissingGroups         []string                            `json:"missingGroups,omitempty"`
	DuplicateGroups       []string                            `json:"duplicateGroups,omitempty"`
	UnexpectedGroups      []string                            `json:"unexpectedGroups,omitempty"`
	GroupsWithoutLeader   []string                            `json:"groupsWithoutLeader,omitempty"`
	UnhealthyGroups       []string                            `json:"unhealthyGroups,omitempty"`
	LaggingGroups         []string                            `json:"laggingGroups,omitempty"`
	MissingProgressGroups []string                            `json:"missingProgressGroups,omitempty"`
	RegressingGroups      []string                            `json:"regressingGroups,omitempty"`
	ProgressRegressions   []ClusterRecoveryProgressRegression `json:"progressRegressions,omitempty"`
	ReadinessStdout       string                              `json:"readinessStdout,omitempty"`
	ReadinessError        string                              `json:"readinessError,omitempty"`
	RaftGroupsStdout      string                              `json:"raftGroupsStdout,omitempty"`
	RaftGroupsError       string                              `json:"raftGroupsError,omitempty"`
	progress              map[string]clusterRaftProgress
	raftInspectionFailed  bool
}

type clusterRaftProgress struct {
	CommitIndex  uint64
	AppliedIndex uint64
}

type clusterRaftGroup struct {
	GroupID      string  `json:"group_id"`
	LeaderNodeID uint64  `json:"leader_node_id"`
	Health       string  `json:"health"`
	CommitIndex  *uint64 `json:"commit_index"`
	AppliedIndex *uint64 `json:"applied_index"`
	LastIndex    uint64  `json:"last_index"`
}

type clusterRaftGroupsResponse struct {
	Groups []clusterRaftGroup `json:"groups"`
}

type clusterRecoveryNodeInspector interface {
	Inspect(context.Context, int, []string, time.Duration, map[string]any) ClusterRecoveryNodeResult
	Close() error
}

type cliClusterRecoveryNodeInspector struct {
	driver      env.EnvironmentDriver
	environment env.Environment
}

func (i cliClusterRecoveryNodeInspector) Inspect(ctx context.Context, ordinal int, expectedGroupIDs []string, commandTimeout time.Duration, target map[string]any) ClusterRecoveryNodeResult {
	return inspectClusterRecoveryNode(ctx, i.driver, i.environment, ordinal, expectedGroupIDs, commandTimeout, target)
}

func (cliClusterRecoveryNodeInspector) Close() error { return nil }

type reusableClusterRecoverySession struct {
	admin   *mycel.AdminClient
	cluster adminv1.AdminClusterServiceClient
}

type reusableClusterRecoveryNodeInspector struct {
	sessions map[int]reusableClusterRecoverySession
}

func executeClusterRecoveryWaitEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any) (ClusterRecoveryWaitResult, error) {
	return executeClusterRecoveryWaitEventWithSessionFactory(ctx, driver, environment, scenario, target, newReusableClusterRecoveryNodeInspector)
}

type clusterRecoverySessionFactory func(context.Context, env.EnvironmentDriver, env.Environment, []int, time.Duration, map[string]any) (clusterRecoveryNodeInspector, error)

func executeClusterRecoveryWaitEventWithSessionFactory(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any, sessionFactory clusterRecoverySessionFactory) (ClusterRecoveryWaitResult, error) {
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
	minimumStablePolls := intTarget(target, "minimumStablePolls", 1)
	if minimumStablePolls < 1 || minimumStablePolls > 1000 {
		return ClusterRecoveryWaitResult{}, errors.New("cluster-recovery-wait minimumStablePolls must be between 1 and 1000")
	}
	requireZeroApplyLag := boolTarget(target, "requireZeroApplyLag", false)
	requireConsistentProgress := boolTarget(target, "requireConsistentProgress", false)
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
	result := ClusterRecoveryWaitResult{Timeout: timeout.String(), Interval: interval.String(), ExpectedGroups: expectedGroups, CommandTimeout: commandTimeout.String(), MinimumStablePolls: minimumStablePolls, RequireZeroApplyLag: requireZeroApplyLag, RequireConsistentProgress: requireConsistentProgress, InspectionMode: "node-cli"}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var inspector clusterRecoveryNodeInspector = cliClusterRecoveryNodeInspector{driver: driver, environment: environment}
	if boolTarget(target, "reuseEndpointSessions", false) {
		if sessionFactory == nil {
			return result, errors.New("cluster-recovery-wait reusable endpoint session factory is unavailable")
		}
		inspector, err = sessionFactory(waitCtx, driver, environment, ordinals, commandTimeout, target)
		if err != nil {
			return result, fmt.Errorf("initialize cluster-recovery-wait reusable endpoint sessions: %w", err)
		}
		result.InspectionMode = "reusable-endpoint-session"
		result.ReusableEndpointSessions = len(ordinals)
	}
	defer inspector.Close()
	timeoutError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("cluster did not recover stable readiness and exact Raft groups %v on ordinals %v within %s", expectedGroupIDs, ordinals, timeout)
	}
	progressHighWater := make(map[int]map[string]clusterRaftProgress, len(ordinals))
	for {
		result.Attempts++
		result.Nodes = make([]ClusterRecoveryNodeResult, len(ordinals))
		var inspections sync.WaitGroup
		for i, ordinal := range ordinals {
			inspections.Add(1)
			go func(index, nodeOrdinal int) {
				defer inspections.Done()
				result.Nodes[index] = inspector.Inspect(waitCtx, nodeOrdinal, expectedGroupIDs, commandTimeout, target)
			}(i, ordinal)
		}
		inspections.Wait()
		if waitCtx.Err() != nil {
			return result, timeoutError()
		}
		allReady := true
		for index := range result.Nodes {
			nodeResult := &result.Nodes[index]
			if requireConsistentProgress {
				enforceClusterProgressConsistency(nodeResult, progressHighWater[nodeResult.Ordinal])
			}
			progressHighWater[nodeResult.Ordinal] = mergeClusterProgressHighWater(progressHighWater[nodeResult.Ordinal], nodeResult.progress)
			if !nodeResult.ReadinessReady || !nodeResult.RaftGroupsReady {
				allReady = false
			}
		}
		if allReady {
			result.StablePolls++
			if result.StablePolls >= minimumStablePolls {
				result.Ready = true
				return result, nil
			}
		} else {
			recordClusterRecoveryRejection(&result)
			result.StablePolls = 0
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

func newReusableClusterRecoveryNodeInspector(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, ordinals []int, commandTimeout time.Duration, target map[string]any) (clusterRecoveryNodeInspector, error) {
	endpoints, err := driver.Endpoints(ctx, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve daemon endpoints: %w", err)
	}
	inspector := &reusableClusterRecoveryNodeInspector{sessions: make(map[int]reusableClusterRecoverySession, len(ordinals))}
	for _, ordinal := range ordinals {
		endpoint, ok := clusterRecoveryEndpointForOrdinal(endpoints, ordinal)
		if !ok || strings.TrimSpace(endpoint.DaemonAddr) == "" {
			_ = inspector.Close()
			return nil, fmt.Errorf("daemon endpoint for ordinal %d is unavailable", ordinal)
		}
		dialCtx, cancel := context.WithTimeout(ctx, commandTimeout)
		admin, dialErr := mycel.DialAdmin(dialCtx, mycel.Config{
			Addr:       endpoint.DaemonAddr,
			Username:   firstNonEmpty(stringTarget(target, "username"), provision.DefaultAdminUsername),
			Password:   firstNonEmpty(stringTarget(target, "password"), provision.DefaultAdminPassword),
			ClientName: "mycel-lab-cluster-recovery",
		})
		cancel()
		if dialErr != nil {
			_ = inspector.Close()
			return nil, fmt.Errorf("authenticate daemon endpoint for ordinal %d: %w", ordinal, dialErr)
		}
		inspector.sessions[ordinal] = reusableClusterRecoverySession{admin: admin, cluster: adminv1.NewAdminClusterServiceClient(admin.Conn)}
	}
	return inspector, nil
}

func clusterRecoveryEndpointForOrdinal(endpoints []env.Endpoint, ordinal int) (env.Endpoint, bool) {
	name := fmt.Sprintf("myceld-%d", ordinal)
	for _, endpoint := range endpoints {
		if endpoint.NodeName == name {
			return endpoint, true
		}
	}
	if ordinal >= 0 && ordinal < len(endpoints) {
		return endpoints[ordinal], true
	}
	return env.Endpoint{}, false
}

func (i *reusableClusterRecoveryNodeInspector) Inspect(ctx context.Context, ordinal int, expectedGroupIDs []string, commandTimeout time.Duration, target map[string]any) ClusterRecoveryNodeResult {
	result := ClusterRecoveryNodeResult{Ordinal: ordinal}
	session, ok := i.sessions[ordinal]
	if !ok || session.cluster == nil {
		result.ReadinessError = "reusable endpoint session is unavailable"
		result.RaftGroupsError = result.ReadinessError
		result.raftInspectionFailed = true
		return result
	}

	readinessCtx, readinessCancel := context.WithTimeout(ctx, commandTimeout)
	status, readinessErr := session.cluster.GetClusterStatus(readinessCtx, &adminv1.GetClusterStatusRequest{})
	readinessCancel()
	if readinessErr != nil {
		result.ReadinessError = bounded("get cluster status: "+readinessErr.Error(), 4096)
	} else {
		if encoded, marshalErr := json.Marshal(status.GetReadiness()); marshalErr == nil {
			result.ReadinessStdout = bounded(string(encoded), 8192)
		}
		if validationErr := validateReusableClusterReadiness(status.GetReadiness()); validationErr != nil {
			result.ReadinessError = bounded(validationErr.Error(), 4096)
		} else {
			result.ReadinessReady = true
		}
	}

	raftCtx, raftCancel := context.WithTimeout(ctx, commandTimeout)
	raftGroups, raftErr := session.cluster.ListRaftGroups(raftCtx, &adminv1.ListRaftGroupsRequest{})
	raftCancel()
	if raftErr != nil {
		result.RaftGroupsError = bounded("list raft groups: "+raftErr.Error(), 4096)
		result.raftInspectionFailed = true
		return result
	}
	groups := make([]clusterRaftGroup, 0, len(raftGroups.GetGroups()))
	for _, group := range raftGroups.GetGroups() {
		if group == nil {
			continue
		}
		commitIndex := group.GetCommitIndex()
		appliedIndex := group.GetAppliedIndex()
		groups = append(groups, clusterRaftGroup{
			GroupID:      group.GetGroupId(),
			LeaderNodeID: group.GetLeaderNodeId(),
			Health:       strings.TrimPrefix(strings.ToLower(group.GetHealth().String()), "raft_group_health_"),
			CommitIndex:  &commitIndex,
			AppliedIndex: &appliedIndex,
			LastIndex:    group.GetLastIndex(),
		})
	}
	if encoded, marshalErr := json.Marshal(clusterRaftGroupsResponse{Groups: groups}); marshalErr == nil {
		result.RaftGroupsStdout = bounded(string(encoded), 16384)
	}
	evaluateClusterRaftGroups(&result, groups, expectedGroupIDs, boolTarget(target, "requireZeroApplyLag", false), boolTarget(target, "requireConsistentProgress", false))
	return result
}

func (i *reusableClusterRecoveryNodeInspector) Close() error {
	if i == nil {
		return nil
	}
	var closeErr error
	for ordinal, session := range i.sessions {
		if session.admin == nil || session.admin.Conn == nil {
			continue
		}
		if err := session.admin.Conn.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close reusable endpoint session for ordinal %d: %w", ordinal, err))
		}
	}
	return closeErr
}

func validateReusableClusterReadiness(readiness *adminv1.ClusterReadiness) error {
	if readiness == nil {
		return errors.New("cluster not ready: readiness is unavailable")
	}
	processReady := readiness.GetProcessReady()
	metadataReady := readiness.GetMetadataReady()
	raftReady := readiness.GetRaftReady()
	readReady := readiness.GetReadReady()
	writeReady := readiness.GetWriteReady()
	if readiness.GetClientReady() && !processReady && !metadataReady && !raftReady && !readReady && !writeReady {
		processReady = true
		metadataReady = readiness.GetMetadataApplied() && readiness.GetMetadataValidated()
		raftReady = readiness.GetPartitionGroupsStarted()
		readReady = true
		writeReady = true
	}
	blockers := append([]string(nil), readiness.GetReadinessBlockers()...)
	if !readiness.GetClientReady() {
		blockers = append(blockers, "client_ready=false")
	}
	if !processReady {
		blockers = append(blockers, "process_ready=false")
	}
	if !readiness.GetMetadataApplied() {
		blockers = append(blockers, "metadata_applied=false")
	}
	if !readiness.GetMetadataValidated() {
		blockers = append(blockers, "metadata_validated=false")
	}
	if !metadataReady {
		blockers = append(blockers, "metadata_ready=false")
	}
	if !readiness.GetPartitionGroupsStarted() {
		blockers = append(blockers, "partition_groups_started=false")
	}
	if !raftReady {
		blockers = append(blockers, "raft_ready=false")
	}
	if !readReady {
		blockers = append(blockers, "read_ready=false")
	}
	if !writeReady {
		blockers = append(blockers, "write_ready=false")
	}
	if authoritative, local := readiness.GetAuthoritativeClusterId(), readiness.GetLocalClusterId(); authoritative != "" && local != "" && authoritative != local {
		blockers = append(blockers, fmt.Sprintf("cluster_id_mismatch authoritative=%s local=%s", authoritative, local))
	}
	if len(blockers) > 0 {
		return fmt.Errorf("cluster not ready: %s", strings.Join(blockers, "; "))
	}
	return nil
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
		result.raftInspectionFailed = true
		return result
	}
	var response clusterRaftGroupsResponse
	if err := json.Unmarshal([]byte(raftGroups.Stdout), &response); err != nil {
		result.RaftGroupsError = "decode raft groups: " + err.Error()
		result.raftInspectionFailed = true
		return result
	}
	evaluateClusterRaftGroups(&result, response.Groups, expectedGroupIDs, boolTarget(target, "requireZeroApplyLag", false), boolTarget(target, "requireConsistentProgress", false))
	return result
}

func evaluateClusterRaftGroups(result *ClusterRecoveryNodeResult, groups []clusterRaftGroup, expectedGroupIDs []string, requireZeroApplyLag, requireConsistentProgress bool) {
	result.GroupCount = len(groups)
	result.progress = make(map[string]clusterRaftProgress, len(groups))
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
		if group.CommitIndex == nil || group.AppliedIndex == nil {
			if requireZeroApplyLag || requireConsistentProgress {
				result.MissingProgressGroups = append(result.MissingProgressGroups, groupID)
			}
			continue
		}
		result.progress[groupID] = clusterRaftProgress{CommitIndex: *group.CommitIndex, AppliedIndex: *group.AppliedIndex}
		if requireZeroApplyLag && *group.CommitIndex != *group.AppliedIndex {
			result.LaggingGroups = append(result.LaggingGroups, fmt.Sprintf("%s:%d", groupID, absoluteDifference(*group.CommitIndex, *group.AppliedIndex)))
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
	sort.Strings(result.LaggingGroups)
	sort.Strings(result.MissingProgressGroups)
	result.RaftGroupsReady = result.GroupCount == len(expectedGroupIDs) && len(result.MissingGroups) == 0 && len(result.DuplicateGroups) == 0 && len(result.UnexpectedGroups) == 0 && len(result.GroupsWithoutLeader) == 0 && len(result.UnhealthyGroups) == 0 && len(result.LaggingGroups) == 0 && len(result.MissingProgressGroups) == 0
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
	if len(result.LaggingGroups) > 0 {
		reasons = append(reasons, "groups with apply lag: "+strings.Join(result.LaggingGroups, ", "))
	}
	if len(result.MissingProgressGroups) > 0 {
		reasons = append(reasons, "groups missing committed progress: "+strings.Join(result.MissingProgressGroups, ", "))
	}
	result.RaftGroupsError = strings.Join(reasons, "; ")
}

func enforceClusterProgressConsistency(result *ClusterRecoveryNodeResult, previous map[string]clusterRaftProgress) {
	if !result.RaftGroupsReady || len(previous) == 0 || len(result.progress) == 0 {
		return
	}
	for groupID, current := range result.progress {
		prior, ok := previous[groupID]
		if !ok {
			continue
		}
		if current.CommitIndex < prior.CommitIndex || current.AppliedIndex < prior.AppliedIndex {
			result.RegressingGroups = append(result.RegressingGroups, groupID)
			result.ProgressRegressions = append(result.ProgressRegressions, ClusterRecoveryProgressRegression{
				GroupID:               groupID,
				ObservedCommitIndex:   strconv.FormatUint(current.CommitIndex, 10),
				HighWaterCommitIndex:  strconv.FormatUint(prior.CommitIndex, 10),
				ObservedAppliedIndex:  strconv.FormatUint(current.AppliedIndex, 10),
				HighWaterAppliedIndex: strconv.FormatUint(prior.AppliedIndex, 10),
			})
		}
	}
	if len(result.RegressingGroups) == 0 {
		return
	}
	sort.Strings(result.RegressingGroups)
	sort.Slice(result.ProgressRegressions, func(i, j int) bool {
		return result.ProgressRegressions[i].GroupID < result.ProgressRegressions[j].GroupID
	})
	result.RaftGroupsReady = false
	reason := "groups with regressing progress: " + strings.Join(result.RegressingGroups, ", ")
	if result.RaftGroupsError == "" {
		result.RaftGroupsError = reason
	} else {
		result.RaftGroupsError += "; " + reason
	}
}

func recordClusterRecoveryRejection(result *ClusterRecoveryWaitResult) {
	const maximumRejectedNodes = 32
	result.RejectedPolls++
	rejected := &ClusterRecoveryRejectedPoll{Attempt: result.Attempts, StablePollsBefore: result.StablePolls}
	for _, node := range result.Nodes {
		summary := ClusterRecoveryRejectedNode{Ordinal: node.Ordinal}
		addReason := func(reason string, count *int) {
			summary.Reasons = append(summary.Reasons, reason)
			(*count)++
		}
		if !node.ReadinessReady {
			addReason("readiness", &result.RejectionCounts.Readiness)
		}
		if node.raftInspectionFailed {
			addReason("raft-inspection", &result.RejectionCounts.RaftInspection)
		}
		topologyRejected := len(node.MissingGroups) > 0 || len(node.DuplicateGroups) > 0 || len(node.UnexpectedGroups) > 0
		if topologyRejected {
			addReason("topology", &result.RejectionCounts.Topology)
		}
		if len(node.GroupsWithoutLeader) > 0 {
			addReason("leadership", &result.RejectionCounts.Leadership)
		}
		if len(node.UnhealthyGroups) > 0 {
			addReason("health", &result.RejectionCounts.Health)
		}
		if len(node.LaggingGroups) > 0 {
			addReason("apply-lag", &result.RejectionCounts.ApplyLag)
			if !topologyRejected {
				summary.LaggingGroups = boundedRecoveryGroups(node.LaggingGroups)
			}
		}
		if len(node.MissingProgressGroups) > 0 {
			addReason("progress-missing", &result.RejectionCounts.ProgressMissing)
		}
		if len(node.ProgressRegressions) > 0 {
			addReason("progress-rollback", &result.RejectionCounts.ProgressRollback)
			summary.ProgressRegressions = boundedProgressRegressions(node.ProgressRegressions)
		}
		if len(summary.Reasons) > 0 {
			rejected.RejectedNodeCount++
			if len(rejected.Nodes) < maximumRejectedNodes {
				rejected.Nodes = append(rejected.Nodes, summary)
			} else {
				rejected.NodesTruncated = true
			}
		}
	}
	result.LastRejectedPoll = rejected
}

func boundedRecoveryGroups(groups []string) []string {
	const maximumGroups = 64
	if len(groups) > maximumGroups {
		groups = groups[:maximumGroups]
	}
	return append([]string(nil), groups...)
}

func boundedProgressRegressions(regressions []ClusterRecoveryProgressRegression) []ClusterRecoveryProgressRegression {
	const maximumGroups = 64
	if len(regressions) > maximumGroups {
		regressions = regressions[:maximumGroups]
	}
	return append([]ClusterRecoveryProgressRegression(nil), regressions...)
}

func mergeClusterProgressHighWater(highWater, observed map[string]clusterRaftProgress) map[string]clusterRaftProgress {
	if len(observed) == 0 {
		return highWater
	}
	if highWater == nil {
		highWater = make(map[string]clusterRaftProgress, len(observed))
	}
	for groupID, current := range observed {
		prior := highWater[groupID]
		if current.CommitIndex > prior.CommitIndex {
			prior.CommitIndex = current.CommitIndex
		}
		if current.AppliedIndex > prior.AppliedIndex {
			prior.AppliedIndex = current.AppliedIndex
		}
		highWater[groupID] = prior
	}
	return highWater
}

func absoluteDifference(left, right uint64) uint64 {
	if left >= right {
		return left - right
	}
	return right - left
}
