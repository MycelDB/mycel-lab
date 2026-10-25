package events

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestDriverRuntimeCleansSensitiveSystemBackupHostDirectories(t *testing.T) {
	if safeSystemBackupHostDir(string(os.PathSeparator)) {
		t.Fatal("filesystem root accepted as backup cleanup path")
	}
	hostDir := t.TempDir()
	archive := filepath.Join(hostDir, "system-backup.tar")
	if err := os.WriteFile(archive, []byte("sensitive"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := NewDriverRuntime(nil, env.Environment{})
	runtime.systemBackups["default"] = SystemBackupOperationResult{HostDir: hostDir}
	if err := runtime.CleanupSensitiveHostArtifacts(); err != nil {
		t.Fatalf("CleanupSensitiveHostArtifacts() error=%v", err)
	}
	if _, err := os.Stat(hostDir); !os.IsNotExist(err) {
		t.Fatalf("host backup directory still exists: %v", err)
	}
	runtime.systemBackups["unsafe"] = SystemBackupOperationResult{HostDir: string(os.PathSeparator)}
	if err := runtime.CleanupSensitiveHostArtifacts(); err == nil {
		t.Fatal("unsafe sensitive cleanup path did not fail closed")
	}
}

func TestEventScheduleStartsAtPhaseBoundary(t *testing.T) {
	phase := spec.PhaseSpec{Name: "outage", Events: []spec.EventSpec{{Type: "pod-stop", Target: map[string]any{"pod": "myceld-1"}}}}
	recorder := &recordingEventRecorder{}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, recorder); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	if len(recorder.records) != 2 {
		t.Fatalf("records=%d, want start+complete", len(recorder.records))
	}
	if recorder.records[0].Type != "event-started" || recorder.records[0].PhaseName != "outage" || recorder.records[0].EventType != "pod-stop" {
		t.Fatalf("first record not event-started at phase boundary: %+v", recorder.records[0])
	}
}

func TestExpectedDegradationDoesNotMaskCorrectnessFailure(t *testing.T) {
	phase := spec.PhaseSpec{Name: "outage", Outcome: map[string]string{"availability": "expected-degradation"}}
	if !FailureIsMaskedByExpectedDegradation(phase, FailureUnavailable) {
		t.Fatal("unavailable failure should be expected degradation")
	}
	if FailureIsMaskedByExpectedDegradation(phase, FailureCorrectness) {
		t.Fatal("correctness failure must not be masked")
	}
	if !CorrectnessFailureFailsScenario(phase) {
		t.Fatal("correctness failure should fail scenario")
	}
}

func TestPodNameTargetingIsDeterministic(t *testing.T) {
	target := map[string]any{"pod": "myceld-2"}
	if PodName(target) != "myceld-2" || PodName(target) != "myceld-2" {
		t.Fatalf("pod targeting is not deterministic")
	}
}

func TestNodeRestartAcceptsOrdinalTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinal": 1}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
}

func TestHostCommandRequiresCommandTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "host", Events: []spec.EventSpec{{Type: "host-command", Target: map[string]any{}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want missing command target")
	}
}

func TestUserBackupEventBuildsExportCommandFromActorAssignment(t *testing.T) {
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", Username: "mlab-writer"}}}
	cmd, actorID, username, err := userBackupCommand("user-backup-export", map[string]any{"actorId": "writer-0", "file": "/tmp/user.tar.zst", "compression": "zstd", "sourceLabel": "test"}, resources, "/tmp/user.tar.zst")
	if err != nil {
		t.Fatalf("userBackupCommand() error=%v", err)
	}
	joined := strings.Join(cmd, " ")
	for _, want := range []string{"admin user-backup export", "--source-username mlab-writer", "--file /tmp/user.tar.zst", "--compression zstd", "--source-label test"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("command %q missing %q", joined, want)
		}
	}
	if actorID != "writer-0" || username != "mlab-writer" {
		t.Fatalf("actorID=%q username=%q", actorID, username)
	}
}

func TestUserBackupImportBuildsCreateUserExecuteCommand(t *testing.T) {
	cmd, _, username, err := userBackupCommand("user-backup-import", map[string]any{"file": "/tmp/user.tar.zst", "targetUsername": "restored", "createUser": true, "execute": true, "domainImportMode": "append"}, nil, "/tmp/user.tar.zst")
	if err != nil {
		t.Fatalf("userBackupCommand() error=%v", err)
	}
	joined := strings.Join(cmd, " ")
	for _, want := range []string{"admin user-backup import", "--file /tmp/user.tar.zst", "--target-username restored", "--create-user", "--new-password", "--execute", "--domain-import-mode append"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("command %q missing %q", joined, want)
		}
	}
	if username != "restored" {
		t.Fatalf("username=%q, want restored", username)
	}
}

func TestDriverRuntimeRepeatsNodeRestartWithOrdinalSequence(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntime(driver, env.Environment{Name: "test", Driver: "test"})
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinalSequence": []any{0, 1, 2}}, Repeat: 5}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	want := []int{0, 1, 2, 0, 1}
	if len(driver.restarted) != len(want) {
		t.Fatalf("restarted=%v, want %v", driver.restarted, want)
	}
	for i := range want {
		if driver.restarted[i] != want[i] {
			t.Fatalf("restarted=%v, want %v", driver.restarted, want)
		}
	}
}

func TestDriverRuntimeExecutesStatefulSetImageUpgrade(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntime(driver, env.Environment{Name: "test", Driver: "test"})
	phase := spec.PhaseSpec{Name: "upgrade", Events: []spec.EventSpec{{Type: "statefulset-image-upgrade", Target: map[string]any{"statefulSet": "myceld", "container": "myceld", "image": "myceldb/mycel:v0.19.3", "rolloutTimeout": "10m"}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	if driver.imageUpdate.Image != "myceldb/mycel:v0.19.3" || driver.imageUpdate.StatefulSet != "myceld" || driver.imageUpdate.Container != "myceld" || driver.imageUpdate.Timeout.String() != "10m0s" {
		t.Fatalf("imageUpdate=%+v", driver.imageUpdate)
	}
}

func TestStatefulSetImageUpgradeRequiresImage(t *testing.T) {
	phase := spec.PhaseSpec{Name: "upgrade", Events: []spec.EventSpec{{Type: "statefulset-image-upgrade", Target: map[string]any{}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want missing image target")
	}
}

func TestDriverRuntimeAwaitsTerminalCommonFolioAutomationFixture(t *testing.T) {
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "automation runs") {
			return env.ExecResult{Stdout: `{"invocations":[{"id":"inv-1","status":"failed","claim_version":2,"updated_at":"2026-10-19T18:00:00Z"}]}`}, nil
		}
		return env.ExecResult{Stdout: `{}`}, nil
	}}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "graph-writers-0", GroupName: "graph-writers", BehaviorType: "graph-transaction", SpaceID: "space-1", DomainID: "11111111-1111-4111-8111-111111111111", PrincipalID: "22222222-2222-4222-8222-222222222222"}}}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "test"}, resources)
	phase := spec.PhaseSpec{Name: "fixture", Events: []spec.EventSpec{{Type: "commonfolio-automation-await-terminal", Target: map[string]any{"limit": 1, "timeout": "50ms", "interval": "1ms"}}}}
	recorder := &recordingEventRecorder{}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	if len(recorder.records) != 2 {
		t.Fatalf("records=%d, want start+complete", len(recorder.records))
	}
	payload, ok := recorder.records[1].Payload["commonfolioTerminalFixture"].(CommonFolioTerminalFixtureResult)
	if !ok || !payload.Ready || len(payload.Assignments) != 1 || len(payload.Assignments[0].TerminalInvocations) != 1 {
		t.Fatalf("terminal fixture payload=%+v", recorder.records[1].Payload)
	}
}

func TestCommonFolioTerminalFixtureTimeoutCancelsBlockedInspection(t *testing.T) {
	driver := &recordingDriver{execWithContext: func(ctx context.Context, _ string) (env.ExecResult, error) {
		<-ctx.Done()
		return env.ExecResult{Stderr: ctx.Err().Error()}, ctx.Err()
	}}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "graph-writers-0", GroupName: "graph-writers", BehaviorType: "graph-transaction", SpaceID: "space-1", DomainID: "11111111-1111-4111-8111-111111111111", PrincipalID: "22222222-2222-4222-8222-222222222222"}}}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "test"}, resources)
	phase := spec.PhaseSpec{Name: "fixture", Events: []spec.EventSpec{{Type: "commonfolio-automation-await-terminal", Target: map[string]any{"limit": 1, "timeout": "10ms", "interval": "1ms"}}}}
	started := time.Now()
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, &recordingEventRecorder{}); err == nil || !strings.Contains(err.Error(), "within 10ms") {
		t.Fatalf("ExecutePhaseEvents() error=%v, want bounded terminal fixture timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("terminal fixture timeout elapsed=%s, want <=1s", elapsed)
	}
}

func TestDriverRuntimeWaitsForAllClusterRaftGroupLeaders(t *testing.T) {
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "cluster readiness check") {
			return env.ExecResult{Stdout: `{"client_ready":true}`}, nil
		}
		if strings.Contains(command, "cluster raft-groups") {
			return env.ExecResult{Stdout: `{"groups":[{"group_id":"system","leader_node_id":1,"health":"healthy"},{"group_id":"space-partition-0","leader_node_id":2,"health":"healthy"}]}`}, nil
		}
		return env.ExecResult{Stdout: `{}`}, nil
	}}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1, Raft: spec.RaftSpec{PartitionCount: 1}}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "test"}, scenario, nil)
	phase := spec.PhaseSpec{Name: "recovery", Events: []spec.EventSpec{{Type: "cluster-recovery-wait", Target: map[string]any{"ordinals": []any{0}, "timeout": "50ms", "interval": "1ms"}}}}
	recorder := &recordingEventRecorder{}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	payload, ok := recorder.records[1].Payload["clusterRecovery"].(ClusterRecoveryWaitResult)
	if !ok || !payload.Ready || payload.ExpectedGroups != 2 || len(payload.Nodes) != 1 || !payload.Nodes[0].RaftGroupsReady {
		t.Fatalf("cluster recovery payload=%+v", recorder.records[1].Payload)
	}
}

func TestClusterRecoveryRequiresExactRaftGroupIdentities(t *testing.T) {
	expected := requiredRaftGroupIDs(1)
	for _, tc := range []struct {
		name   string
		groups []clusterRaftGroup
		check  func(ClusterRecoveryNodeResult) bool
	}{
		{
			name:   "missing system replaced by extra group",
			groups: []clusterRaftGroup{{GroupID: "space-partition-0", LeaderNodeID: 1, Health: "healthy"}, {GroupID: "other", LeaderNodeID: 1, Health: "healthy"}},
			check: func(result ClusterRecoveryNodeResult) bool {
				return len(result.MissingGroups) == 1 && result.MissingGroups[0] == "system" && len(result.UnexpectedGroups) == 1 && result.UnexpectedGroups[0] == "other"
			},
		},
		{
			name:   "missing partition replaced by duplicate system",
			groups: []clusterRaftGroup{{GroupID: "system", LeaderNodeID: 1, Health: "healthy"}, {GroupID: "system", LeaderNodeID: 1, Health: "healthy"}},
			check: func(result ClusterRecoveryNodeResult) bool {
				return len(result.MissingGroups) == 1 && result.MissingGroups[0] == "space-partition-0" && len(result.DuplicateGroups) == 1 && result.DuplicateGroups[0] == "system"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := ClusterRecoveryNodeResult{}
			evaluateClusterRaftGroups(&result, tc.groups, expected, false)
			if result.RaftGroupsReady || result.RaftGroupsError == "" || !tc.check(result) {
				t.Fatalf("result=%+v, want exact identity failure", result)
			}
		})
	}
}

func TestClusterRecoveryRejectsApplyLagWhenZeroLagRequired(t *testing.T) {
	result := ClusterRecoveryNodeResult{}
	evaluateClusterRaftGroups(&result, []clusterRaftGroup{{GroupID: "system", LeaderNodeID: 1, Health: "healthy", CommitIndex: 10, AppliedIndex: 9, LastIndex: 10}}, []string{"system"}, true)
	if result.RaftGroupsReady || len(result.LaggingGroups) != 1 || result.LaggingGroups[0] != "system:1" || !strings.Contains(result.RaftGroupsError, "apply lag") {
		t.Fatalf("result=%+v, want zero-lag rejection", result)
	}
}

func TestClusterRecoveryRequiresConfiguredConsecutiveStablePolls(t *testing.T) {
	raftCalls := 0
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "cluster readiness check") {
			return env.ExecResult{Stdout: `{"client_ready":true}`}, nil
		}
		if strings.Contains(command, "cluster raft-groups") {
			raftCalls++
			return env.ExecResult{Stdout: fmt.Sprintf(`{"groups":[{"group_id":"system","leader_node_id":1,"health":"healthy","commit_index":%d,"applied_index":%d,"last_index":%d}]}`, raftCalls, raftCalls, raftCalls)}, nil
		}
		return env.ExecResult{Stdout: `{}`}, nil
	}}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1, Raft: spec.RaftSpec{PartitionCount: 0}}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "test"}, scenario, nil)
	phase := spec.PhaseSpec{Name: "recovery", Events: []spec.EventSpec{{Type: "cluster-recovery-wait", Target: map[string]any{"ordinals": []any{0}, "timeout": "100ms", "interval": "1ms", "minimumStablePolls": 3, "requireZeroApplyLag": true, "requireConsistentProgress": true}}}}
	recorder := &recordingEventRecorder{}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatal(err)
	}
	payload := recorder.records[1].Payload["clusterRecovery"].(ClusterRecoveryWaitResult)
	if payload.Attempts != 3 || payload.StablePolls != 3 || payload.MinimumStablePolls != 3 || !payload.RequireZeroApplyLag || !payload.RequireConsistentProgress {
		t.Fatalf("payload=%+v, want three consecutive stable polls", payload)
	}
}

func TestClusterRecoveryRejectsRegressingProgress(t *testing.T) {
	result := ClusterRecoveryNodeResult{RaftGroupsReady: true, progress: map[string]clusterRaftProgress{"system": {CommitIndex: 9, AppliedIndex: 9, LastIndex: 9}}}
	enforceClusterProgressConsistency(&result, map[string]clusterRaftProgress{"system": {CommitIndex: 10, AppliedIndex: 10, LastIndex: 10}})
	if result.RaftGroupsReady || len(result.RegressingGroups) != 1 || result.RegressingGroups[0] != "system" || !strings.Contains(result.RaftGroupsError, "regressing") {
		t.Fatalf("result=%+v, want progress regression rejection", result)
	}
}

func TestDriverRuntimeRecordsPartialClusterRecoveryDiagnosticsOnFailure(t *testing.T) {
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "cluster readiness check") {
			return env.ExecResult{Stdout: `{"client_ready":true}`}, nil
		}
		if strings.Contains(command, "cluster raft-groups") {
			return env.ExecResult{Stdout: `{"groups":[{"group_id":"system","leader_node_id":1,"health":"healthy"}]}`}, nil
		}
		return env.ExecResult{Stdout: `{}`}, nil
	}}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1, Raft: spec.RaftSpec{PartitionCount: 1}}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "test"}, scenario, nil)
	phase := spec.PhaseSpec{Name: "recovery", Events: []spec.EventSpec{{Type: "cluster-recovery-wait", Target: map[string]any{"ordinals": []any{0}, "timeout": "1ms", "interval": "1ms"}}}}
	recorder := &recordingEventRecorder{}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want recovery timeout")
	}
	if len(recorder.records) != 2 || recorder.records[1].Type != "event-failed" {
		t.Fatalf("records=%+v, want start+failed", recorder.records)
	}
	payload, ok := recorder.records[1].Payload["clusterRecovery"].(ClusterRecoveryWaitResult)
	if !ok || payload.Ready || len(payload.Nodes) != 1 || payload.Nodes[0].GroupCount != 1 || payload.Nodes[0].RaftGroupsError == "" {
		t.Fatalf("partial cluster recovery payload=%+v", recorder.records[1].Payload)
	}
}

func TestClusterRecoveryTimeoutCancelsBlockedInspection(t *testing.T) {
	driver := &recordingDriver{execWithContext: func(ctx context.Context, _ string) (env.ExecResult, error) {
		<-ctx.Done()
		return env.ExecResult{Stderr: ctx.Err().Error()}, ctx.Err()
	}}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1, Raft: spec.RaftSpec{PartitionCount: 1}}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "test"}, scenario, nil)
	phase := spec.PhaseSpec{Name: "recovery", Events: []spec.EventSpec{{Type: "cluster-recovery-wait", Target: map[string]any{"ordinals": []any{0}, "timeout": "10ms", "interval": "1ms"}}}}
	started := time.Now()
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, &recordingEventRecorder{}); err == nil || !strings.Contains(err.Error(), "within 10ms") {
		t.Fatalf("ExecutePhaseEvents() error=%v, want bounded recovery timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("recovery timeout elapsed=%s, want <=1s", elapsed)
	}
}

func TestDriverRuntimeSeedsCommonFolioAutomation(t *testing.T) {
	driver := &recordingDriver{}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "graph-writers-0", GroupName: "graph-writers", BehaviorType: "graph-transaction", SpaceID: "space-1", DomainID: "11111111-1111-4111-8111-111111111111", PrincipalID: "22222222-2222-4222-8222-222222222222"}}}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "test"}, resources)
	phase := spec.PhaseSpec{Name: "seed", Events: []spec.EventSpec{{Type: "commonfolio-automation-seed", Target: map[string]any{"limit": 1}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, nil); err != nil {
		t.Fatalf("ExecutePhaseEvents() error=%v", err)
	}
	joined := strings.Join(driver.execCommands, "\n")
	for _, want := range []string{"automation procedure put", "automation binding put", "commonfolio.page-summary.graph-writers-0", "11111111-1111-4111-8111-111111111111"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commonfolio seed command missing %q in:\n%s", want, joined)
		}
	}
}

func TestCommonFolioAutomationJSONSupportsVersionedProcedureDefinitions(t *testing.T) {
	assignment := provision.ActorAssignment{ActorID: "graph-writers-0", SpaceID: "space-1", DomainID: "11111111-1111-4111-8111-111111111111", PrincipalID: "22222222-2222-4222-8222-222222222222"}
	procedureJSON, bindingJSON, err := commonFolioAutomationJSON(map[string]any{
		"procedureVersion": 2,
		"procedurePrompt":  "Return the updated CommonFolio page summary.",
	}, assignment)
	if err != nil {
		t.Fatalf("commonFolioAutomationJSON() error=%v", err)
	}
	var procedure map[string]any
	if err := json.Unmarshal([]byte(procedureJSON), &procedure); err != nil {
		t.Fatalf("decode procedure JSON: %v", err)
	}
	if procedure["version"] != float64(2) || procedure["prompt"] != "Return the updated CommonFolio page summary." {
		t.Fatalf("procedure=%v, want version 2 and updated prompt", procedure)
	}
	var binding map[string]any
	if err := json.Unmarshal([]byte(bindingJSON), &binding); err != nil {
		t.Fatalf("decode binding JSON: %v", err)
	}
	if binding["version"] != float64(2) || binding["procedure_version"] != float64(2) {
		t.Fatalf("binding=%v, want version 2 and procedure_version 2", binding)
	}
}

func TestExecuteHostCommandCapturesOutput(t *testing.T) {
	command := []any{"sh", "-c", "printf hello"}
	if runtime.GOOS == "windows" {
		command = []any{"cmd", "/c", "echo hello"}
	}
	result, err := ExecuteHostCommand(context.Background(), map[string]any{"command": command})
	if err != nil {
		t.Fatalf("ExecuteHostCommand() error=%v", err)
	}
	if !strings.Contains(result.Stdout, "hello") {
		t.Fatalf("stdout=%q, want hello", result.Stdout)
	}
}

func TestPodStopRequiresPodTarget(t *testing.T) {
	phase := spec.PhaseSpec{Name: "bad", Events: []spec.EventSpec{{Type: "pod-stop", Target: map[string]any{}}}}
	if err := (LocalRuntime{}).ExecutePhaseEvents(context.Background(), phase, true, nil); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want missing pod target")
	}
}

type recordingEventRecorder struct{ records []Record }

func (r *recordingEventRecorder) RecordRuntimeEvent(_ context.Context, record Record) error {
	r.records = append(r.records, record)
	return nil
}

type recordingDriver struct {
	restarted       []int
	rollingRestarts int
	imageUpdate     env.StatefulSetImageUpdateRequest
	execCommands    []string
	execResult      func(command string) (env.ExecResult, error)
	execWithContext func(context.Context, string) (env.ExecResult, error)
}

func (d *recordingDriver) Name() string { return "test" }
func (d *recordingDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeRestart)
}
func (d *recordingDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *recordingDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *recordingDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{}, nil
}
func (d *recordingDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return nil, nil
}
func (d *recordingDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *recordingDriver) Exec(ctx context.Context, _ env.Environment, _ env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	command := strings.Join(req.Command, " ")
	d.execCommands = append(d.execCommands, command)
	if d.execWithContext != nil {
		return d.execWithContext(ctx, command)
	}
	if d.execResult != nil {
		return d.execResult(command)
	}
	return env.ExecResult{Stdout: "{}\n"}, nil
}
func (d *recordingDriver) RestartNode(_ context.Context, _ env.Environment, node env.NodeRef) error {
	d.restarted = append(d.restarted, node.Ordinal)
	return nil
}
func (d *recordingDriver) RollingRestart(context.Context, env.Environment) error {
	d.rollingRestarts++
	return nil
}
func (d *recordingDriver) UpdateStatefulSetImage(_ context.Context, _ env.Environment, req env.StatefulSetImageUpdateRequest) (env.ExecResult, error) {
	d.imageUpdate = req
	return env.ExecResult{Stdout: "updated\n"}, nil
}
func (d *recordingDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *recordingDriver) Delete(context.Context, env.Environment) error { return nil }
