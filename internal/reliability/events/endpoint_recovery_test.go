package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type orderedEndpointRecovery struct {
	snapshots         []env.EndpointForwardSnapshot
	operationComplete func(string) bool
	refreshNodes      []string
}

func (r *orderedEndpointRecovery) SnapshotEndpoint(nodeName string) (env.EndpointForwardSnapshot, bool) {
	for _, snapshot := range r.snapshots {
		if snapshot.NodeName == nodeName {
			return snapshot, true
		}
	}
	return env.EndpointForwardSnapshot{}, false
}

func (r *orderedEndpointRecovery) SnapshotEndpoints() []env.EndpointForwardSnapshot {
	return append([]env.EndpointForwardSnapshot(nil), r.snapshots...)
}

func (r *orderedEndpointRecovery) RefreshAndAwaitEndpoint(_ context.Context, nodeName string, afterGeneration uint64) (env.EndpointRecoveryEvidence, error) {
	if r.operationComplete != nil && !r.operationComplete(nodeName) {
		return env.EndpointRecoveryEvidence{}, errors.New("endpoint recovery ran before disruptive operation completed")
	}
	snapshot, ok := r.SnapshotEndpoint(nodeName)
	if !ok {
		return env.EndpointRecoveryEvidence{}, errors.New("unknown endpoint")
	}
	r.refreshNodes = append(r.refreshNodes, nodeName)
	return env.EndpointRecoveryEvidence{NodeName: nodeName, DaemonAddr: snapshot.DaemonAddr, PreviousGeneration: afterGeneration, RefreshGeneration: afterGeneration, ReadyGeneration: afterGeneration + 1, Reconnected: true, Status: "transport-ready"}, nil
}

type recordingEndpointRecovery struct {
	driver       *recordingDriver
	snapshotNode string
	refreshNode  string
	refreshNodes []string
	after        uint64
}

func (r *recordingEndpointRecovery) SnapshotEndpoint(nodeName string) (env.EndpointForwardSnapshot, bool) {
	r.snapshotNode = nodeName
	return env.EndpointForwardSnapshot{NodeName: nodeName, DaemonAddr: "127.0.0.1:20091", Generation: 4, Ready: true}, true
}

func (r *recordingEndpointRecovery) SnapshotEndpoints() []env.EndpointForwardSnapshot {
	return []env.EndpointForwardSnapshot{
		{NodeName: "myceld-0", DaemonAddr: "127.0.0.1:20091", Generation: 4, Ready: true},
		{NodeName: "myceld-1", DaemonAddr: "127.0.0.1:20092", Generation: 7, Ready: true},
	}
}

func (r *recordingEndpointRecovery) RefreshAndAwaitEndpoint(_ context.Context, nodeName string, afterGeneration uint64) (env.EndpointRecoveryEvidence, error) {
	if r.snapshotNode != "" && len(r.driver.restarted) != 1 {
		return env.EndpointRecoveryEvidence{}, errors.New("endpoint recovery ran before node restart")
	}
	r.refreshNode = nodeName
	r.refreshNodes = append(r.refreshNodes, nodeName)
	r.after = afterGeneration
	return env.EndpointRecoveryEvidence{NodeName: nodeName, DaemonAddr: "127.0.0.1:20091", PreviousGeneration: afterGeneration, RefreshGeneration: afterGeneration, ReadyGeneration: afterGeneration + 1, Reconnected: true, Status: "transport-ready"}, nil
}

func TestDriverRuntimeRestartsEndpointAndProbesExternalGRPCBeforeCompleting(t *testing.T) {
	driver := &recordingDriver{}
	recovery := &recordingEndpointRecovery{driver: driver}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	probeAddr := ""
	runtime.endpointProbe = func(_ context.Context, _ *provision.ScenarioResources, daemonAddr string) (int, error) {
		probeAddr = daemonAddr
		return 2, nil
	}
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "node-restart", Target: map[string]any{"ordinal": 0}})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := payload["endpointRecovery"].(endpointRecoveryResult)
	if !ok {
		t.Fatalf("endpoint recovery payload=%+v", payload)
	}
	if recovery.snapshotNode != "myceld-0" || recovery.refreshNode != "myceld-0" || recovery.after != 4 || probeAddr != "127.0.0.1:20091" {
		t.Fatalf("recovery=%+v probeAddr=%q", recovery, probeAddr)
	}
	if result.TransportStatus != "transport-ready" || result.GRPCProbeStatus != "completed" || result.PreviousGeneration != 4 || result.RefreshGeneration != 4 || result.ReadyGeneration != 5 || result.ProbeAttempts != 2 || !result.Reconnected {
		t.Fatalf("endpoint recovery result=%+v", result)
	}
}

func TestDriverRuntimeRollingRestartRecoversEveryManagedEndpoint(t *testing.T) {
	driver := &recordingDriver{}
	recovery := &recordingEndpointRecovery{driver: driver}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "rolling-restart"})
	if err != nil {
		t.Fatal(err)
	}
	recoveries, ok := payload["endpointRecoveries"].([]endpointRecoveryResult)
	if !ok || len(recoveries) != 2 || len(recovery.refreshNodes) != 2 || driver.rollingRestarts != 1 {
		t.Fatalf("payload=%+v refreshNodes=%v rollingRestarts=%d", payload, recovery.refreshNodes, driver.rollingRestarts)
	}
	if recovery.refreshNodes[0] != "myceld-0" || recovery.refreshNodes[1] != "myceld-1" {
		t.Fatalf("refreshNodes=%v", recovery.refreshNodes)
	}
}

func TestDriverRuntimeImageUpgradeRecoversEveryManagedEndpoint(t *testing.T) {
	driver := &recordingDriver{}
	recovery := &recordingEndpointRecovery{driver: driver}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "statefulset-image-upgrade", Target: map[string]any{"image": "mycel:test"}})
	if err != nil {
		t.Fatal(err)
	}
	recoveries, ok := payload["endpointRecoveries"].([]endpointRecoveryResult)
	if !ok || len(recoveries) != 2 || len(recovery.refreshNodes) != 2 || driver.imageUpdate.Image != "mycel:test" {
		t.Fatalf("payload=%+v refreshNodes=%v imageUpdate=%+v", payload, recovery.refreshNodes, driver.imageUpdate)
	}
}

func TestDriverRuntimeIdentityReplayRecoversRestartedEndpoint(t *testing.T) {
	driver := &identityReplayDriver{}
	recovery := &orderedEndpointRecovery{
		snapshots: []env.EndpointForwardSnapshot{
			{NodeName: "myceld-0", DaemonAddr: "127.0.0.1:20091", Generation: 1, Ready: true},
			{NodeName: "myceld-1", DaemonAddr: "127.0.0.1:20092", Generation: 1, Ready: true},
			{NodeName: "myceld-2", DaemonAddr: "127.0.0.1:20093", Generation: 1, Ready: true},
		},
		operationComplete: func(node string) bool { return node == "myceld-1" && len(driver.restarted) == 1 },
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "k3d"}, scenario, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "identity-duplicate-principal-replay", Target: map[string]any{"settle": "0s"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.refreshNodes) != 1 || recovery.refreshNodes[0] != "myceld-1" {
		t.Fatalf("refreshNodes=%v payload=%+v", recovery.refreshNodes, payload)
	}
	if _, ok := payload["endpointRecovery"].(endpointRecoveryResult); !ok {
		t.Fatalf("missing endpoint recovery evidence: %+v", payload)
	}
}

func TestDriverRuntimeRaftPVCReplacementRecoversReplacedEndpoint(t *testing.T) {
	driver := &raftRejoinDriver{oldUIDs: map[string]string{"data-myceld-2": "old"}, newUIDs: map[string]string{"data-myceld-2": "new"}}
	recovery := &orderedEndpointRecovery{
		snapshots: []env.EndpointForwardSnapshot{{NodeName: "myceld-2", DaemonAddr: "127.0.0.1:20093", Generation: 2, Ready: true}},
		operationComplete: func(node string) bool {
			return node == "myceld-2" && strings.Contains(strings.Join(driver.ops, "\n"), "wait-pvcs:myceld:3")
		},
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "k3d"}, scenario, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "raft-pvc-replace-node", Target: map[string]any{"statefulSet": "myceld", "ordinal": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.refreshNodes) != 1 || recovery.refreshNodes[0] != "myceld-2" {
		t.Fatalf("refreshNodes=%v payload=%+v", recovery.refreshNodes, payload)
	}
	if _, ok := payload["endpointRecovery"].(endpointRecoveryResult); !ok {
		t.Fatalf("missing endpoint recovery evidence: %+v", payload)
	}
}

func TestDriverRuntimeClusterRestoreRecoversEveryManagedEndpoint(t *testing.T) {
	driver := &restoreApplyDriver{}
	recovery := &orderedEndpointRecovery{
		snapshots: []env.EndpointForwardSnapshot{
			{NodeName: "myceld-0", DaemonAddr: "127.0.0.1:20091", Generation: 3, Ready: true},
			{NodeName: "myceld-1", DaemonAddr: "127.0.0.1:20092", Generation: 3, Ready: true},
		},
		operationComplete: func(string) bool { return strings.Contains(strings.Join(driver.ops, "\n"), "scale:myceld:2") },
	}
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	backup := SystemBackupOperationResult{BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir, Artifacts: []SystemBackupArtifact{
		{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", ManifestName: "node-0.manifest.json", LocalArchive: "/tmp/node-0.tar", LocalManifest: "/tmp/node-0.manifest.json"},
		{PodName: "myceld-1", Ordinal: 1, ArchiveName: "node-1.tar", ManifestName: "node-1.manifest.json", LocalArchive: "/tmp/node-1.tar", LocalManifest: "/tmp/node-1.manifest.json"},
	}}
	scenario := spec.ResolvedScenario{Environment: spec.EnvironmentSpec{Namespace: "test-ns"}, Cluster: spec.ClusterSpec{Nodes: 2, Image: "myceldb/mycel:latest", Raft: spec.RaftSpec{NodeCount: 2, PartitionCount: 8, ReplicaFactor: 2}}}
	runtime := NewDriverRuntimeWithScenario(driver, env.Environment{Name: "test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-test"}, scenario, &provision.ScenarioResources{})
	runtime.systemBackups["default"] = backup
	runtime.SetEndpointRecovery(recovery)
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "cluster-restore-apply", Target: map[string]any{"statefulSet": "myceld"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.refreshNodes) != 2 || recovery.refreshNodes[0] != "myceld-0" || recovery.refreshNodes[1] != "myceld-1" {
		t.Fatalf("refreshNodes=%v payload=%+v", recovery.refreshNodes, payload)
	}
	recoveries, ok := payload["endpointRecoveries"].([]endpointRecoveryResult)
	if !ok || len(recoveries) != 2 {
		t.Fatalf("missing endpoint recovery evidence: %+v", payload)
	}
}

func TestDriverRuntimePersistsClosedEndpointRecoveryEvidence(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(&recordingEndpointRecovery{driver: driver})
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	recorder := &recordingEventRecorder{}
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinal": 0}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatal(err)
	}
	if len(recorder.records) != 2 || recorder.records[1].Type != "event-completed" {
		t.Fatalf("records=%+v", recorder.records)
	}
	encoded, err := json.Marshal(recorder.records[1].Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"nodeName":"myceld-0"`, `"previousGeneration":4`, `"refreshGeneration":4`, `"readyGeneration":5`, `"transportStatus":"transport-ready"`, `"grpcProbeStatus":"completed"`, `"probeAttempts":1`} {
		if !bytes.Contains(encoded, []byte(required)) {
			t.Fatalf("endpoint recovery evidence missing %s: %s", required, encoded)
		}
	}
	for _, prohibited := range []string{"AccessToken", "RefreshToken", "raw probe failure"} {
		if bytes.Contains(encoded, []byte(prohibited)) {
			t.Fatalf("endpoint recovery evidence leaked %q: %s", prohibited, encoded)
		}
	}
}

func TestDriverRuntimeFailsClosedWhenExternalGRPCProbeFails(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(&recordingEndpointRecovery{driver: driver})
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) {
		return 3, errors.New("raw probe failure")
	}
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "node-restart", Target: map[string]any{"ordinal": 0}})
	if err == nil || err.Error() != "external endpoint did not become ready" {
		t.Fatalf("applyEvent() error=%v", err)
	}
	result := payload["endpointRecovery"].(endpointRecoveryResult)
	if result.GRPCProbeStatus != "failed" || result.ProbeAttempts != 3 {
		t.Fatalf("endpoint recovery result=%+v", result)
	}
}
