package events

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestRaftSnapshotCreateFailsOnZeroSnapshotIndex(t *testing.T) {
	driver := &raftRejoinDriver{execStdout: `{"results":[{"group_id":"system","snapshot_index":0}]}`}
	_, err := executeRaftSnapshotCreate(context.Background(), driver, env.Environment{}, map[string]any{"ordinals": []any{0}})
	if err == nil || !strings.Contains(err.Error(), "zero snapshot index") {
		t.Fatalf("executeRaftSnapshotCreate() error=%v, want zero snapshot index", err)
	}
}

func TestRaftPVCReplaceNodeReplacesHighestOrdinalPVC(t *testing.T) {
	driver := &raftRejoinDriver{oldUIDs: map[string]string{"data-myceld-0": "a", "data-myceld-1": "b", "data-myceld-2": "old"}, newUIDs: map[string]string{"data-myceld-0": "a", "data-myceld-1": "b", "data-myceld-2": "new"}}
	result, err := executeRaftPVCReplaceNode(context.Background(), driver, env.Environment{}, spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3}}, map[string]any{"statefulSet": "myceld", "ordinal": 2})
	if err != nil {
		t.Fatalf("executeRaftPVCReplaceNode() error=%v", err)
	}
	if result.PVCName != "data-myceld-2" || result.OldPVCUID != "old" || result.NewPVCUID != "new" {
		t.Fatalf("unexpected result: %#v", result)
	}
	joined := strings.Join(driver.ops, "\n")
	for _, want := range []string{"pvcuids:myceld:3", "scale:myceld:2", "delete-pvc:data-myceld-2", "scale:myceld:3", "wait-pvcs:myceld:3"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ops missing %q\n%s", want, joined)
		}
	}
}

func TestRaftSnapshotVerifyRejoinedRequiresNonzeroSnapshotIndexes(t *testing.T) {
	driver := &raftRejoinDriver{execStdout: `{"groups":[{"group_id":"system","kind":"system","health":"healthy","snapshot_index":1},{"group_id":"space-partition-0","kind":"partition","health":"healthy","snapshot_index":0}]}`}
	result, err := executeRaftSnapshotVerifyRejoined(context.Background(), driver, env.Environment{}, spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3}}, map[string]any{"ordinal": 2, "verifyTimeout": "0s"})
	if err == nil || !strings.Contains(err.Error(), "zero snapshot_index") {
		t.Fatalf("executeRaftSnapshotVerifyRejoined() error=%v, want zero snapshot_index", err)
	}
	if result.RejoinedNode == nil || len(result.RejoinedNode.Groups) != 2 {
		t.Fatalf("expected parsed groups in result: %#v", result)
	}
}

type raftRejoinDriver struct {
	execStdout string
	oldUIDs    map[string]string
	newUIDs    map[string]string
	pvcCalls   int
	ops        []string
}

func (d *raftRejoinDriver) Name() string { return "test" }
func (d *raftRejoinDriver) Capabilities() env.CapabilitySet {
	return env.CapabilitySet{env.CapabilityVolumeReplacement: {}}
}
func (d *raftRejoinDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *raftRejoinDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *raftRejoinDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{}, nil
}
func (d *raftRejoinDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *raftRejoinDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return nil, nil
}
func (d *raftRejoinDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *raftRejoinDriver) Exec(context.Context, env.Environment, env.NodeRef, env.ExecRequest) (env.ExecResult, error) {
	return env.ExecResult{Stdout: d.execStdout}, nil
}
func (d *raftRejoinDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *raftRejoinDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *raftRejoinDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *raftRejoinDriver) Delete(context.Context, env.Environment) error { return nil }
func (d *raftRejoinDriver) PVCUIDs(_ context.Context, _ env.Environment, statefulSet string, count int) (map[string]string, error) {
	d.ops = append(d.ops, "pvcuids:"+statefulSet+":"+strconv.Itoa(count))
	d.pvcCalls++
	if d.pvcCalls == 1 {
		return d.oldUIDs, nil
	}
	return d.newUIDs, nil
}
func (d *raftRejoinDriver) WaitPVCs(_ context.Context, _ env.Environment, statefulSet string, count int) error {
	d.ops = append(d.ops, "wait-pvcs:"+statefulSet+":"+strconv.Itoa(count))
	return nil
}
func (d *raftRejoinDriver) ResetNamespace(context.Context, env.Environment) error { return nil }
func (d *raftRejoinDriver) ApplyYAML(context.Context, env.Environment, string) (env.ExecResult, error) {
	return env.ExecResult{}, nil
}
func (d *raftRejoinDriver) ScaleStatefulSet(_ context.Context, _ env.Environment, statefulSet string, replicas int) error {
	d.ops = append(d.ops, "scale:"+statefulSet+":"+strconv.Itoa(replicas))
	return nil
}
func (d *raftRejoinDriver) DeletePVC(_ context.Context, _ env.Environment, pvcName string) error {
	d.ops = append(d.ops, "delete-pvc:"+pvcName)
	return nil
}
func (d *raftRejoinDriver) RestoreArchiveToPVC(context.Context, env.Environment, string, string) error {
	return nil
}
