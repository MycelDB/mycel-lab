package events

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestIdentityDuplicatePrincipalReplaySnapshotsLeaderThenRestartsIt(t *testing.T) {
	driver := &identityReplayDriver{}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 3}}
	result, err := executeIdentityReplayEvent(context.Background(), driver, env.Environment{}, scenario, map[string]any{"settle": "0s"})
	if err != nil {
		t.Fatalf("executeIdentityReplayEvent() error=%v", err)
	}
	if result.SnapshotOrdinal != 1 || result.DuplicateOrdinal != 1 || result.RestartOrdinal != 1 || result.LeaderNodeID != 2 {
		t.Fatalf("unexpected result: %#v", result)
	}
	joined := strings.Join(driver.ops, "\n")
	for _, want := range []string{"snapshot:0", "snapshot:1", "duplicate:1", "restart:1"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ops missing %q:\n%s", want, joined)
		}
	}
}

func TestIdentityDuplicatePrincipalReplayRequiresDuplicateError(t *testing.T) {
	driver := &identityReplayDriver{duplicateErr: errors.New("rpc error: unavailable")}
	_, err := executeIdentityReplayEvent(context.Background(), driver, env.Environment{}, spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 2}}, map[string]any{"settle": "0s"})
	if err == nil || !strings.Contains(err.Error(), "without expected duplicate-principal") {
		t.Fatalf("executeIdentityReplayEvent() error=%v, want expected duplicate-principal failure", err)
	}
}

type identityReplayDriver struct {
	duplicateErr error
	restarted    []int
	ops          []string
}

func (d *identityReplayDriver) Name() string { return "test" }
func (d *identityReplayDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeExec, env.CapabilityNodeRestart)
}
func (d *identityReplayDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *identityReplayDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *identityReplayDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{}, nil
}
func (d *identityReplayDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *identityReplayDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return nil, nil
}
func (d *identityReplayDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *identityReplayDriver) Exec(_ context.Context, _ env.Environment, node env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	joined := strings.Join(req.Command, " ")
	if strings.Contains(joined, "cluster raft-snapshot create") {
		d.ops = append(d.ops, "snapshot:"+strconv.Itoa(node.Ordinal))
		if node.Ordinal == 0 {
			return executil.Result{Stdout: `{"results":[{"group_id":"system","snapshot_index":5,"compacted":true,"before":{"group_id":"system","local_node_id":1,"leader_node_id":2}}]}`}, nil
		}
		return executil.Result{Stdout: `{"results":[{"group_id":"system","snapshot_index":7,"compacted":true,"before":{"group_id":"system","local_node_id":2,"leader_node_id":2}}]}`}, nil
	}
	if strings.Contains(joined, "user add") {
		d.ops = append(d.ops, "duplicate:"+strconv.Itoa(node.Ordinal))
		if d.duplicateErr != nil {
			return executil.Result{Command: joined, Stderr: d.duplicateErr.Error()}, d.duplicateErr
		}
		return executil.Result{Command: joined, Stderr: "principal already exists"}, errors.New("principal already exists")
	}
	return executil.Result{}, nil
}
func (d *identityReplayDriver) RestartNode(_ context.Context, _ env.Environment, node env.NodeRef) error {
	d.restarted = append(d.restarted, node.Ordinal)
	d.ops = append(d.ops, "restart:"+strconv.Itoa(node.Ordinal))
	return nil
}
func (d *identityReplayDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *identityReplayDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *identityReplayDriver) Delete(context.Context, env.Environment) error { return nil }
