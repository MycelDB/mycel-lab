package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/actors"
	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/metrics"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
)

func TestPhaseTransitionsAreValid(t *testing.T) {
	phase := PhaseState{Name: "warmup", Status: PhasePending}
	if err := Transition(&phase, PhasePassed, phase.StartedAt); err == nil {
		t.Fatal("Transition(pending->passed) error=nil, want invalid transition")
	}
	if err := Transition(&phase, PhaseRunning, phase.StartedAt); err != nil {
		t.Fatalf("Transition(pending->running) error=%v", err)
	}
	if err := Transition(&phase, PhasePassed, phase.StartedAt); err != nil {
		t.Fatalf("Transition(running->passed) error=%v", err)
	}
	if err := Transition(&phase, PhaseFailed, phase.StartedAt); err == nil {
		t.Fatal("Transition(terminal->failed) error=nil, want invalid transition")
	}
}

func TestRunScenarioDryRunWritesArtifactsAndTerminalStatus(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	result, err := RunScenario(context.Background(), scenario, Options{DryRun: true, ArtifactRoot: t.TempDir(), Store: store.NewMemoryStore()})
	if err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	if result.Status != RunPassed {
		t.Fatalf("status=%s, want passed", result.Status)
	}
	for _, name := range []string{"resolved-scenario.json", "events.jsonl", "result.json", "summary.md", filepath.Join("manifests", "myceld.yaml"), filepath.Join("environment", "state.json")} {
		if _, err := os.Stat(filepath.Join(result.ArtifactRoot, name)); err != nil {
			t.Fatalf("artifact %s missing: %v", name, err)
		}
	}
}

func TestRunScenarioGraphMetricsAggregate(t *testing.T) {
	scenario := resolveFixtureScenario(t, "graph-actor-smoke.yaml")
	st := store.NewMemoryStore()
	result, err := RunScenario(context.Background(), scenario, Options{ArtifactRoot: t.TempDir(), Store: st, ConfirmDestructive: true, EnvironmentDriver: env.DryRunDriver{}})
	if err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	details, err := st.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun() error=%v", err)
	}
	summary := metrics.FromRunDetails(details)
	if summary.CommitsPerSecond <= 0 {
		t.Fatalf("summary has no commit rate: %+v", summary)
	}
	if details.Run.Status != string(RunPassed) || details.Run.FinishedAt == nil {
		t.Fatalf("run did not finish cleanly in store: %+v", details.Run)
	}
}

func TestRunSuiteFileExecutesSequentialScenarios(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "suites", "raft-baseline.yaml")
	result, err := RunSuiteFile(context.Background(), path, Options{DryRun: true, ArtifactRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("RunSuiteFile() error=%v", err)
	}
	if result.SuiteName != "raft-baseline" || len(result.Results) != 1 {
		t.Fatalf("suite result=%+v", result)
	}
	if result.Results[0].Status != RunPassed {
		t.Fatalf("scenario status=%s", result.Results[0].Status)
	}
}

func TestCleanupInvokedOnFailure(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	driver := &recordingDriver{}
	_, err := RunScenario(context.Background(), scenario, Options{DryRun: true, ArtifactRoot: t.TempDir(), EnvironmentDriver: driver, ActorFactory: func(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec) actors.Actor {
		return failingActor{}
	}})
	if err == nil {
		t.Fatal("RunScenario() error=nil, want actor failure")
	}
	if !driver.deleted {
		t.Fatal("environment cleanup was not invoked")
	}
}

func TestFinalClusterAssertionsPassForHealthySharedIdentity(t *testing.T) {
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 2}, Assertions: map[string]any{"final": map[string]any{"requireHealthyCluster": true}}}
	nodes := []env.Node{{Name: "myceld-0", Ordinal: 0}, {Name: "myceld-1", Ordinal: 1}}
	if err := runFinalClusterAssertions(context.Background(), sink, scenario, &clusterAssertionDriver{}, env.Environment{Name: "test", Driver: "test"}, nodes, true); err != nil {
		t.Fatalf("runFinalClusterAssertions() error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(sink.Root(), "oracle", "cluster-report.json")); err != nil {
		t.Fatalf("cluster report missing: %v", err)
	}
}

func TestFinalClusterAssertionsWaitForHealthConvergence(t *testing.T) {
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 2}, Assertions: map[string]any{"final": map[string]any{"requireHealthyCluster": true, "healthConvergenceTimeout": "100ms", "healthConvergenceInterval": "1ms"}}}
	nodes := []env.Node{{Name: "myceld-0", Ordinal: 0}, {Name: "myceld-1", Ordinal: 1}}
	driver := &clusterAssertionDriver{healthStatuses: []string{"unhealthy", "unhealthy", "healthy", "healthy"}}
	if err := runFinalClusterAssertions(context.Background(), sink, scenario, driver, env.Environment{Name: "test", Driver: "test"}, nodes, true); err != nil {
		t.Fatalf("runFinalClusterAssertions() error=%v", err)
	}
	if driver.healthCalls < 4 {
		t.Fatalf("health calls=%d, want at least 4", driver.healthCalls)
	}
	raw, err := os.ReadFile(filepath.Join(sink.Root(), "oracle", "cluster-report.json"))
	if err != nil {
		t.Fatalf("read cluster report: %v", err)
	}
	var report clusterAssertionReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("unmarshal cluster report: %v", err)
	}
	if !report.OK || report.Attempts < 2 {
		t.Fatalf("report=%+v, want convergence after retries", report)
	}
}

func TestFinalClusterAssertionsFailOnClusterIDMismatch(t *testing.T) {
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	scenario := spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 2}, Assertions: map[string]any{"final": map[string]any{"requireSharedClusterIdentity": true}}}
	nodes := []env.Node{{Name: "myceld-0", Ordinal: 0}, {Name: "myceld-1", Ordinal: 1}}
	driver := &clusterAssertionDriver{clusterIDs: []string{"cluster-a", "cluster-b"}}
	if err := runFinalClusterAssertions(context.Background(), sink, scenario, driver, env.Environment{Name: "test", Driver: "test"}, nodes, true); err == nil {
		t.Fatal("runFinalClusterAssertions() error=nil, want mismatch error")
	}
}

func resolveFixtureScenario(t *testing.T, name string) spec.ResolvedScenario {
	t.Helper()
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", name)
	scenario, err := catalog.ResolveScenarioFile(path, catalog.ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error=%v", err)
	}
	return scenario
}

type failingActor struct{}

func (failingActor) Start(context.Context) error                     { return errors.New("boom") }
func (failingActor) UpdateRate(context.Context, spec.RateSpec) error { return nil }
func (failingActor) Stop(context.Context) error                      { return nil }

type clusterAssertionDriver struct {
	recordingDriver
	clusterIDs     []string
	healthStatuses []string
	healthCalls    int
}

func (d *clusterAssertionDriver) Exec(_ context.Context, _ env.Environment, node env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	clusterID := "cluster-a"
	if node.Ordinal >= 0 && node.Ordinal < len(d.clusterIDs) && d.clusterIDs[node.Ordinal] != "" {
		clusterID = d.clusterIDs[node.Ordinal]
	}
	for _, part := range req.Command {
		if part == "status" {
			return env.ExecResult{Stdout: `{"cluster":{"cluster_id":"` + clusterID + `","cluster_name":"test","mode":"clustered"},"node":{"node_id":"node","state":"clustered","admitted":true},"peers":[{},{}]}`}, nil
		}
		if part == "health" {
			status := "healthy"
			if d.healthCalls < len(d.healthStatuses) && d.healthStatuses[d.healthCalls] != "" {
				status = d.healthStatuses[d.healthCalls]
			}
			d.healthCalls++
			warnings := `[]`
			if status != "healthy" {
				warnings = `["raft groups without leaders: space-partition-4"]`
			}
			return env.ExecResult{Stdout: `{"status":"` + status + `","active_members":2,"pending_members":0,"unreachable_peers":0,"warnings":` + warnings + `}`}, nil
		}
	}
	return env.ExecResult{}, nil
}

type recordingDriver struct{ deleted bool }

func (d *recordingDriver) Name() string { return "test" }
func (d *recordingDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityArtifacts, env.CapabilityPerNodeEndpoints)
}
func (d *recordingDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *recordingDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *recordingDriver) Create(_ context.Context, scenario spec.ResolvedScenario, _ spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: scenario.Metadata.Name, Driver: "test", Namespace: "test", Metadata: map[string]string{"nodeCount": "1"}}, nil
}
func (d *recordingDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0, Resource: "test/myceld-0"}}, nil
}
func (d *recordingDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return []env.Endpoint{{NodeName: "myceld-0", DaemonAddr: "127.0.0.1:19091", LocalAddress: "127.0.0.1", LocalPort: 19091, RemotePort: 9091}}, nil
}
func (d *recordingDriver) Exec(context.Context, env.Environment, env.NodeRef, env.ExecRequest) (env.ExecResult, error) {
	return env.ExecResult{}, nil
}
func (d *recordingDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *recordingDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) Delete(context.Context, env.Environment) error {
	d.deleted = true
	return nil
}
func (d *recordingDriver) CaptureState(_ context.Context, environment env.Environment, sink *artifacts.Sink) error {
	_, err := sink.WriteJSON("environment/state.json", environment)
	return err
}
