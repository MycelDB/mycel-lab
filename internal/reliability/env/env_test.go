package env

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestRequireDestructiveConfirmation(t *testing.T) {
	if err := RequireDestructiveConfirmation(Options{}); err == nil {
		t.Fatal("RequireDestructiveConfirmation() error=nil, want error")
	}
	if err := RequireDestructiveConfirmation(Options{ConfirmDestructive: true}); err != nil {
		t.Fatalf("RequireDestructiveConfirmation(confirmed) error=%v", err)
	}
	if err := RequireDestructiveConfirmation(Options{DryRun: true}); err != nil {
		t.Fatalf("RequireDestructiveConfirmation(dry-run) error=%v", err)
	}
}

func TestDryRunDriverLifecycle(t *testing.T) {
	driver := DryRunDriver{}
	environmentSpec := spec.EnvironmentSpec{Namespace: "test-ns"}
	if err := driver.Preflight(context.Background(), environmentSpec); err != nil {
		t.Fatalf("Preflight() error=%v", err)
	}
	environment, err := driver.Create(context.Background(), spec.ResolvedScenario{Metadata: spec.Metadata{Name: "example"}, Cluster: spec.ClusterSpec{Nodes: 2}}, environmentSpec)
	if err != nil {
		t.Fatalf("Create() error=%v", err)
	}
	if environment.Name != "example" || environment.Namespace != "test-ns" {
		t.Fatalf("environment=%+v", environment)
	}
	nodes, err := driver.Nodes(context.Background(), environment)
	if err != nil || len(nodes) != 2 {
		t.Fatalf("Nodes() len=%d err=%v, want 2 nil", len(nodes), err)
	}
	endpoints, err := driver.Endpoints(context.Background(), environment)
	if err != nil || len(endpoints) != 2 {
		t.Fatalf("Endpoints() len=%d err=%v, want 2 nil", len(endpoints), err)
	}
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	if err := driver.CaptureState(context.Background(), environment, sink); err != nil {
		t.Fatalf("CaptureState() error=%v", err)
	}
}

func TestConsoleEndpointsForNodeCount(t *testing.T) {
	endpoints := ConsoleEndpointsForNodeCount(3, 19091)
	if len(endpoints) != 3 {
		t.Fatalf("endpoints len=%d, want 3", len(endpoints))
	}
	checks := []struct {
		index       int
		nodeName    string
		serviceName string
		daemonAddr  string
		localPort   int
	}{
		{0, "myceld-0", "myceld-0-client", "127.0.0.1:19091", 19091},
		{1, "myceld-1", "myceld-1-client", "127.0.0.1:19092", 19092},
		{2, "myceld-2", "myceld-2-client", "127.0.0.1:19093", 19093},
	}
	for _, check := range checks {
		got := endpoints[check.index]
		if got.NodeName != check.nodeName || got.ServiceName != check.serviceName || got.DaemonAddr != check.daemonAddr || got.LocalPort != check.localPort || got.RemotePort != DefaultDaemonGRPCPort {
			t.Fatalf("endpoint[%d]=%+v", check.index, got)
		}
	}
}

func TestK3DPreflightRequiresConfirmation(t *testing.T) {
	err := (K3DDriver{}).Preflight(context.Background(), spec.EnvironmentSpec{})
	if err == nil {
		t.Fatal("Preflight() error=nil, want confirmation error")
	}
	if !strings.Contains(err.Error(), "--confirm-destructive") {
		t.Fatalf("Preflight() error=%v, want confirmation message", err)
	}
}

func TestK3DDriverCreateAppliesManifestsAndWaits(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner, WaitTimeout: time.Second}
	scenario := spec.ResolvedScenario{Metadata: spec.Metadata{Name: "Example Scenario"}, Environment: spec.EnvironmentSpec{Namespace: "test-ns"}, Cluster: spec.ClusterSpec{Nodes: 1, Image: "myceldb/mycel:latest", Raft: spec.RaftSpec{NodeCount: 1, PartitionCount: 8, ReplicaFactor: 1}}}
	environment, err := driver.Create(context.Background(), scenario, scenario.Environment)
	if err != nil {
		t.Fatalf("Create() error=%v", err)
	}
	if environment.Driver != "k3d" || environment.Namespace != "test-ns" || !strings.HasPrefix(environment.Context, "k3d-mlab-example-scenar-") {
		t.Fatalf("environment=%+v", environment)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"k3d cluster create", "docker image inspect myceldb/mycel:latest", "k3d image import myceldb/mycel:latest -c " + environment.Name, "kubectl --context " + environment.Context + " apply -f", "kubectl --context " + environment.Context + " -n test-ns rollout status statefulset/myceld", "kubectl --context " + environment.Context + " -n test-ns wait pods -l app=myceld"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
}

func TestK3DDriverCaptureStateWritesArtifacts(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner}
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	environment := Environment{Name: "mycel-lab-test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-mycel-lab-test"}
	if err := driver.CaptureState(context.Background(), environment, sink); err != nil {
		t.Fatalf("CaptureState() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	if !strings.Contains(joined, "get all,pvc") || !strings.Contains(joined, "describe pods") || !strings.Contains(joined, "logs statefulset/myceld") {
		t.Fatalf("capture commands missing expected kubectl calls:\n%s", joined)
	}
}

type fakeRunner struct{ commands []string }

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	return executil.Result{Command: cmd, Stdout: "ok\n"}, nil
}

func TestSelectDriverValidatesRequiredCapabilities(t *testing.T) {
	_, _, err := SelectDriver(spec.EnvironmentSpec{Driver: "compose", Capabilities: spec.CapabilityRequirements{Required: []string{string(CapabilityVolumeReplacement)}}}, DriverSelectionOptions{ConfirmDestructive: true, Runner: &fakeRunner{}})
	if err == nil || !strings.Contains(err.Error(), "volume-replacement") || !strings.Contains(err.Error(), "compose") {
		t.Fatalf("SelectDriver() error=%v, want missing capability error", err)
	}
}

func TestSelectDriverDryRunOverride(t *testing.T) {
	driver, effective, err := SelectDriver(spec.EnvironmentSpec{Driver: "compose"}, DriverSelectionOptions{DryRun: true})
	if err != nil {
		t.Fatalf("SelectDriver() error=%v", err)
	}
	if driver.Name() != "dry-run" || effective.Driver != "dry-run" {
		t.Fatalf("driver=%s effective=%+v, want dry-run", driver.Name(), effective)
	}
}

func TestComposeDriverLifecycleCommands(t *testing.T) {
	composeFile := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(composeFile, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}
	runner := &fakeRunner{}
	driver := ComposeDriver{Confirmed: true, Runner: runner}
	environmentSpec := spec.EnvironmentSpec{Driver: "compose", Options: map[string]any{"composeFile": composeFile, "projectName": "mlab-test", "serviceNames": []any{"myceld-a", "myceld-b"}, "grpcPorts": []any{19091, 19092}}}
	if err := driver.Preflight(context.Background(), environmentSpec); err != nil {
		t.Fatalf("Preflight() error=%v", err)
	}
	scenario := spec.ResolvedScenario{Metadata: spec.Metadata{Name: "compose smoke"}, Cluster: spec.ClusterSpec{Nodes: 2}}
	environment, err := driver.Create(context.Background(), scenario, environmentSpec)
	if err != nil {
		t.Fatalf("Create() error=%v", err)
	}
	endpoints, err := driver.Endpoints(context.Background(), environment)
	if err != nil || len(endpoints) != 2 || endpoints[1].DaemonAddr != "127.0.0.1:19092" {
		t.Fatalf("Endpoints()=%+v err=%v", endpoints, err)
	}
	if err := driver.RestartNode(context.Background(), environment, NodeRef{Ordinal: 1}); err != nil {
		t.Fatalf("RestartNode() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"docker compose version", "docker compose -f " + composeFile + " -p mlab-test down --volumes --remove-orphans", "docker compose -f " + composeFile + " -p mlab-test up -d --wait", "docker compose -f " + composeFile + " -p mlab-test restart myceld-b"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
}
