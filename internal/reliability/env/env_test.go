package env

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestK3DDriverCreateLeavesReadinessToRunner(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner, WaitTimeout: time.Second}
	scenario := spec.ResolvedScenario{Metadata: spec.Metadata{Name: "Example Scenario"}, Environment: spec.EnvironmentSpec{Namespace: "test-ns"}, Cluster: spec.ClusterSpec{Nodes: 1, Image: "myceldb/mycel:latest", Raft: spec.RaftSpec{NodeCount: 1, PartitionCount: 8, ReplicaFactor: 1}}}
	environment, err := driver.Create(context.Background(), scenario, scenario.Environment)
	if err != nil {
		t.Fatalf("Create() error=%v", err)
	}
	if environment.Driver != "k3d" || environment.Namespace != "test-ns" || !strings.HasPrefix(environment.Context, "k3d-mlab-example-scenar-") || len(environment.Name) > k3dClusterNameMaxLength {
		t.Fatalf("environment=%+v", environment)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"k3d cluster create", "docker image inspect myceldb/mycel:latest", "k3d image import myceldb/mycel:latest -c " + environment.Name, "kubectl --context " + environment.Context + " apply -f"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "rollout status") || strings.Contains(joined, "wait pods") {
		t.Fatalf("Create performed readiness checks outside the runner cleanup boundary:\n%s", joined)
	}
	if err := driver.WaitReady(context.Background(), environment); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(runner.commands, "\n")
	for _, want := range []string{"rollout status statefulset/myceld", "wait pods -l app=myceld"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("WaitReady commands missing %q\n%s", want, joined)
		}
	}
}

func TestK3DClusterNameHonorsK3DLengthLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "k3d-cluster-validation", prefix: "mycel-lab"},
		{name: "k3d-system-backup-restore", prefix: "mlab-sbr"},
		{name: "very-long-scenario-name-that-would-overflow", prefix: "very-long-prefix-that-would-overflow"},
	} {
		got := k3dClusterName(tc.name, tc.prefix)
		if len(got) > k3dClusterNameMaxLength {
			t.Fatalf("k3dClusterName(%q, %q)=%q len=%d, want <= %d", tc.name, tc.prefix, got, len(got), k3dClusterNameMaxLength)
		}
		if strings.HasPrefix(got, "-") || strings.HasSuffix(got, "-") || strings.Contains(got, "--") {
			t.Fatalf("k3dClusterName(%q, %q)=%q is not cleanly sanitized", tc.name, tc.prefix, got)
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
	for _, want := range []string{"get all,pvc", "describe pods", "logs statefulset/myceld", "logs -l app=myceld --all-containers=true --prefix=true --tail=5000", "logs -l app=myceld --all-containers=true --prefix=true --previous --tail=5000"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("capture commands missing %q:\n%s", want, joined)
		}
	}
}

func TestK3DDriverWaitPVCsChecksEveryOrdinalPVC(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner, WaitTimeout: time.Second}
	environment := Environment{Name: "mycel-lab-test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-mycel-lab-test"}
	if err := driver.WaitPVCs(context.Background(), environment, "myceld", 3); err != nil {
		t.Fatalf("WaitPVCs() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"get pvc data-myceld-0", "get pvc data-myceld-1", "get pvc data-myceld-2"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("WaitPVCs commands missing %q\n%s", want, joined)
		}
	}
}

func TestK3DDriverScaleStatefulSetWaitsOnlyDesiredOrdinals(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner, WaitTimeout: time.Second}
	environment := Environment{Name: "mycel-lab-test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-mycel-lab-test"}
	if err := driver.ScaleStatefulSet(context.Background(), environment, "myceld", 2); err != nil {
		t.Fatalf("ScaleStatefulSet() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"scale statefulset/myceld --replicas 2", "rollout status statefulset/myceld", "wait pod/myceld-0", "wait pod/myceld-1"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ScaleStatefulSet commands missing %q\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "wait pod/myceld-2") || strings.Contains(joined, "wait pods -l app=myceld") {
		t.Fatalf("ScaleStatefulSet waited on stale/broad pods during downscale:\n%s", joined)
	}
}

func TestK3DDriverUpdateStatefulSetImageWaitsForRollout(t *testing.T) {
	runner := &fakeRunner{}
	driver := K3DDriver{Confirmed: true, Runner: runner, WaitTimeout: time.Second}
	environment := Environment{Name: "mycel-lab-test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-mycel-lab-test"}
	_, err := driver.UpdateStatefulSetImage(context.Background(), environment, StatefulSetImageUpdateRequest{StatefulSet: "myceld", Container: "myceld", Image: "myceldb/mycel:v0.19.3", Timeout: 2 * time.Minute})
	if err != nil {
		t.Fatalf("UpdateStatefulSetImage() error=%v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"set image statefulset/myceld myceld=myceldb/mycel:v0.19.3", "rollout status statefulset/myceld --timeout=2m0s", "wait pods -l app=myceld --for=condition=Ready --timeout=2m0s"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("UpdateStatefulSetImage commands missing %q\n%s", want, joined)
		}
	}
}

type fakeRunner struct{ commands []string }

type k3dListRunner struct{ output string }

type k3dDiagnosticRunner struct {
	podNames  string
	logOutput string
	execError error
}

func (r k3dDiagnosticRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	command := executil.ShellCommand(name, args...)
	if strings.Contains(command, "get pods -l app=myceld") {
		return executil.Result{Command: command, Stdout: r.podNames}, nil
	}
	if strings.Contains(command, " exec ") {
		return executil.Result{Command: command, Stdout: r.logOutput}, r.execError
	}
	return executil.Result{Command: command}, nil
}

func (r k3dListRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	return executil.Result{Command: executil.ShellCommand(name, args...), Stdout: r.output}, nil
}

type deletionRunner struct {
	commands []string
	remain   map[string]string
}

func (r *deletionRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	for kind, output := range r.remain {
		if strings.Contains(cmd, kind) {
			return executil.Result{Command: cmd, Stdout: output}, nil
		}
	}
	return executil.Result{Command: cmd}, nil
}

type composeCaptureRunner struct {
	commands              []string
	imageOutput           string
	imageError            error
	inspectOutput         string
	inspectIdentityOutput string
	diagnosticOutput      string
	diagnosticError       error
}

func (r *composeCaptureRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	if strings.Contains(cmd, " image inspect ") {
		if strings.Contains(cmd, `"repoDigests"`) {
			return executil.Result{Command: cmd, Stdout: r.inspectIdentityOutput}, nil
		}
		return executil.Result{Command: cmd, Stdout: r.inspectOutput}, nil
	}
	if strings.Contains(cmd, " exec -T ") && strings.Contains(cmd, "myceld.log") {
		return executil.Result{Command: cmd, Stdout: r.diagnosticOutput}, r.diagnosticError
	}
	if strings.Contains(cmd, " images --format json ") {
		if r.imageError != nil {
			return executil.Result{Command: cmd, Stderr: r.imageError.Error()}, r.imageError
		}
		if r.imageOutput != "" {
			return executil.Result{Command: cmd, Stdout: r.imageOutput}, nil
		}
		service := args[len(args)-1]
		return executil.Result{Command: cmd, Stdout: `[{"ID":"sha256:` + service + `","ContainerName":"mlab-test-` + service + `-1","Repository":"local/mycel","Tag":"head"}]` + "\n"}, nil
	}
	return executil.Result{Command: cmd, Stdout: "ok\n"}, nil
}

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	return executil.Result{Command: cmd, Stdout: "ok\n"}, nil
}

func TestRunningImageSourceEvidenceBindsProvenanceToEquivalentRuntimeDigest(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	const contextDigest = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	const configDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const manifestDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	runner := &composeCaptureRunner{
		inspectIdentityOutput: `{"id":"` + configDigest + `","repoDigests":["registry.example/mycel@` + manifestDigest + `"]}`,
		inspectOutput:         `{"org.opencontainers.image.revision":"` + revision + `","org.opencontainers.image.source":"https://github.com/MycelDB/mycel","io.myceldb.source.dirty":"false","io.myceldb.build.context-digest":"` + contextDigest + `","io.myceldb.build.command":"docker-buildx-local-linux-arm64"}`,
	}
	for _, runtimeID := range []string{"docker-pullable://registry.example/mycel@" + manifestDigest, "containerd://" + configDigest} {
		evidence := runningImageSourceEvidence(context.Background(), runner, "mutable:configured", runtimeID+"\n", nil)
		if evidence.ID != canonicalImageDigest(runtimeID) || evidence.ConfiguredImageID != configDigest || len(evidence.RunningImageIDs) != 1 || evidence.ImageOverride != "mutable:configured" || !validImageProvenance(evidence.Provenance, "https://github.com/MycelDB/mycel") || !runningImageMatchesConfigured(evidence) {
			t.Fatalf("runtimeID=%q evidence=%+v", runtimeID, evidence)
		}
	}
	if commands := strings.Join(runner.commands, "\n"); !strings.Contains(commands, "docker image inspect "+configDigest) || strings.Count(commands, "image inspect mutable:configured") != 2 {
		t.Fatalf("configured identity was not captured atomically before immutable provenance inspection: %s", commands)
	}
	mismatchDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	mismatch := runningImageSourceEvidence(context.Background(), runner, "mutable:configured", "docker-pullable://registry.example/mycel@"+mismatchDigest+"\n", nil)
	if runningImageMatchesConfigured(mismatch) || mismatch.Provenance.CaptureStatus != "unavailable" {
		t.Fatalf("configured/running mismatch accepted: %+v", mismatch)
	}
	localImport := runningImageSourceEvidence(context.Background(), runner, "mutable:configured", "docker-pullable://registry.example/mycel@"+mismatchDigest+"\n", map[string]string{mismatchDigest: configDigest})
	if !runningImageMatchesConfigured(localImport) || !validImageProvenance(localImport.Provenance, "https://github.com/MycelDB/mycel") {
		t.Fatalf("local runtime manifest was not bound through its config digest: %+v", localImport)
	}
	heterogeneous := runningImageSourceEvidence(context.Background(), runner, "mutable:configured", "docker://"+configDigest+"\ncontainerd://"+manifestDigest+"\n", nil)
	if heterogeneous.ID != "" || len(heterogeneous.RunningImageIDs) != 2 || heterogeneous.Provenance.CaptureStatus != "unavailable" {
		t.Fatalf("heterogeneous evidence accepted: %+v", heterogeneous)
	}
}

func TestK3DRuntimeConfigDigestsResolveManifestToConfig(t *testing.T) {
	const manifestDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const configDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	runner := &runtimeManifestRunner{manifest: `{"config":{"digest":"` + configDigest + `"}}`}
	resolved := k3dRuntimeConfigDigests(context.Background(), runner, Environment{Name: "acceptance"}, "docker-pullable://registry.example/mycel@"+manifestDigest+"\n")
	if resolved[manifestDigest] != configDigest {
		t.Fatalf("resolved=%+v", resolved)
	}
	if !strings.Contains(strings.Join(runner.commands, "\n"), "docker exec k3d-acceptance-server-0 sh -c") || !strings.Contains(strings.Join(runner.commands, "\n"), "content get "+manifestDigest+" | head -c 1048576") {
		t.Fatalf("commands=%v", runner.commands)
	}
}

type runtimeManifestRunner struct {
	manifest string
	commands []string
}

func (r *runtimeManifestRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	r.commands = append(r.commands, cmd)
	return executil.Result{Command: cmd, Stdout: r.manifest}, nil
}

func TestCaptureK3DDaemonDiagnosticsIsClosedBoundedAndFailClosed(t *testing.T) {
	outputKeyHash := strings.Repeat("a", 64)
	raw := strings.Repeat("x", 2*1024*1024) + "\n" + `{"level":"INFO","msg":"procedure_inference_publication_stage","stage":"recovery_run_persistence","result":"failed","category":"unavailable","run_id":"secret-run","attempt_id":"secret-attempt","output_key_hash":"` + outputKeyHash + `","prompt":"secret-prompt","error":"secret-error"}`
	environment := Environment{Context: "k3d-test", Namespace: "default", Metadata: map[string]string{"nodeCount": "2"}}
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := captureK3DDaemonDiagnostics(context.Background(), k3dDiagnosticRunner{podNames: "myceld-0\nmyceld-1\n", logOutput: raw}, environment, sink); err != nil {
		t.Fatal(err)
	}
	for _, pod := range []string{"myceld-0", "myceld-1"} {
		data, err := os.ReadFile(filepath.Join(sink.Root(), "environment", "diagnostics", pod+".json"))
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, required := range []string{"recovery_run_persistence", "unavailable", outputKeyHash} {
			if !strings.Contains(text, required) {
				t.Fatalf("diagnostic missing %q: %s", required, text)
			}
		}
		for _, forbidden := range []string{"secret-run", "secret-attempt", "secret-prompt", "secret-error"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("diagnostic leaked %q: %s", forbidden, text)
			}
		}
	}
	failedSink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := captureK3DDaemonDiagnostics(context.Background(), k3dDiagnosticRunner{podNames: "myceld-0\nmyceld-1\n", execError: errors.New("secret command failure")}, environment, failedSink); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("unavailable capture error=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(failedSink.Root(), "environment", "diagnostics", "myceld-0.json"))
	if err != nil || !strings.Contains(string(data), `"captureStatus": "unavailable"`) || strings.Contains(string(data), "secret command failure") {
		t.Fatalf("unsafe unavailable diagnostic data=%s err=%v", data, err)
	}
}

func TestK3DVerifyDeletedChecksClusterAbsence(t *testing.T) {
	environment := Environment{Name: "target-cluster"}
	verification, err := (K3DDriver{Runner: k3dListRunner{output: `[{"name":"other-cluster"}]`}}).VerifyDeleted(context.Background(), environment)
	if err != nil || !verification.Supported || !verification.Completed || verification.Clusters != 0 {
		t.Fatalf("verification=%+v err=%v", verification, err)
	}
	verification, err = (K3DDriver{Runner: k3dListRunner{output: `[{"name":"target-cluster"}]`}}).VerifyDeleted(context.Background(), environment)
	if err == nil || verification.Completed || verification.Clusters != 1 {
		t.Fatalf("verification=%+v err=%v", verification, err)
	}
}

func TestComposeVerifyDeletedChecksProjectResources(t *testing.T) {
	environment := Environment{Metadata: map[string]string{"projectName": "bounded-project"}}
	t.Run("absent", func(t *testing.T) {
		runner := &deletionRunner{}
		verification, err := (ComposeDriver{Runner: runner}).VerifyDeleted(context.Background(), environment)
		if err != nil || !verification.Supported || !verification.Completed || verification.Containers != 0 || verification.Networks != 0 || verification.Volumes != 0 {
			t.Fatalf("verification=%+v err=%v", verification, err)
		}
		joined := strings.Join(runner.commands, "\n")
		if strings.Count(joined, "label=com.docker.compose.project=bounded-project") != 3 {
			t.Fatalf("verification commands=%s", joined)
		}
	})
	t.Run("remaining", func(t *testing.T) {
		runner := &deletionRunner{remain: map[string]string{" ps ": "container-id\n"}}
		verification, err := (ComposeDriver{Runner: runner}).VerifyDeleted(context.Background(), environment)
		if err == nil || verification.Completed || verification.Containers != 1 {
			t.Fatalf("verification=%+v err=%v", verification, err)
		}
	})
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

func TestComposePreflightRejectsOccupiedGRPCPort(t *testing.T) {
	composeFile := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(composeFile, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	driver := ComposeDriver{Confirmed: true, Runner: &fakeRunner{}}
	err = driver.Preflight(context.Background(), spec.EnvironmentSpec{Driver: "compose", Options: map[string]any{"composeFile": composeFile, "grpcPorts": []any{port}}})
	if err == nil || !strings.Contains(err.Error(), "host port") || !strings.Contains(err.Error(), portText) {
		t.Fatalf("Preflight() error=%v, want occupied port error", err)
	}
}

func TestComposeDriverLifecycleCommands(t *testing.T) {
	composeFile := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(composeFile, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}
	runner := &fakeRunner{}
	driver := ComposeDriver{Confirmed: true, Runner: runner}
	environmentSpec := spec.EnvironmentSpec{Driver: "compose", Options: map[string]any{"composeFile": composeFile, "projectName": "mlab-test", "serviceNames": []any{"myceld-a", "myceld-b"}, "grpcPorts": []any{19091, 19092}, "imageOverride": "local/mycel:head"}}
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
	for _, want := range []string{"docker compose version", "env MYCEL_IMAGE=local/mycel:head docker compose -f " + composeFile + " -p mlab-test down --volumes --remove-orphans", "env MYCEL_IMAGE=local/mycel:head docker compose -f " + composeFile + " -p mlab-test up -d --wait", "env MYCEL_IMAGE=local/mycel:head docker compose -f " + composeFile + " -p mlab-test restart myceld-b"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands missing %q\n%s", want, joined)
		}
	}
}

func TestComposeImageProvenanceIsAllowlisted(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	const contextDigest = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	runner := &composeCaptureRunner{inspectOutput: `{"org.opencontainers.image.revision":"` + revision + `","org.opencontainers.image.source":"https://github.com/MycelDB/mycel","io.myceldb.source.dirty":"false","io.myceldb.build.context-digest":"` + contextDigest + `","io.myceldb.build.command":"docker-buildx-local-linux-arm64","unsafe":"secret"}`}
	provenance := (ComposeDriver{Runner: runner}).imageProvenance(context.Background(), "sha256:test")
	if provenance.CaptureStatus != "completed" || provenance.Revision != revision || provenance.ContextDigest != contextDigest || provenance.Dirty != "false" || provenance.Source != "https://github.com/MycelDB/mycel" || provenance.BuildCommand != "docker-buildx-local-linux-arm64" || !validImageProvenance(provenance, "https://github.com/MycelDB/mycel") {
		t.Fatalf("provenance=%+v", provenance)
	}
	incomplete := provenance
	incomplete.Dirty = "true"
	if validImageProvenance(incomplete, "https://github.com/MycelDB/mycel") {
		t.Fatalf("dirty provenance accepted: %+v", incomplete)
	}
	encoded, err := json.Marshal(provenance)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("unsafe")) || bytes.Contains(encoded, []byte("secret")) {
		t.Fatalf("provenance leaked arbitrary labels: %s", encoded)
	}
}

func TestComposeCaptureStateRequiresResolvedImageEvidence(t *testing.T) {
	environment := Environment{Name: "mlab-test", Driver: "compose", Metadata: map[string]string{"composeFile": "compose.yml", "projectName": "mlab-test", "serviceNames": "myceld-a,myceld-b", "imageOverride": "local/mycel:head"}}
	for _, tc := range []struct {
		name         string
		imageOutput  string
		imageError   error
		wantError    bool
		wantFragment string
	}{
		{name: "complete"},
		{name: "command failure", imageError: errors.New("docker unavailable"), wantError: true, wantFragment: "capture compose image"},
		{name: "malformed JSON", imageOutput: "{not-json", wantError: true, wantFragment: "parse compose image"},
		{name: "empty output", imageOutput: " \n", wantError: true, wantFragment: "parse compose image"},
		{name: "missing record", imageOutput: "[]\n", wantError: true, wantFragment: "image evidence"},
		{name: "empty ID", imageOutput: `[{"ContainerName":"mlab-test-myceld-a-1"}]`, wantError: true, wantFragment: "image evidence"},
		{name: "empty container name", imageOutput: `[{"ID":"sha256:test"}]`, wantError: true, wantFragment: "image evidence"},
		{name: "multiple records", imageOutput: `[{"ID":"sha256:a","ContainerName":"a"},{"ID":"sha256:b","ContainerName":"b"}]`, wantError: true, wantFragment: "exactly one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &composeCaptureRunner{imageOutput: tc.imageOutput, imageError: tc.imageError}
			driver := ComposeDriver{Confirmed: true, Runner: runner}
			sink, err := artifacts.NewSink(t.TempDir())
			if err != nil {
				t.Fatalf("NewSink() error=%v", err)
			}
			err = driver.CaptureState(context.Background(), environment, sink)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), tc.wantFragment) {
					t.Fatalf("CaptureState() error=%v, want fragment %q", err, tc.wantFragment)
				}
				return
			}
			if err != nil {
				t.Fatalf("CaptureState() error=%v", err)
			}
			joinedCommands := strings.Join(runner.commands, "\n")
			for _, service := range []string{"myceld-a", "myceld-b"} {
				for _, want := range []string{"logs --no-color --tail=5000 " + service, "exec -T " + service + " sh -c", "tail -c 1048576"} {
					if !strings.Contains(joinedCommands, want) {
						t.Fatalf("CaptureState commands missing %q\n%s", want, joinedCommands)
					}
				}
			}
			data, err := os.ReadFile(filepath.Join(sink.Root(), "environment", "compose-images.jsonl"))
			if err != nil {
				t.Fatalf("read compose image evidence: %v", err)
			}
			for _, want := range []string{"sha256:myceld-a", "sha256:myceld-b", "local/mycel:head"} {
				if !strings.Contains(string(data), want) {
					t.Fatalf("compose image evidence missing %q: %s", want, data)
				}
			}
		})
	}
}

func TestComposeClosedCaptureSkipsRawLogsAndFailsClosed(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	const contextDigest = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	environment := Environment{Name: "mlab-test", Driver: "compose", Metadata: map[string]string{
		"composeFile":            "compose.yml",
		"projectName":            "mlab-test",
		"serviceNames":           "myceld-a",
		"imageOverride":          "local/mycel:head",
		"requireImageProvenance": "true",
	}}
	outputKeyHash := strings.Repeat("a", 64)
	runner := &composeCaptureRunner{
		inspectOutput:    `{"org.opencontainers.image.revision":"` + revision + `","org.opencontainers.image.source":"https://github.com/MycelDB/mycel","io.myceldb.source.dirty":"false","io.myceldb.build.context-digest":"` + contextDigest + `","io.myceldb.build.command":"docker-buildx-local-linux-arm64"}`,
		diagnosticOutput: `{"level":"INFO","msg":"procedure_inference_publication_stage","stage":"graph_commit","result":"failed","category":"failed_precondition","output_key_hash":"` + outputKeyHash + `","prompt":"secret-prompt","error":"secret-error"}`,
	}
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (ComposeDriver{Confirmed: true, Runner: runner}).CaptureState(context.Background(), environment, sink); err != nil {
		t.Fatal(err)
	}
	commands := strings.Join(runner.commands, "\n")
	if strings.Contains(commands, " logs --no-color") || !strings.Contains(commands, "tail -c 1048576") {
		t.Fatalf("closed capture requested raw or unbounded logs:\n%s", commands)
	}
	diagnostic, err := os.ReadFile(filepath.Join(sink.Root(), "environment", "logs", "myceld-a-procedure-inference.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, prohibited := range []string{"secret-prompt", "secret-error", `"prompt"`, `"error"`} {
		if bytes.Contains(diagnostic, []byte(prohibited)) {
			t.Fatalf("closed diagnostic leaked %q: %s", prohibited, diagnostic)
		}
	}

	failedRunner := &composeCaptureRunner{inspectOutput: runner.inspectOutput, diagnosticError: errors.New("secret diagnostic failure")}
	failedSink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = (ComposeDriver{Confirmed: true, Runner: failedRunner}).CaptureState(context.Background(), environment, failedSink)
	if err == nil || err.Error() != "daemon diagnostic capture is incomplete" {
		t.Fatalf("closed diagnostic failure error=%v", err)
	}
	unavailable, readErr := os.ReadFile(filepath.Join(failedSink.Root(), "environment", "logs", "myceld-a-procedure-inference.json"))
	if readErr != nil || !bytes.Contains(unavailable, []byte(`"captureStatus": "unavailable"`)) || bytes.Contains(unavailable, []byte("secret diagnostic failure")) {
		t.Fatalf("unsafe unavailable evidence=%s err=%v", unavailable, readErr)
	}
}

func TestComposeDeletePreservesGeneratedOverrideUntilSuccessfulRetry(t *testing.T) {
	dir := t.TempDir()
	generated := filepath.Join(dir, "override.yml")
	if err := os.WriteFile(generated, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &composeDeleteRetryRunner{failures: 1}
	driver := ComposeDriver{Runner: runner}
	environment := Environment{Metadata: map[string]string{
		"composeFile":                   "base.yml",
		"composeFiles":                  strings.Join([]string{"base.yml", generated}, string(os.PathListSeparator)),
		"projectName":                   "retry-test",
		"generatedProviderEmulatorFile": generated,
	}}
	if err := driver.Delete(context.Background(), environment); err == nil {
		t.Fatal("first delete unexpectedly succeeded")
	}
	if _, err := os.Stat(generated); err != nil {
		t.Fatalf("generated override removed before successful retry: %v", err)
	}
	if err := driver.Delete(context.Background(), environment); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(generated); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("generated override retained after successful delete: %v", err)
	}
	if runner.calls != 2 {
		t.Fatalf("delete calls=%d", runner.calls)
	}
}

type composeDeleteRetryRunner struct {
	failures int
	calls    int
}

func (r *composeDeleteRetryRunner) Run(_ context.Context, name string, args ...string) (executil.Result, error) {
	cmd := executil.ShellCommand(name, args...)
	if strings.Contains(cmd, " down --volumes --remove-orphans") {
		r.calls++
		if r.failures > 0 {
			r.failures--
			return executil.Result{Command: cmd}, errors.New("temporary delete failure")
		}
	}
	return executil.Result{Command: cmd}, nil
}

func TestBoundedDaemonDiagnosticCommandPropagatesProducerFailure(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "myceld.log")
	if err := os.WriteFile(input, []byte("diagnostic\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeTail := filepath.Join(dir, "tail")
	if err := os.WriteFile(fakeTail, []byte("#!/bin/sh\nif [ \"$1\" = \"-c\" ]; then exit 7; fi\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("sh", "-c", boundedDaemonDiagnosticCommand(input))
	command.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := command.Run(); err == nil {
		t.Fatal("byte-bounding producer failure was masked by the line-bound stage")
	}
}

func TestParseProcedureInferenceDaemonDiagnosticsIsClosedAndSecretFree(t *testing.T) {
	outputKeyHash := strings.Repeat("a", 64)
	raw := strings.Repeat("x", 2*1024*1024) + "\n" + strings.Join([]string{
		`{"time":"2026-10-24T13:40:22.1Z","level":"INFO","msg":"procedure_inference_publication_stage","stage":"graph_commit","result":"failed","category":"failed_precondition","run_id":"run-secret","attempt_id":"attempt-secret","claim_owner_node_id":2,"claim_version":3,"authorization_revision":4,"global_authorization_revision":5,"space_authorization_revision":6,"output_key_hash":"` + outputKeyHash + `","graph_revision":7,"claim_token":"never-persist","prompt":"never-persist","error":"never-persist"}`,
		`{"level":"WARN","msg":"automation_worker_process_pending_failed","category":"operation_failed","error":"never-persist"}`,
		`{"level":"INFO","msg":"procedure_inference_publication_stage","stage":"raw-stage","result":"raw-result","category":"raw-category","run_id":"raw-run","attempt_id":"raw-attempt"}`,
		`{"level":"INFO","msg":"unrelated","secret":"never-persist"}`,
	}, "\n")
	evidence := parseProcedureInferenceDaemonDiagnostics("myceld-a", raw)
	if evidence.CaptureStatus != "completed" || len(evidence.Records) != 3 {
		t.Fatalf("evidence=%+v", evidence)
	}
	first := evidence.Records[0]
	if first.Stage != "graph_commit" || first.Result != "failed" || first.Category != "failed_precondition" || first.ClaimOwnerNodeID != 2 || first.ClaimVersion != 3 || first.AuthorizationRevision != 4 || first.GlobalAuthorizationRevision != 5 || first.SpaceAuthorizationRevision != 6 || first.OutputKeyHash != outputKeyHash || first.GraphRevision != 7 || len(first.RunIDHash) != 64 || len(first.AttemptIDHash) != 64 {
		t.Fatalf("first diagnostic=%+v", first)
	}
	if evidence.Records[2].Stage != "invalid" || evidence.Records[2].Result != "invalid" || evidence.Records[2].Category != "invalid" {
		t.Fatalf("unknown values were not closed: %+v", evidence.Records[2])
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, prohibited := range []string{"never-persist", "run-secret", "attempt-secret", "raw-stage", "raw-result", "raw-category", "raw-run", "raw-attempt", "claim_token", "prompt", "error"} {
		if bytes.Contains(encoded, []byte(prohibited)) {
			t.Fatalf("diagnostic evidence leaked %q: %s", prohibited, encoded)
		}
	}
}

func TestSelectDriverRejectsGenericKubernetesDriver(t *testing.T) {
	_, _, err := SelectDriver(spec.EnvironmentSpec{Driver: "kubernetes"}, DriverSelectionOptions{})
	if err == nil || !strings.Contains(err.Error(), "unsupported environment driver") {
		t.Fatalf("SelectDriver() error=%v, want unsupported environment driver", err)
	}
}
