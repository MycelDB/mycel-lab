package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/actors"
	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/events"
	"github.com/MycelDB/mycel-lab/internal/reliability/metrics"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
)

type capturingRunStore struct {
	*store.MemoryStore
	run    store.Run
	phases []store.RunPhase
}

func (s *capturingRunStore) CreateRun(ctx context.Context, run store.Run, actors []store.RunActorProfile, phases []store.RunPhase) error {
	s.run = run
	s.phases = append([]store.RunPhase(nil), phases...)
	return s.MemoryStore.CreateRun(ctx, run, actors, phases)
}

func TestStoreSnapshotsRemoveProcedureGraphContent(t *testing.T) {
	scenario := resolveFixtureScenario(t, "graph-actor-smoke.yaml")
	scenario.Phases[0].Events = []spec.EventSpec{{Type: "procedure-inference-node-create", Target: map[string]any{"nodeId": "private-node", "text": "private graph text", "timeout": "1s"}}}
	st := &capturingRunStore{MemoryStore: store.NewMemoryStore()}
	if _, err := RunScenario(context.Background(), scenario, Options{DryRun: true, ArtifactRoot: t.TempDir(), Store: st, EnvironmentDriver: env.DryRunDriver{}}); err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	for name, raw := range map[string][]byte{"run": st.run.ResolvedJSON, "phase": st.phases[0].SpecSnapshotJSON} {
		if bytes.Contains(raw, []byte("private-node")) || bytes.Contains(raw, []byte("private graph text")) {
			t.Fatalf("%s store snapshot retained graph content: %s", name, raw)
		}
	}
}

func TestEvidenceSafeResolvedScenarioRemovesProcedureGraphContent(t *testing.T) {
	scenario := spec.ResolvedScenario{Phases: []spec.PhaseSpec{{Events: []spec.EventSpec{
		{Type: "procedure-inference-node-create", Target: map[string]any{"nodeId": "private-id", "text": "private text", "timeout": "1s"}},
		{Type: "system-backup-fixture", Target: map[string]any{"blobPayload": "private payload", "ordinal": 0}},
	}}}}
	safe := evidenceSafeResolvedScenario(scenario)
	for _, event := range safe.Phases[0].Events {
		for _, field := range []string{"nodeId", "text", "blobPayload"} {
			if _, exists := event.Target[field]; exists {
				t.Fatalf("%s retained in %s evidence", field, event.Type)
			}
		}
		if event.Target["graphContentRedacted"] != true {
			t.Fatalf("redaction marker missing for %s", event.Type)
		}
	}
	if scenario.Phases[0].Events[0].Target["text"] != "private text" {
		t.Fatal("runtime scenario was mutated")
	}
}

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

func TestMarkLastPassedPhaseFailedKeepsClosedBudgetEvidenceConsistent(t *testing.T) {
	phases := []PhaseState{{Name: "warmup", Status: PhasePassed}, {Name: "steady", Status: PhasePassed}}
	markLastPassedPhaseFailed(phases)
	if phases[0].Status != PhasePassed || phases[1].Status != PhaseFailed {
		t.Fatalf("phase states = %#v", phases)
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
	for _, name := range []string{"harness-build.json", "resolved-scenario.json", "events.jsonl", "result.json", "summary.md", filepath.Join("manifests", "myceld.yaml"), filepath.Join("environment", "state.json")} {
		if _, err := os.Stat(filepath.Join(result.ArtifactRoot, name)); err != nil {
			t.Fatalf("artifact %s missing: %v", name, err)
		}
	}
}

func TestRunScenarioProviderBudgetExhaustionStopsScheduling(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := provider.Resolve(profile, source, provider.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resolved.Profile.Budgets.MaxOperations = 2 // provisioning plus the first event
	scenario.ProviderProfile = &resolved
	scenario.ActorGroups = nil
	scenario.Phases = []spec.PhaseSpec{{Name: "budget-exhaustion", Events: []spec.EventSpec{{Type: "rolling-restart"}, {Type: "rolling-restart"}}}}

	result, err := RunScenario(context.Background(), scenario, Options{DryRun: true, ArtifactRoot: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), provider.ErrBudgetExhausted.Error()) {
		t.Fatalf("RunScenario() result=%#v error=%v", result, err)
	}
	if result.Status != RunFailed || result.ProviderBudget == nil || !result.ProviderBudget.Closed || result.ProviderBudget.Operations != 2 {
		t.Fatalf("unexpected budget result: %#v", result)
	}
	if _, err := os.Stat(filepath.Join(result.ArtifactRoot, "provider", "budget-final.json")); err != nil {
		t.Fatalf("budget artifact missing: %v", err)
	}
}

func TestReserveProviderOperationPreservesEarlierParentDeadline(t *testing.T) {
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := provider.Resolve(profile, source, provider.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := provider.NewLedger(resolved.Profile.Budgets)
	if err != nil {
		t.Fatal(err)
	}
	parent, cancelParent := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelParent()
	operationCtx, release, err := reserveProviderOperation(parent, ledger)
	if err != nil {
		t.Fatal(err)
	}
	<-operationCtx.Done()
	if err := release(); err != nil {
		t.Fatalf("parent deadline was misclassified: %v", err)
	}
	if snapshot := ledger.Snapshot(); snapshot.Closed {
		t.Fatalf("parent deadline closed provider ledger: %#v", snapshot)
	}
}

func TestReserveProviderOperationReleaseLinearizesBeforeDeadline(t *testing.T) {
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := provider.Resolve(profile, source, provider.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resolved.Profile.Budgets.RequestTimeout = provider.Duration{Duration: 5 * time.Millisecond}
	ledger, err := provider.NewLedger(resolved.Profile.Budgets)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := reserveProviderOperation(context.Background(), ledger)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatalf("completed operation release error: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if snapshot := ledger.Snapshot(); snapshot.Closed || snapshot.ConcurrentOperations != 0 {
		t.Fatalf("completed operation later timed out: %#v", snapshot)
	}
}

type swallowingTimeoutRuntime struct{}

func (swallowingTimeoutRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, _ bool, recorder events.Recorder) error {
	limiter, ok := recorder.(events.OperationBudgetRecorder)
	if !ok {
		return errors.New("operation budget recorder is missing")
	}
	operationCtx, release, err := limiter.ReserveRuntimeOperation(ctx, phase.Name, "swallowed-timeout")
	if err != nil {
		return err
	}
	<-operationCtx.Done()
	release()
	return nil
}

func TestRunScenarioRequestTimeoutCannotProducePassedRun(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := provider.Resolve(profile, source, provider.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resolved.Profile.Budgets.RequestTimeout = provider.Duration{Duration: time.Millisecond}
	scenario.ProviderProfile = &resolved
	scenario.ActorGroups = nil
	scenario.Phases = []spec.PhaseSpec{{Name: "request-timeout", Events: []spec.EventSpec{{Type: "rolling-restart"}}}}

	result, err := RunScenario(context.Background(), scenario, Options{
		DryRun:       true,
		ArtifactRoot: t.TempDir(),
		EventRuntime: swallowingTimeoutRuntime{},
	})
	if err == nil || !strings.Contains(err.Error(), provider.ErrBudgetExhausted.Error()) {
		t.Fatalf("RunScenario() result=%#v error=%v", result, err)
	}
	if result.Status != RunFailed || result.ProviderBudget == nil || !result.ProviderBudget.Closed || result.ProviderBudget.StopReason != "operation deadline reached" {
		t.Fatalf("unexpected request-timeout result: %#v", result)
	}
	if len(result.Phases) != 1 || result.Phases[0].Status != PhaseFailed {
		t.Fatalf("request-timeout phase did not fail immediately: %#v", result.Phases)
	}
	if _, err := os.Stat(filepath.Join(result.ArtifactRoot, "provider", "budget-final.json")); err != nil {
		t.Fatalf("budget artifact missing: %v", err)
	}
}

func TestRunScenarioImageOverrideIsRecordedInResolvedArtifacts(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	originalImage := scenario.Cluster.Image
	const candidateImage = "myceldb/mycel:candidate"
	result, err := RunScenario(context.Background(), scenario, Options{DryRun: true, ArtifactRoot: t.TempDir(), ImageOverride: candidateImage})
	if err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(result.ArtifactRoot, "resolved-scenario.json"))
	if err != nil {
		t.Fatalf("read resolved scenario: %v", err)
	}
	var resolved spec.ResolvedScenario
	if err := json.Unmarshal(data, &resolved); err != nil {
		t.Fatalf("decode resolved scenario: %v", err)
	}
	if resolved.Cluster.Image != candidateImage {
		t.Fatalf("resolved image=%q, want %q", resolved.Cluster.Image, candidateImage)
	}
	if _, leaked := resolved.Environment.Options["imageOverride"]; leaked {
		t.Fatalf("dry-run/non-Compose environment received Compose-only imageOverride: %+v", resolved.Environment.Options)
	}
	if scenario.Cluster.Image != originalImage {
		t.Fatalf("input scenario image mutated to %q", scenario.Cluster.Image)
	}
}

func TestRunScenarioRoutesImageOverrideThroughK3DClusterImageOnly(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	scenario.Environment.Driver = "k3d"
	scenario.Environment.Options = map[string]any{"clusterNamePrefix": "mlab"}
	scenario.ActorGroups = nil
	scenario.Phases = nil
	scenario.Assertions = nil
	driver := &recordingDriver{omitEndpoints: true}
	const image = "local/mycel:exact-head"
	if _, err := RunScenario(context.Background(), scenario, Options{ArtifactRoot: t.TempDir(), ConfirmDestructive: true, ImageOverride: image, EnvironmentDriver: driver}); err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	if driver.createdScenario.Cluster.Image != image {
		t.Fatalf("K3D scenario image=%q, want %q", driver.createdScenario.Cluster.Image, image)
	}
	if _, leaked := driver.createdScenario.Environment.Options["imageOverride"]; leaked {
		t.Fatalf("K3D scenario received Compose-only imageOverride: %+v", driver.createdScenario.Environment.Options)
	}
}

func TestRunScenarioRoutesImageOverrideThroughComposeCreate(t *testing.T) {
	scenario := resolveFixtureScenario(t, "example.yaml")
	scenario.Environment.Driver = "compose"
	scenario.Environment.Options = map[string]any{"composeFile": "compose.yml"}
	scenario.ActorGroups = nil
	scenario.Phases = nil
	scenario.Assertions = nil
	driver := &recordingDriver{omitEndpoints: true}
	const image = "local/mycel:exact-head"
	if _, err := RunScenario(context.Background(), scenario, Options{ArtifactRoot: t.TempDir(), ConfirmDestructive: true, ImageOverride: image, EnvironmentDriver: driver}); err != nil {
		t.Fatalf("RunScenario() error=%v", err)
	}
	if got := driver.createdScenario.Environment.Options["imageOverride"]; got != image {
		t.Fatalf("Compose scenario imageOverride=%v, want %q", got, image)
	}
	if got := driver.preflightEnvironmentSpec.Options["imageOverride"]; got != image {
		t.Fatalf("Compose Preflight environment imageOverride=%v, want %q", got, image)
	}
	if got := driver.createdEnvironmentSpec.Options["imageOverride"]; got != image {
		t.Fatalf("Compose Create environment imageOverride=%v, want %q", got, image)
	}
}

func TestApplyComposeImageOverrideDoesNotLeakIntoK3D(t *testing.T) {
	compose := spec.EnvironmentSpec{Driver: "compose", Options: map[string]any{"composeFile": "compose.yml"}}
	applyComposeImageOverride(&compose, " local/mycel:head ")
	if got := compose.Options["imageOverride"]; got != "local/mycel:head" {
		t.Fatalf("Compose imageOverride=%v", got)
	}
	k3d := spec.EnvironmentSpec{Driver: "k3d", Options: map[string]any{"clusterNamePrefix": "mlab"}}
	applyComposeImageOverride(&k3d, "local/mycel:head")
	if _, exists := k3d.Options["imageOverride"]; exists {
		t.Fatalf("k3d received Compose-only imageOverride: %+v", k3d.Options)
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
	if driver.captured < 2 {
		t.Fatalf("capture count=%d, want initial and failure captures", driver.captured)
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

func TestFinalOracleAssertionsEnforceNodeAndEdgeCounts(t *testing.T) {
	resources := provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", BehaviorType: "graph-transaction", SpaceID: "space", DomainID: "domain", DaemonAddr: "daemon"}}}
	options := finalOracleAssertionOptions{RequireConvergedCounts: true, RequireNoAcknowledgedGraphTransactionLoss: true}
	for _, tc := range []struct {
		name   string
		actual graphCounts
		ok     bool
	}{
		{name: "converged", actual: graphCounts{Nodes: 2, Edges: 1}, ok: true},
		{name: "node loss", actual: graphCounts{Nodes: 1, Edges: 1}},
		{name: "edge loss", actual: graphCounts{Nodes: 2, Edges: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := finalOracleReport{RequiredAssertions: options, LiveChecks: []finalOracleLiveCheck{{ActorID: "writer-0", Expected: graphCounts{Nodes: 2, Edges: 1}, Actual: tc.actual, OK: true}}}
			evaluateFinalOracleAssertions(&report, resources, true)
			if report.AssertionsOK != tc.ok {
				t.Fatalf("AssertionsOK=%t errors=%v, want %t", report.AssertionsOK, report.AssertionErrors, tc.ok)
			}
		})
	}
}

func TestFinalOracleAssertionsFailWithoutGraphChecks(t *testing.T) {
	report := finalOracleReport{RequiredAssertions: finalOracleAssertionOptions{RequireConvergedCounts: true}}
	evaluateFinalOracleAssertions(&report, provision.ScenarioResources{}, true)
	if report.AssertionsOK || len(report.AssertionErrors) == 0 {
		t.Fatalf("report=%+v, want missing-check failure", report)
	}
}

func TestFinalOracleAssertionsSkipDryRunLiveChecks(t *testing.T) {
	report := finalOracleReport{RequiredAssertions: finalOracleAssertionOptions{RequireConvergedCounts: true}}
	evaluateFinalOracleAssertions(&report, provision.ScenarioResources{}, false)
	if !report.AssertionsOK || !report.AssertionsSkipped {
		t.Fatalf("report=%+v, want skipped successful dry-run assertions", report)
	}
}

func TestPhaseOracleProgressRequiresAcknowledgedWriteAndSuccessfulRead(t *testing.T) {
	assertions := map[string]any{"requireWorkloadProgress": true}
	before := phaseOracleSnapshot{AcknowledgedTransactions: 4, SuccessfulReads: 7}
	if err := assertPhaseOracleProgress(assertions, before, phaseOracleSnapshot{AcknowledgedTransactions: 5, SuccessfulReads: 8}, true); err != nil {
		t.Fatalf("assertPhaseOracleProgress() error=%v", err)
	}
	for _, after := range []phaseOracleSnapshot{{AcknowledgedTransactions: 4, SuccessfulReads: 8}, {AcknowledgedTransactions: 5, SuccessfulReads: 7}, {AcknowledgedTransactions: 4, SuccessfulReads: 7}} {
		if err := assertPhaseOracleProgress(assertions, before, after, true); err == nil {
			t.Fatalf("assertPhaseOracleProgress(%+v) error=nil", after)
		}
	}
	if err := assertPhaseOracleProgress(assertions, before, before, false); err != nil {
		t.Fatalf("dry-run progress assertion error=%v", err)
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

func TestRecorderSanitizesDatabasePayloadsBeforePersistence(t *testing.T) {
	const secret = "opaque-database-secret"
	ctx := context.Background()
	st := store.NewMemoryStore()
	if err := st.CreateRun(ctx, store.Run{ID: "sanitized", ScenarioName: "test", Status: "running"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	sink, err := artifacts.NewSanitizedSink(t.TempDir(), provider.NewRedactor([]string{secret}))
	if err != nil {
		t.Fatal(err)
	}
	if err := appendEvent(ctx, st, sink, "sanitized", artifacts.EventNow("test", "phase", "actor", map[string]any{"password": "visible", "note": secret, secret: "safe-key-value"})); err != nil {
		t.Fatal(err)
	}
	if err := appendMetric(ctx, st, sink, "sanitized", "test", 1, map[string]any{"authorization": "Bearer abcdefghijklmnop", "note": secret}); err != nil {
		t.Fatal(err)
	}
	details, err := st.GetRun(ctx, "sanitized")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(details)
	for _, prohibited := range []string{secret, "visible", "abcdefghijklmnop"} {
		if strings.Contains(string(raw), prohibited) {
			t.Fatalf("database details leaked %q: %s", prohibited, raw)
		}
	}
}

func TestFinalizeRedactsReturnedErrors(t *testing.T) {
	const secret = "opaque-error-secret"
	sink, err := artifacts.NewSanitizedSink(t.TempDir(), provider.NewRedactor([]string{secret}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = finalize(context.Background(), nil, sink, Result{RunID: "redacted-error", ScenarioName: "test"}, RunFailed, errors.New("provider failure: "+secret))
	if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), provider.Redacted) {
		t.Fatalf("finalize() error = %v", err)
	}
}

func TestFinalizeQuarantinesUnscannableArtifactRoot(t *testing.T) {
	sink, err := artifacts.NewSanitizedSink(t.TempDir(), provider.NewRedactor(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(sink.Root(), "unsafe-link")); err != nil {
		t.Fatal(err)
	}
	result, err := finalize(context.Background(), nil, sink, Result{RunID: "unscannable", ScenarioName: "test"}, RunPassed, nil)
	if err == nil || !strings.Contains(err.Error(), "redaction scan failed") || result.Status != RunFailed {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	if _, err := os.Lstat(filepath.Join(sink.Root(), "unsafe-link")); !os.IsNotExist(err) {
		t.Fatalf("unsafe symlink survived quarantine: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sink.Root(), "redaction-scan.json")); err != nil {
		t.Fatalf("redaction report missing after quarantine: %v", err)
	}
}

func TestFinalizeFailsGateWhenArtifactBypassesRedaction(t *testing.T) {
	const secret = "opaque-finalize-secret"
	sink, err := artifacts.NewSanitizedSink(t.TempDir(), provider.NewRedactor([]string{secret}))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sink.Root(), "bypass.log"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := finalize(context.Background(), nil, sink, Result{RunID: "redaction-gate", ScenarioName: "test"}, RunPassed, nil)
	if err == nil || !strings.Contains(err.Error(), "redaction scan found") {
		t.Fatalf("finalize() error = %v", err)
	}
	if result.Status != RunFailed {
		t.Fatalf("status = %q", result.Status)
	}
	report, readErr := os.ReadFile(filepath.Join(sink.Root(), "redaction-scan.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(report), secret) {
		t.Fatalf("scan report leaked secret: %s", report)
	}
	quarantined, readErr := os.ReadFile(filepath.Join(sink.Root(), "bypass.log"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(quarantined), secret) {
		t.Fatalf("quarantined artifact retained secret: %s", quarantined)
	}
}

type recordingDriver struct {
	deleted                  bool
	captured                 int
	omitEndpoints            bool
	createdScenario          spec.ResolvedScenario
	preflightEnvironmentSpec spec.EnvironmentSpec
	createdEnvironmentSpec   spec.EnvironmentSpec
}

func (d *recordingDriver) Name() string { return "test" }
func (d *recordingDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityArtifacts, env.CapabilityPerNodeEndpoints)
}
func (d *recordingDriver) Validate(spec.EnvironmentSpec) error { return nil }
func (d *recordingDriver) Preflight(_ context.Context, environmentSpec spec.EnvironmentSpec) error {
	d.preflightEnvironmentSpec = environmentSpec
	return nil
}
func (d *recordingDriver) Create(_ context.Context, scenario spec.ResolvedScenario, environmentSpec spec.EnvironmentSpec) (env.Environment, error) {
	d.createdScenario = scenario
	d.createdEnvironmentSpec = environmentSpec
	return env.Environment{Name: scenario.Metadata.Name, Driver: "test", Namespace: "test", Metadata: map[string]string{"nodeCount": "1"}}, nil
}
func (d *recordingDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *recordingDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0, Resource: "test/myceld-0"}}, nil
}
func (d *recordingDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	if d.omitEndpoints {
		return nil, nil
	}
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
	d.captured++
	_, err := sink.WriteJSON("environment/state.json", environment)
	return err
}
