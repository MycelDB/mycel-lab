package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/actors"
	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/events"
	"github.com/MycelDB/mycel-lab/internal/reliability/oracle"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/report"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
)

type PhaseStatus string

const (
	PhasePending PhaseStatus = "pending"
	PhaseRunning PhaseStatus = "running"
	PhasePassed  PhaseStatus = "passed"
	PhaseFailed  PhaseStatus = "failed"
	PhaseAborted PhaseStatus = "aborted"
)

type RunStatus string

const (
	RunRunning RunStatus = "running"
	RunPassed  RunStatus = "passed"
	RunFailed  RunStatus = "failed"
	RunAborted RunStatus = "aborted"
)

type PhaseState struct {
	Name       string        `json:"name"`
	Status     PhaseStatus   `json:"status"`
	Duration   time.Duration `json:"duration"`
	StartedAt  time.Time     `json:"startedAt,omitempty"`
	FinishedAt time.Time     `json:"finishedAt,omitempty"`
}

type Result struct {
	RunID        string       `json:"runId"`
	ScenarioName string       `json:"scenarioName"`
	Status       RunStatus    `json:"status"`
	DryRun       bool         `json:"dryRun"`
	StartedAt    time.Time    `json:"startedAt"`
	FinishedAt   time.Time    `json:"finishedAt"`
	ArtifactRoot string       `json:"artifactRoot"`
	Phases       []PhaseState `json:"phases"`
}

type Options struct {
	DryRun                   bool
	ArtifactRoot             string
	Store                    store.Store
	ScenarioVersion          int
	ConfirmDestructive       bool
	KeepEnvironmentOnFailure bool
	ConsoleEndpoints         bool
	ConsolePortBase          int
	EnvironmentOverride      *spec.EnvironmentSpec
	ProgressWriter           io.Writer
	EnvironmentDriver        env.EnvironmentDriver
	EventRuntime             events.Runtime
	ActorFactory             actors.Factory
}

type SuiteResult struct {
	SuiteName string   `json:"suiteName"`
	DryRun    bool     `json:"dryRun"`
	Results   []Result `json:"results"`
}

func ValidTransition(from, to PhaseStatus) bool {
	switch from {
	case PhasePending:
		return to == PhaseRunning || to == PhaseAborted
	case PhaseRunning:
		return to == PhasePassed || to == PhaseFailed || to == PhaseAborted
	case PhasePassed, PhaseFailed, PhaseAborted:
		return false
	default:
		return false
	}
}

func Transition(phase *PhaseState, to PhaseStatus, now time.Time) error {
	if !ValidTransition(phase.Status, to) {
		return fmt.Errorf("invalid phase transition %s -> %s", phase.Status, to)
	}
	phase.Status = to
	if to == PhaseRunning {
		phase.StartedAt = now
	}
	if to == PhasePassed || to == PhaseFailed || to == PhaseAborted {
		phase.FinishedAt = now
	}
	return nil
}

func RunScenario(ctx context.Context, scenario spec.ResolvedScenario, opts Options) (Result, error) {
	started := time.Now().UTC()
	runID := fmt.Sprintf("%s-%s", safeName(scenario.Metadata.Name), started.Format("20060102-150405"))
	sink, err := artifacts.NewSink(artifacts.RunRoot(opts.ArtifactRoot, runID))
	if err != nil {
		return Result{}, err
	}
	result := Result{RunID: runID, ScenarioName: scenario.Metadata.Name, Status: RunRunning, DryRun: opts.DryRun, StartedAt: started, ArtifactRoot: sink.Root()}
	for _, phase := range scenario.Phases {
		result.Phases = append(result.Phases, PhaseState{Name: phase.Name, Status: PhasePending, Duration: phase.Duration.Duration})
	}
	if opts.EnvironmentOverride != nil {
		scenario.Environment = mergeEnvironmentOverride(scenario.Environment, *opts.EnvironmentOverride)
	}
	driver := opts.EnvironmentDriver
	effectiveEnvironment := scenario.Environment
	if driver == nil {
		var selectErr error
		driver, effectiveEnvironment, selectErr = env.SelectDriver(scenario.Environment, env.DriverSelectionOptions{DryRun: opts.DryRun, ConfirmDestructive: opts.ConfirmDestructive})
		if selectErr != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, selectErr)
		}
	} else if opts.DryRun {
		effectiveEnvironment.Driver = "dry-run"
	}
	scenario.Environment = effectiveEnvironment
	if err := writeInitialArtifacts(sink, scenario); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	if opts.Store != nil {
		resolvedJSON, _ := json.Marshal(scenario)
		var runPhases []store.RunPhase
		for _, phase := range scenario.Phases {
			phaseJSON, _ := json.Marshal(phase)
			runPhases = append(runPhases, store.RunPhase{Name: phase.Name, Status: string(PhasePending), SpecSnapshotJSON: phaseJSON})
		}
		if err := opts.Store.CreateRun(ctx, store.Run{ID: runID, ScenarioName: scenario.Metadata.Name, ScenarioVersion: opts.ScenarioVersion, Status: string(RunRunning), Seed: scenario.Seed, ResolvedJSON: resolvedJSON}, nil, runPhases); err != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
	}
	if !opts.DryRun {
		if err := env.RequireDestructiveConfirmation(env.Options{ConfirmDestructive: opts.ConfirmDestructive}); err != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
	}
	if err := driver.Preflight(ctx, effectiveEnvironment); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	environment, err := driver.Create(ctx, scenario, effectiveEnvironment)
	if err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	if err := driver.WaitReady(ctx, environment); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	cleanupEnvironment := true
	runFailedAfterEnvironmentCreate := false
	defer func() {
		if cleanupEnvironment && !(runFailedAfterEnvironmentCreate && opts.KeepEnvironmentOnFailure) {
			_ = driver.Delete(context.Background(), environment)
		}
	}()
	failAfterEnvironmentCreate := func(status RunStatus, err error) (Result, error) {
		runFailedAfterEnvironmentCreate = true
		return finalize(ctx, opts.Store, sink, result, status, err)
	}
	nodes, err := driver.Nodes(ctx, environment)
	if err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	driverEndpoints, err := driver.Endpoints(ctx, environment)
	if err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if err := env.WriteEnvironmentArtifacts(sink, environment, driver, nodes, driverEndpoints); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if err := driver.CaptureState(ctx, environment, sink); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	endpoints := consoleEndpointsFromDriver(driverEndpoints)
	endpointAddrs := endpointAddresses(endpoints)
	resources, err := provision.PlanScenario(scenario, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: opts.DryRun})
	if err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if _, err := sink.WriteJSON("environment/scenario-resources.json", resources); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	needsActorEndpoints := len(resources.Assignments) > 0
	portForwardEnvironment := environment.Driver == "k3d"
	if (opts.ConsoleEndpoints || needsActorEndpoints) && !opts.DryRun {
		if _, err := sink.WriteJSON("environment/console-endpoints.json", endpoints); err != nil {
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if portForwardEnvironment {
			session, err := env.StartConsolePortForwards(ctx, environment, endpoints)
			if err != nil {
				return failAfterEnvironmentCreate(RunFailed, err)
			}
			defer session.Stop(context.Background())
		}
		if opts.ConsoleEndpoints {
			writeConsoleEndpoints(opts.ProgressWriter, endpoints)
		}
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("console-endpoints-ready", "", "", map[string]any{"endpoints": endpoints}))
	}
	provisionDryRun := opts.DryRun || environment.Driver == "dry-run" || len(endpointAddrs) == 0
	if err := provision.ProvisionScenario(ctx, &resources, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: provisionDryRun}); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if _, err := sink.WriteJSON("environment/scenario-resources.json", resources); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if _, err := sink.WriteJSON("actors/assignments.json", resources.Assignments); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	recorder := &runRecorder{store: opts.Store, sink: sink, runID: runID}
	eventRuntime := opts.EventRuntime
	if eventRuntime == nil {
		eventRuntime = defaultEventRuntime(opts, driver, environment)
	}
	assignmentMap := provision.AssignmentMap(resources)
	if provisionDryRun {
		assignmentMap = nil
	}
	scheduler := actors.NewSchedulerWithRecorderAndAssignments(scenario, opts.ActorFactory, recorder, assignmentMap)
	if err := scheduler.Start(ctx); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	schedulerStopped := false
	defer func() {
		if !schedulerStopped {
			_ = scheduler.Stop(context.Background())
		}
	}()

	for i := range result.Phases {
		select {
		case <-ctx.Done():
			_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
			return failAfterEnvironmentCreate(RunAborted, ctx.Err())
		default:
		}
		if err := Transition(&result.Phases[i], PhaseRunning, time.Now().UTC()); err != nil {
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		phaseSpec := scenario.Phases[i]
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-started", phaseSpec.Name, "", map[string]any{"dryRun": opts.DryRun}))
		if err := eventRuntime.ExecutePhaseEvents(ctx, phaseSpec, opts.DryRun, recorder); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if err := scheduler.ApplyPhase(ctx, phaseSpec); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if opts.DryRun {
			_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-dry-run", phaseSpec.Name, "", map[string]any{"plannedDuration": phaseSpec.Duration.String()}))
		} else if phaseSpec.Duration.Duration > 0 {
			timer := time.NewTimer(phaseSpec.Duration.Duration)
			select {
			case <-ctx.Done():
				timer.Stop()
				_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
				return failAfterEnvironmentCreate(RunAborted, ctx.Err())
			case <-timer.C:
			}
		}
		if err := scheduler.RestoreBaseRates(ctx); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if err := Transition(&result.Phases[i], PhasePassed, time.Now().UTC()); err != nil {
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-passed", phaseSpec.Name, "", nil))
	}
	if err := scheduler.Stop(ctx); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	schedulerStopped = true
	if err := writeFinalOracleReport(ctx, sink, recorder, resources, !provisionDryRun); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if err := provision.CleanupScenario(ctx, &resources, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: provisionDryRun}); err != nil {
		_, _ = sink.WriteJSON("environment/scenario-resources.json", resources)
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	_, _ = sink.WriteJSON("environment/scenario-resources.json", resources)
	return finalize(ctx, opts.Store, sink, result, RunPassed, nil)
}

func mergeEnvironmentOverride(base spec.EnvironmentSpec, override spec.EnvironmentSpec) spec.EnvironmentSpec {
	out := base
	if override.Driver != "" {
		out.Driver = override.Driver
	}
	if override.Namespace != "" {
		out.Namespace = override.Namespace
	}
	if override.KeepOnFailure {
		out.KeepOnFailure = true
	}
	if len(override.Options) > 0 {
		if out.Options == nil {
			out.Options = map[string]any{}
		}
		for key, value := range override.Options {
			out.Options[key] = value
		}
	}
	if len(override.Capabilities.Required) > 0 {
		out.Capabilities.Required = append([]string(nil), override.Capabilities.Required...)
	}
	return out
}

func consoleEndpointsFromDriver(endpoints []env.Endpoint) []env.ConsoleEndpoint {
	out := make([]env.ConsoleEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		out = append(out, env.ConsoleEndpoint{NodeName: endpoint.NodeName, ServiceName: endpoint.ServiceName, LocalAddress: firstNonEmpty(endpoint.LocalAddress, "127.0.0.1"), LocalPort: endpoint.LocalPort, RemotePort: endpoint.RemotePort, DaemonAddr: endpoint.DaemonAddr})
	}
	return out
}

func endpointAddresses(endpoints []env.ConsoleEndpoint) []string {
	out := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.DaemonAddr != "" {
			out = append(out, endpoint.DaemonAddr)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func writeConsoleEndpoints(w io.Writer, endpoints []env.ConsoleEndpoint) {
	if w == nil || len(endpoints) == 0 {
		return
	}
	fmt.Fprintln(w, "Console endpoints:")
	for _, endpoint := range endpoints {
		fmt.Fprintf(w, "  %s  %s\n", endpoint.NodeName, endpoint.DaemonAddr)
	}
}

func defaultEventRuntime(opts Options, driver env.EnvironmentDriver, environment env.Environment) events.Runtime {
	if opts.DryRun {
		return events.LocalRuntime{}
	}
	return events.NewDriverRuntime(driver, environment)
}

func RunSuiteFile(ctx context.Context, path string, opts Options) (SuiteResult, error) {
	loaded, err := spec.LoadFile(path)
	if err != nil {
		return SuiteResult{}, err
	}
	suite, ok := loaded.(spec.Suite)
	if !ok {
		return SuiteResult{}, fmt.Errorf("%s is %T, want Suite", path, loaded)
	}
	baseDir := filepath.Dir(path)
	out := SuiteResult{SuiteName: suite.Metadata.Name, DryRun: opts.DryRun}
	for _, scenarioRef := range suite.Scenarios {
		if scenarioRef.Path == "" {
			return out, fmt.Errorf("suite scenario refs are not supported until catalog run wiring")
		}
		scenarioPath := scenarioRef.Path
		if !filepath.IsAbs(scenarioPath) {
			scenarioPath = filepath.Join(baseDir, scenarioPath)
		}
		resolved, err := catalog.ResolveScenarioFile(scenarioPath, catalog.ResolveOptions{})
		if err != nil {
			return out, err
		}
		result, err := RunScenario(ctx, resolved, opts)
		out.Results = append(out.Results, result)
		if err != nil && suite.Execution.StopOnFailure {
			return out, err
		}
	}
	return out, nil
}

func writeInitialArtifacts(sink *artifacts.Sink, scenario spec.ResolvedScenario) error {
	if _, err := sink.WriteJSON("resolved-scenario.json", scenario); err != nil {
		return err
	}
	manifests, err := deploy.RenderKubernetesManifests(scenario)
	if err != nil {
		return err
	}
	if _, err := sink.WriteText("manifests/myceld.yaml", manifests.YAML); err != nil {
		return err
	}
	_, err = sink.AppendJSONL("events.jsonl", artifacts.EventNow("run-created", "", "", map[string]any{"scenario": scenario.Metadata.Name}))
	return err
}

func writeFinalOracleReport(ctx context.Context, sink *artifacts.Sink, recorder *runRecorder, resources provision.ScenarioResources, live bool) error {
	if recorder == nil {
		return nil
	}
	recorder.mu.Lock()
	acknowledged := append([]oracle.Transaction(nil), recorder.acknowledged...)
	readChecks := append([]map[string]any(nil), recorder.readChecks...)
	failedWrites := recorder.failedWrites
	expectedByActor := cloneGraphCountsMap(recorder.expectedByActor)
	recorder.mu.Unlock()
	failedReads := 0
	for _, check := range readChecks {
		if ok, _ := check["ok"].(bool); !ok {
			failedReads++
		}
	}
	report := finalOracleReport{
		AcknowledgedTransactions: len(acknowledged),
		ExpectedCounts:           oracle.ExpectedCounts(acknowledged),
		ExpectedCountsByActor:    expectedByActor,
		ReadChecks:               len(readChecks),
		FailedReads:              failedReads,
		FailedWrites:             failedWrites,
	}
	if live {
		report.LiveChecks = runLiveFinalOracleChecks(ctx, resources, expectedByActor)
	}
	report.OK = report.FailedReads == 0 && report.FailedWrites == 0
	for _, check := range report.LiveChecks {
		if !check.OK {
			report.OK = false
			break
		}
	}
	if _, err := sink.WriteJSON("oracle/final-report.json", report); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Final oracle report\n\n")
	fmt.Fprintf(&b, "- Acknowledged transactions: %d\n", report.AcknowledgedTransactions)
	fmt.Fprintf(&b, "- Expected nodes: %d\n", report.ExpectedCounts.Nodes)
	fmt.Fprintf(&b, "- Expected edges: %d\n", report.ExpectedCounts.Edges)
	fmt.Fprintf(&b, "- Failed writes: %d\n", report.FailedWrites)
	fmt.Fprintf(&b, "- Read checks: %d\n", report.ReadChecks)
	fmt.Fprintf(&b, "- Failed reads: %d\n", report.FailedReads)
	if len(report.LiveChecks) > 0 {
		fmt.Fprintf(&b, "\n## Live graph checks\n\n")
		for _, check := range report.LiveChecks {
			fmt.Fprintf(&b, "- `%s`: expected nodes=%d, actual nodes=%d, event-derived expected edges=%d, ok=%t", check.ActorID, check.Expected.Nodes, check.Actual.Nodes, check.Expected.Edges, check.OK)
			if check.Error != "" {
				fmt.Fprintf(&b, ", error=%s", check.Error)
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	fmt.Fprintf(&b, "\n- OK: %t\n", report.OK)
	_, err := sink.WriteText("oracle/final-report.md", b.String())
	return err
}

type finalOracleReport struct {
	AcknowledgedTransactions int                    `json:"acknowledgedTransactions"`
	ExpectedCounts           oracle.Counts          `json:"expectedCounts"`
	ExpectedCountsByActor    map[string]graphCounts `json:"expectedCountsByActor,omitempty"`
	LiveChecks               []finalOracleLiveCheck `json:"liveChecks,omitempty"`
	ReadChecks               int                    `json:"readChecks"`
	FailedReads              int                    `json:"failedReads"`
	FailedWrites             int                    `json:"failedWrites"`
	OK                       bool                   `json:"ok"`
}

type finalOracleLiveCheck struct {
	ActorID    string      `json:"actorId"`
	SpaceID    string      `json:"spaceId"`
	DomainID   string      `json:"domainId"`
	DaemonAddr string      `json:"daemonAddr"`
	Expected   graphCounts `json:"expected"`
	Actual     graphCounts `json:"actual"`
	OK         bool        `json:"ok"`
	Error      string      `json:"error,omitempty"`
}

type graphCounts struct {
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
}

func runLiveFinalOracleChecks(ctx context.Context, resources provision.ScenarioResources, expectedByActor map[string]graphCounts) []finalOracleLiveCheck {
	checks := []finalOracleLiveCheck{}
	for _, assignment := range resources.Assignments {
		if assignment.BehaviorType != "graph-transaction" || assignment.SpaceID == "" || assignment.DomainID == "" || assignment.DaemonAddr == "" {
			continue
		}
		expected := expectedByActor[assignment.ActorID]
		check := finalOracleLiveCheck{ActorID: assignment.ActorID, SpaceID: assignment.SpaceID, DomainID: assignment.DomainID, DaemonAddr: assignment.DaemonAddr, Expected: expected}
		cfg := mycel.Config{Addr: assignment.DaemonAddr, Username: assignment.Username, Password: assignment.Password, ClientName: "mycel-lab-oracle"}
		if assignment.AccessToken != "" {
			cfg.Username = ""
			cfg.Password = ""
			cfg.AccessToken = assignment.AccessToken
			cfg.RefreshToken = assignment.RefreshToken
			cfg.AccessTokenExpireTime = assignment.TokenExpires
		}
		client, err := mycel.Dial(ctx, cfg)
		if err != nil {
			check.Error = err.Error()
			checks = append(checks, check)
			continue
		}
		nodes, nodeErr := queryCount(ctx, client, assignment.SpaceID, assignment.DomainID, "MATCH (n) RETURN count(n) FETCH FIRST 1 ROW ONLY")
		_ = client.Close()
		check.Actual = graphCounts{Nodes: int(nodes)}
		if nodeErr != nil {
			check.Error = nodeErr.Error()
		} else {
			check.OK = check.Actual.Nodes == check.Expected.Nodes
		}
		checks = append(checks, check)
	}
	return checks
}

func queryCount(ctx context.Context, client *mycel.Client, spaceID, domainID, gql string) (int64, error) {
	res, err := client.QueryGQLReadOnly(ctx, spaceID, domainID, gql, 1)
	if err != nil {
		return 0, err
	}
	return countFromResult(res)
}

func countFromResult(res *clientv1.QueryResult) (int64, error) {
	rows := res.GetRows()
	if len(rows) != 1 {
		return 0, fmt.Errorf("count query returned %d rows", len(rows))
	}
	field := rows[0].GetFields()["count"]
	if field == nil || field.GetScalar() == nil {
		for _, v := range rows[0].GetFields() {
			if v.GetScalar() != nil {
				field = v
				break
			}
		}
	}
	if field == nil || field.GetScalar() == nil {
		return 0, fmt.Errorf("count query did not return a scalar count")
	}
	n := field.GetScalar().GetNumberValue()
	if math.Trunc(n) != n {
		return 0, fmt.Errorf("count query returned non-integer %v", n)
	}
	return int64(n), nil
}

func cloneGraphCountsMap(in map[string]graphCounts) map[string]graphCounts {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]graphCounts, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func countTransactionOps(tx oracle.Transaction) graphCounts {
	var counts graphCounts
	for _, op := range tx.Operations {
		switch op.Type {
		case "createNode":
			counts.Nodes++
		case "createEdge":
			counts.Edges++
		}
	}
	return counts
}

func finalize(ctx context.Context, st store.Store, sink *artifacts.Sink, result Result, status RunStatus, runErr error) (Result, error) {
	result.Status = status
	result.FinishedAt = time.Now().UTC()
	for i := range result.Phases {
		if result.Phases[i].Status == PhasePending {
			_ = Transition(&result.Phases[i], PhaseAborted, result.FinishedAt)
		}
		if result.Phases[i].Status == PhaseRunning {
			_ = Transition(&result.Phases[i], PhaseAborted, result.FinishedAt)
		}
	}
	var artifactPaths []string
	if path, err := sink.WriteJSON("result.json", result); err == nil {
		artifactPaths = append(artifactPaths, path)
	}
	summary := report.RunSummary{RunID: result.RunID, ScenarioName: result.ScenarioName, Status: string(result.Status), StartedAt: result.StartedAt, FinishedAt: result.FinishedAt, DryRun: result.DryRun, Artifacts: artifactPaths}
	for _, phase := range result.Phases {
		actualDuration := time.Duration(0)
		if !phase.StartedAt.IsZero() && !phase.FinishedAt.IsZero() {
			actualDuration = phase.FinishedAt.Sub(phase.StartedAt).Round(time.Millisecond)
		}
		summary.Phases = append(summary.Phases, report.PhaseSummary{Name: phase.Name, Status: string(phase.Status), PlannedDuration: phase.Duration, ActualDuration: actualDuration})
	}
	if path, err := sink.WriteText("summary.md", report.Markdown(summary)); err == nil {
		artifactPaths = append(artifactPaths, path)
	}
	if st != nil {
		_ = st.FinishRun(ctx, result.RunID, string(result.Status))
		for _, artifactPath := range artifactPaths {
			_ = st.AppendArtifact(ctx, store.RunArtifact{RunID: result.RunID, Type: "file", Path: artifactPath, Metadata: []byte(`{}`)})
		}
	}
	if runErr != nil {
		return result, runErr
	}
	return result, nil
}

type runRecorder struct {
	store store.Store
	sink  *artifacts.Sink
	runID string

	mu              sync.Mutex
	acknowledged    []oracle.Transaction
	expectedByActor map[string]graphCounts
	readChecks      []map[string]any
	failedWrites    int
}

func (r *runRecorder) RecordActorEvent(ctx context.Context, event actors.ActorEvent) error {
	if err := appendEvent(ctx, r.store, r.sink, r.runID, artifacts.EventNow(event.Type, "", event.ActorID, event)); err != nil {
		return err
	}
	switch event.Type {
	case "graph-transaction-acknowledged":
		r.mu.Lock()
		r.acknowledged = append(r.acknowledged, event.Transaction)
		if r.expectedByActor == nil {
			r.expectedByActor = map[string]graphCounts{}
		}
		counts := countTransactionOps(event.Transaction)
		current := r.expectedByActor[event.ActorID]
		current.Nodes += counts.Nodes
		current.Edges += counts.Edges
		r.expectedByActor[event.ActorID] = current
		r.mu.Unlock()
		_, _ = r.sink.AppendJSONL("actors/acknowledged-commits.jsonl", event.Transaction)
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "commits", 1, map[string]any{"actorId": event.ActorID, "groupName": event.GroupName})
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "transaction_latency_ms", 0, map[string]any{"actorId": event.ActorID})
	case "graph-transaction-failed":
		if event.Transaction.ErrorClass == "permanent" {
			r.mu.Lock()
			r.failedWrites++
			r.mu.Unlock()
		}
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "write_failures", 1, map[string]any{"actorId": event.ActorID, "groupName": event.GroupName, "class": event.Transaction.ErrorClass})
	case "graph-read-check":
		payload := map[string]any{"actorId": event.ActorID, "groupName": event.GroupName, "time": time.Now().UTC()}
		for k, v := range event.Payload {
			payload[k] = v
		}
		r.mu.Lock()
		r.readChecks = append(r.readChecks, payload)
		r.mu.Unlock()
		_, _ = r.sink.AppendJSONL("actors/read-checks.jsonl", payload)
		if ok, _ := payload["ok"].(bool); !ok {
			_ = appendMetric(ctx, r.store, r.sink, r.runID, "read_failures", 1, map[string]any{"actorId": event.ActorID, "groupName": event.GroupName, "class": payload["class"]})
		}
	}
	return nil
}

func (r *runRecorder) RecordRuntimeEvent(ctx context.Context, record events.Record) error {
	if err := appendEvent(ctx, r.store, r.sink, r.runID, events.ArtifactEvent(record)); err != nil {
		return err
	}
	if record.Type == "event-started" && (record.EventType == "pod-restart" || record.EventType == "rolling-restart") {
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "pod_restarts", 1, map[string]any{"phase": record.PhaseName, "eventType": record.EventType})
	}
	return nil
}

func appendMetric(ctx context.Context, st store.Store, sink *artifacts.Sink, runID, name string, value float64, labels map[string]any) error {
	if labels == nil {
		labels = map[string]any{}
	}
	labelJSON, _ := json.Marshal(labels)
	record := map[string]any{"runId": runID, "name": name, "value": value, "labels": labels, "time": time.Now().UTC()}
	if _, err := sink.AppendJSONL("metrics.jsonl", record); err != nil {
		return err
	}
	if st != nil {
		return st.AppendMetric(ctx, store.RunMetric{RunID: runID, Name: name, Value: value, Labels: labelJSON})
	}
	return nil
}

func appendEvent(ctx context.Context, st store.Store, sink *artifacts.Sink, runID string, event artifacts.Event) error {
	if _, err := sink.AppendJSONL("events.jsonl", event); err != nil {
		return err
	}
	if st != nil {
		payload, _ := json.Marshal(event.Payload)
		if len(payload) == 0 || string(payload) == "null" {
			payload = []byte(`{}`)
		}
		return st.AppendEvent(ctx, store.RunEvent{RunID: runID, PhaseName: event.PhaseName, EventType: event.Type, ActorID: event.ActorID, Payload: payload})
	}
	return nil
}

func safeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "run"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

var ErrAborted = errors.New("run aborted")
