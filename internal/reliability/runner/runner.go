package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/actors"
	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/events"
	"github.com/MycelDB/mycel-lab/internal/reliability/report"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
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
	driver := opts.EnvironmentDriver
	if driver == nil {
		driver = env.DryRunDriver{}
	}
	if !opts.DryRun {
		if err := env.RequireDestructiveConfirmation(env.Options{ConfirmDestructive: opts.ConfirmDestructive}); err != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
	}
	if err := driver.Preflight(ctx); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	environment, err := driver.Create(ctx, scenario)
	if err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	cleanupEnvironment := true
	defer func() {
		if cleanupEnvironment {
			_ = driver.Delete(context.Background(), environment)
		}
	}()
	if err := driver.CaptureState(ctx, environment, sink); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	recorder := &runRecorder{store: opts.Store, sink: sink, runID: runID}
	eventRuntime := opts.EventRuntime
	if eventRuntime == nil {
		eventRuntime = events.LocalRuntime{}
	}
	scheduler := actors.NewSchedulerWithRecorder(scenario, opts.ActorFactory, recorder)
	if err := scheduler.Start(ctx); err != nil {
		return finalize(ctx, opts.Store, sink, result, RunFailed, err)
	}
	defer scheduler.Stop(context.Background())

	for i := range result.Phases {
		select {
		case <-ctx.Done():
			_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
			return finalize(ctx, opts.Store, sink, result, RunAborted, ctx.Err())
		default:
		}
		if err := Transition(&result.Phases[i], PhaseRunning, time.Now().UTC()); err != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
		phaseSpec := scenario.Phases[i]
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-started", phaseSpec.Name, "", map[string]any{"dryRun": opts.DryRun}))
		if err := eventRuntime.ExecutePhaseEvents(ctx, phaseSpec, opts.DryRun, recorder); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
		if err := scheduler.ApplyPhase(ctx, phaseSpec); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
		if opts.DryRun {
			_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-dry-run", phaseSpec.Name, "", map[string]any{"plannedDuration": phaseSpec.Duration.String()}))
		} else if phaseSpec.Duration.Duration > 0 {
			timer := time.NewTimer(phaseSpec.Duration.Duration)
			select {
			case <-ctx.Done():
				timer.Stop()
				_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
				return finalize(ctx, opts.Store, sink, result, RunAborted, ctx.Err())
			case <-timer.C:
			}
		}
		if err := scheduler.RestoreBaseRates(ctx); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
		if err := Transition(&result.Phases[i], PhasePassed, time.Now().UTC()); err != nil {
			return finalize(ctx, opts.Store, sink, result, RunFailed, err)
		}
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-passed", phaseSpec.Name, "", nil))
	}
	if result.Status == RunFailed && opts.KeepEnvironmentOnFailure {
		cleanupEnvironment = false
	}
	return finalize(ctx, opts.Store, sink, result, RunPassed, nil)
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
}

func (r *runRecorder) RecordActorEvent(ctx context.Context, event actors.ActorEvent) error {
	if err := appendEvent(ctx, r.store, r.sink, r.runID, artifacts.EventNow(event.Type, "", event.ActorID, event)); err != nil {
		return err
	}
	if event.Type == "graph-transaction-acknowledged" {
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "commits", 1, map[string]any{"actorId": event.ActorID, "groupName": event.GroupName})
		_ = appendMetric(ctx, r.store, r.sink, r.runID, "transaction_latency_ms", 0, map[string]any{"actorId": event.ActorID})
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
