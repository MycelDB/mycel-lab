package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
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
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
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

type harnessBuildIdentity struct {
	BuildInfoAvailable bool              `json:"buildInfoAvailable"`
	ExecutableSHA256   string            `json:"executableSHA256,omitempty"`
	GoVersion          string            `json:"goVersion,omitempty"`
	ModulePath         string            `json:"modulePath,omitempty"`
	ModuleVersion      string            `json:"moduleVersion,omitempty"`
	ModuleSum          string            `json:"moduleSum,omitempty"`
	VCS                map[string]string `json:"vcs,omitempty"`
}

type Result struct {
	RunID          string                   `json:"runId"`
	ScenarioName   string                   `json:"scenarioName"`
	Status         RunStatus                `json:"status"`
	DryRun         bool                     `json:"dryRun"`
	StartedAt      time.Time                `json:"startedAt"`
	FinishedAt     time.Time                `json:"finishedAt"`
	ArtifactRoot   string                   `json:"artifactRoot"`
	Phases         []PhaseState             `json:"phases"`
	ProviderBudget *provider.BudgetSnapshot `json:"providerBudget,omitempty"`
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
	ImageOverride            string
	ProviderProfileOverride  string
	ProviderManifest         provider.CapabilityManifest
	ProfileDirs              []string
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
	parentCtx := ctx
	started := time.Now().UTC()
	runID := fmt.Sprintf("%s-%s", safeName(scenario.Metadata.Name), started.Format("20060102-150405"))
	secretValues := []string{provision.DefaultAdminPassword}
	if scenario.ProviderProfile != nil {
		secretValues = append(secretValues, scenario.ProviderProfile.RuntimeSecretValues()...)
	}
	redactor := provider.NewRedactor(secretValues)
	sink, err := artifacts.NewSanitizedSink(artifacts.RunRoot(opts.ArtifactRoot, runID), redactor)
	if err != nil {
		return Result{}, err
	}
	result := Result{RunID: runID, ScenarioName: scenario.Metadata.Name, Status: RunRunning, DryRun: opts.DryRun, StartedAt: started, ArtifactRoot: sink.Root()}
	for _, phase := range scenario.Phases {
		result.Phases = append(result.Phases, PhaseState{Name: phase.Name, Status: PhasePending, Duration: phase.Duration.Duration})
	}
	var budgetLedger *provider.Ledger
	budgetCancel := func() {}
	if scenario.ProviderProfile != nil {
		budgetLedger, err = provider.NewLedger(scenario.ProviderProfile.Profile.Budgets)
		if err != nil {
			return finalize(parentCtx, opts.Store, sink, result, RunFailed, err)
		}
		ctx, budgetCancel = budgetLedger.TotalContext(ctx)
		defer budgetCancel()
	}
	var sensitiveCleanup func() error
	sensitiveCleanupDone := false
	finish := func(status RunStatus, runErr error) (Result, error) {
		if sensitiveCleanup != nil && !sensitiveCleanupDone {
			sensitiveCleanupDone = true
			cleanupErr := sensitiveCleanup()
			cleanupEvidence := map[string]any{"completed": cleanupErr == nil}
			if cleanupErr != nil {
				cleanupEvidence["errorCategory"] = "sensitive_host_cleanup_failed"
				runErr = errors.Join(runErr, errors.New("sensitive host artifact cleanup failed"))
				status = RunFailed
			}
			if _, writeErr := sink.WriteJSON("environment/sensitive-host-cleanup.json", cleanupEvidence); writeErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf("write sensitive host cleanup evidence: %w", writeErr))
				status = RunFailed
			}
		}
		if budgetLedger != nil {
			snapshot := budgetLedger.Snapshot()
			if errors.Is(context.Cause(ctx), provider.ErrTotalTimeout) || !time.Now().UTC().Before(snapshot.Deadline) {
				budgetLedger.Close("total timeout reached")
				runErr = errors.Join(runErr, fmt.Errorf("%w: total timeout reached", provider.ErrBudgetExhausted))
				status = RunFailed
				snapshot = budgetLedger.Snapshot()
			}
			if snapshot.Closed {
				markLastPassedPhaseFailed(result.Phases)
				if status == RunPassed {
					runErr = errors.Join(runErr, fmt.Errorf("%w: provider budget closed before successful completion", provider.ErrBudgetExhausted))
					status = RunFailed
				}
			}
			result.ProviderBudget = &snapshot
			if _, writeErr := sink.WriteJSON("provider/budget-final.json", snapshot); writeErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf("write provider budget artifact: %w", writeErr))
				status = RunFailed
			}
		}
		return finalize(parentCtx, opts.Store, sink, result, status, runErr)
	}
	if opts.EnvironmentOverride != nil {
		scenario.Environment = mergeEnvironmentOverride(scenario.Environment, *opts.EnvironmentOverride)
	}
	imageOverride := strings.TrimSpace(opts.ImageOverride)
	if imageOverride != "" {
		scenario.Cluster.Image = imageOverride
	}
	driver := opts.EnvironmentDriver
	effectiveEnvironment := scenario.Environment
	if driver == nil {
		var selectErr error
		driver, effectiveEnvironment, selectErr = env.SelectDriver(scenario.Environment, env.DriverSelectionOptions{DryRun: opts.DryRun, ConfirmDestructive: opts.ConfirmDestructive})
		if selectErr != nil {
			return finish(RunFailed, selectErr)
		}
	} else if opts.DryRun {
		effectiveEnvironment.Driver = "dry-run"
	}
	applyComposeImageOverride(&effectiveEnvironment, imageOverride)
	scenario.Environment = effectiveEnvironment
	buildIdentity := currentHarnessBuildIdentity()
	if err := writeInitialArtifacts(sink, scenario, buildIdentity); err != nil {
		return finish(RunFailed, err)
	}
	if !opts.DryRun && requiresImmutableBuildProvenance(scenario) && !validHarnessBuildIdentity(buildIdentity) {
		return finish(RunFailed, errors.New("immutable harness build provenance is required"))
	}
	if opts.Store != nil {
		evidenceScenario := evidenceSafeResolvedScenario(scenario)
		resolvedJSON, sanitizeErr := sanitizedJSON(sink, evidenceScenario)
		if sanitizeErr != nil {
			return finish(RunFailed, sanitizeErr)
		}
		var runPhases []store.RunPhase
		for _, phase := range evidenceScenario.Phases {
			phaseJSON, sanitizeErr := sanitizedJSON(sink, phase)
			if sanitizeErr != nil {
				return finish(RunFailed, sanitizeErr)
			}
			runPhases = append(runPhases, store.RunPhase{Name: phase.Name, Status: string(PhasePending), SpecSnapshotJSON: phaseJSON})
		}
		if err := opts.Store.CreateRun(ctx, store.Run{ID: runID, ScenarioName: scenario.Metadata.Name, ScenarioVersion: opts.ScenarioVersion, Status: string(RunRunning), Seed: scenario.Seed, ResolvedJSON: resolvedJSON}, nil, runPhases); err != nil {
			return finish(RunFailed, err)
		}
	}
	if !opts.DryRun {
		if err := env.RequireDestructiveConfirmation(env.Options{ConfirmDestructive: opts.ConfirmDestructive}); err != nil {
			return finish(RunFailed, err)
		}
	}
	if err := driver.Preflight(ctx, effectiveEnvironment); err != nil {
		return finish(RunFailed, err)
	}
	retainEnvironmentOnFailure := opts.KeepEnvironmentOnFailure || effectiveEnvironment.KeepOnFailure
	environment, createErr := driver.Create(ctx, scenario, effectiveEnvironment)
	if createErr != nil && strings.TrimSpace(environment.Name) == "" {
		return finish(RunFailed, createErr)
	}
	if requiresImmutableBuildProvenance(scenario) {
		if environment.Metadata == nil {
			environment.Metadata = map[string]string{}
		}
		environment.Metadata["requireImageProvenance"] = strconv.FormatBool(!opts.DryRun)
	}
	cleanupEnvironment := true
	var resources provision.ScenarioResources
	resourcesPlanned := false
	scenarioCleanupCompleted := false
	var endpointAddrs []string
	defer func() {
		if cleanupEnvironment && !retainEnvironmentOnFailure {
			deleteCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			_ = driver.Delete(deleteCtx, environment)
			cancel()
		}
	}()
	cleanupAfterEnvironment := func(retentionRequested bool) error {
		evidence := map[string]any{
			"schemaVersion":            "reliability.mycel.io/post-run-cleanup/v1alpha1",
			"retentionRequested":       retentionRequested,
			"scenarioCleanupAttempted": scenarioCleanupCompleted,
			"scenarioCleanupAttempts":  0,
			"scenarioCleanupCompleted": scenarioCleanupCompleted,
			"driverDeleteAttempts":     0,
			"driverDeleteCompleted":    false,
			"completed":                false,
		}
		if retentionRequested {
			cleanupEnvironment = false
			evidence["retained"] = true
			_, writeErr := sink.WriteJSON("environment/post-run-cleanup.json", evidence)
			return writeErr
		}
		errorCategories := []string{}
		if resourcesPlanned && !scenarioCleanupCompleted {
			evidence["scenarioCleanupAttempted"] = true
			cleanupDryRun := opts.DryRun || environment.Driver == "dry-run" || len(endpointAddrs) == 0
			attempts, cleanupErr := runBoundedCleanupAttempts(3, 60*time.Second, func(cleanupCtx context.Context) error {
				return provision.CleanupScenario(cleanupCtx, &resources, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: cleanupDryRun})
			})
			evidence["scenarioCleanupAttempts"] = attempts
			if cleanupErr == nil {
				scenarioCleanupCompleted = true
				evidence["scenarioCleanupCompleted"] = true
			}
			if !scenarioCleanupCompleted {
				errorCategories = append(errorCategories, "scenario_cleanup_failed")
			}
			_, _ = sink.WriteJSON("environment/scenario-resources.json", resources)
		}
		deleteCompleted := false
		for attempt := 1; attempt <= 3; attempt++ {
			evidence["driverDeleteAttempts"] = attempt
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 60*time.Second)
			deleteErr := driver.Delete(deleteCtx, environment)
			deleteCancel()
			if deleteErr == nil {
				deleteCompleted = true
				break
			}
		}
		cleanupEnvironment = false
		if deleteCompleted {
			evidence["driverDeleteCompleted"] = true
		} else {
			errorCategories = append(errorCategories, "environment_delete_failed")
		}
		verification := env.DeletionVerification{}
		if verifier, ok := driver.(env.EnvironmentDeletionVerifier); ok {
			verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
			observed, err := verifier.VerifyDeleted(verifyCtx, environment)
			verifyCancel()
			verification = observed
			if err != nil {
				errorCategories = append(errorCategories, "environment_absence_check_failed")
			}
		} else if !opts.DryRun {
			errorCategories = append(errorCategories, "environment_absence_check_unsupported")
		}
		evidence["absenceCheck"] = verification
		verificationComplete := verification.Supported && verification.Completed
		if opts.DryRun && !verification.Supported {
			verificationComplete = true
		}
		completed := len(errorCategories) == 0 && deleteCompleted && verificationComplete
		evidence["completed"] = completed
		if len(errorCategories) > 0 {
			evidence["errorCategories"] = errorCategories
		}
		if _, err := sink.WriteJSON("environment/post-run-cleanup.json", evidence); err != nil {
			return err
		}
		if !completed {
			return errors.New("post-run cleanup did not complete")
		}
		return nil
	}
	failAfterEnvironmentCreate := func(status RunStatus, err error) (Result, error) {
		captureCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_ = driver.CaptureState(captureCtx, environment, sink)
		cancel()
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("environment-failure-state-captured", "", "", map[string]any{"status": status, "errorCategory": "environment_failure"}))
		if cleanupErr := cleanupAfterEnvironment(retainEnvironmentOnFailure); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
			status = RunFailed
		}
		return finish(status, err)
	}
	if createErr != nil {
		return failAfterEnvironmentCreate(RunFailed, createErr)
	}
	if err := driver.WaitReady(ctx, environment); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
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
	endpointAddrs = endpointAddresses(endpoints)
	resources, err = provision.PlanScenario(scenario, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: opts.DryRun})
	if err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	resourcesPlanned = true
	if _, err := sink.WriteJSON("environment/scenario-resources.json", resources); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	needsActorEndpoints := len(resources.Assignments) > 0
	portForwardEnvironment := environment.Driver == "k3d"
	needsProvisioningEndpoint := portForwardEnvironment && len(endpointAddrs) > 0
	if (opts.ConsoleEndpoints || needsActorEndpoints || needsProvisioningEndpoint) && !opts.DryRun {
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
	provisionCtx, releaseProvision, err := reserveProviderOperation(ctx, budgetLedger)
	if err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	provisionErr := provision.ProvisionScenario(provisionCtx, &resources, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: provisionDryRun})
	provisionErr = errors.Join(provisionErr, releaseProvision())
	if provisionErr != nil {
		return failAfterEnvironmentCreate(RunFailed, provisionErr)
	}
	if _, err := sink.WriteJSON("environment/scenario-resources.json", resources); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if _, err := sink.WriteJSON("actors/assignments.json", resources.Assignments); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	recorder := &runRecorder{store: opts.Store, sink: sink, runID: runID, budget: budgetLedger, budgetCancel: budgetCancel}
	eventRuntime := opts.EventRuntime
	if eventRuntime == nil {
		eventRuntime = defaultEventRuntime(opts, driver, environment, scenario, &resources)
	}
	if cleaner, ok := eventRuntime.(interface{ CleanupSensitiveHostArtifacts() error }); ok {
		sensitiveCleanup = cleaner.CleanupSensitiveHostArtifacts
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
	captureRunFailureEvidence := func() {
		if !schedulerStopped {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
			_ = scheduler.Stop(stopCtx)
			stopCancel()
			schedulerStopped = true
		}
		evidenceCtx, evidenceCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer evidenceCancel()
		_ = captureClusterAssertionsOnce(evidenceCtx, sink, scenario, driver, environment, nodes)
		_ = writeFinalOracleReport(evidenceCtx, sink, recorder, resources, scenario.Assertions, !provisionDryRun)
	}

	for i := range result.Phases {
		select {
		case <-ctx.Done():
			if budgetErr := recorder.budgetError(); budgetErr != nil {
				_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
				captureRunFailureEvidence()
				return failAfterEnvironmentCreate(RunFailed, budgetErr)
			}
			_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
			captureRunFailureEvidence()
			return failAfterEnvironmentCreate(RunAborted, ctx.Err())
		default:
		}
		if err := Transition(&result.Phases[i], PhaseRunning, time.Now().UTC()); err != nil {
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		phaseSpec := scenario.Phases[i]
		phaseOracleStart := recorder.oracleSnapshot()
		_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-started", phaseSpec.Name, "", map[string]any{"dryRun": opts.DryRun}))
		if err := eventRuntime.ExecutePhaseEvents(ctx, phaseSpec, opts.DryRun, recorder); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			captureRunFailureEvidence()
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if err := scheduler.ApplyPhase(ctx, phaseSpec); err != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			captureRunFailureEvidence()
			return failAfterEnvironmentCreate(RunFailed, err)
		}
		if budgetErr := recorder.budgetError(); budgetErr != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			captureRunFailureEvidence()
			return failAfterEnvironmentCreate(RunFailed, budgetErr)
		}
		if opts.DryRun {
			_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-dry-run", phaseSpec.Name, "", map[string]any{"plannedDuration": phaseSpec.Duration.String()}))
		} else if phaseSpec.Duration.Duration > 0 {
			timer := time.NewTimer(phaseSpec.Duration.Duration)
			select {
			case <-ctx.Done():
				timer.Stop()
				if budgetErr := recorder.budgetError(); budgetErr != nil {
					_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
					captureRunFailureEvidence()
					return failAfterEnvironmentCreate(RunFailed, budgetErr)
				}
				_ = Transition(&result.Phases[i], PhaseAborted, time.Now().UTC())
				captureRunFailureEvidence()
				return failAfterEnvironmentCreate(RunAborted, ctx.Err())
			case <-timer.C:
			}
		}
		phaseOracleEnd := recorder.oracleSnapshot()
		progressErr := assertPhaseOracleProgress(phaseSpec.Assertions, phaseOracleStart, phaseOracleEnd, !provisionDryRun)
		if boolFromMap(phaseSpec.Assertions, "requireWorkloadProgress") && !provisionDryRun {
			_ = appendEvent(ctx, opts.Store, sink, runID, artifacts.EventNow("phase-workload-progress", phaseSpec.Name, "", map[string]any{
				"acknowledgedTransactions": phaseOracleEnd.AcknowledgedTransactions - phaseOracleStart.AcknowledgedTransactions,
				"successfulReads":          phaseOracleEnd.SuccessfulReads - phaseOracleStart.SuccessfulReads,
				"ok":                       progressErr == nil,
			}))
		}
		if progressErr != nil {
			_ = Transition(&result.Phases[i], PhaseFailed, time.Now().UTC())
			captureRunFailureEvidence()
			return failAfterEnvironmentCreate(RunFailed, progressErr)
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
	if budgetErr := recorder.budgetError(); budgetErr != nil {
		return failAfterEnvironmentCreate(RunFailed, budgetErr)
	}
	if err := runFinalClusterAssertions(ctx, sink, scenario, driver, environment, nodes, !provisionDryRun); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if err := writeFinalOracleReport(ctx, sink, recorder, resources, scenario.Assertions, !provisionDryRun); err != nil {
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	if err := provision.CleanupScenario(ctx, &resources, provision.Options{RunID: runID, DaemonAddrs: endpointAddrs, DryRun: provisionDryRun}); err != nil {
		_, _ = sink.WriteJSON("environment/scenario-resources.json", resources)
		return failAfterEnvironmentCreate(RunFailed, err)
	}
	scenarioCleanupCompleted = true
	_, _ = sink.WriteJSON("environment/scenario-resources.json", resources)
	if err := cleanupAfterEnvironment(false); err != nil {
		return finish(RunFailed, err)
	}
	return finish(RunPassed, nil)
}

func markLastPassedPhaseFailed(phases []PhaseState) {
	for index := range phases {
		if phases[index].Status == PhaseFailed || phases[index].Status == PhaseAborted {
			return
		}
	}
	for index := len(phases) - 1; index >= 0; index-- {
		if phases[index].Status == PhasePassed {
			phases[index].Status = PhaseFailed
			phases[index].FinishedAt = time.Now().UTC()
			return
		}
	}
}

func applyComposeImageOverride(environment *spec.EnvironmentSpec, image string) {
	if environment == nil || strings.TrimSpace(image) == "" || !strings.EqualFold(strings.TrimSpace(environment.Driver), "compose") {
		return
	}
	options := make(map[string]any, len(environment.Options)+1)
	for key, value := range environment.Options {
		options[key] = value
	}
	options["imageOverride"] = strings.TrimSpace(image)
	environment.Options = options
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

func defaultEventRuntime(opts Options, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, resources *provision.ScenarioResources) events.Runtime {
	if opts.DryRun {
		return events.LocalRuntime{}
	}
	return events.NewDriverRuntimeWithScenario(driver, environment, scenario, resources)
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
		resolved, err := catalog.ResolveScenarioFile(scenarioPath, catalog.ResolveOptions{ProfileDirs: opts.ProfileDirs, ProviderProfileOverride: opts.ProviderProfileOverride, ProviderDryRun: opts.DryRun, ProviderManifest: opts.ProviderManifest})
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

func evidenceSafeResolvedScenario(scenario spec.ResolvedScenario) spec.ResolvedScenario {
	copy := scenario
	copy.Phases = append([]spec.PhaseSpec(nil), scenario.Phases...)
	for phaseIndex := range copy.Phases {
		copy.Phases[phaseIndex].Events = append([]spec.EventSpec(nil), scenario.Phases[phaseIndex].Events...)
		for eventIndex := range copy.Phases[phaseIndex].Events {
			event := &copy.Phases[phaseIndex].Events[eventIndex]
			if event.Type != "procedure-inference-node-create" && event.Type != "system-backup-fixture" {
				continue
			}
			target := make(map[string]any, len(event.Target))
			for key, value := range event.Target {
				if key == "text" || key == "nodeId" || key == "blobPayload" {
					continue
				}
				target[key] = value
			}
			target["graphContentRedacted"] = true
			event.Target = target
		}
	}
	return copy
}

func writeInitialArtifacts(sink *artifacts.Sink, scenario spec.ResolvedScenario, buildIdentity harnessBuildIdentity) error {
	if _, err := sink.WriteJSON("harness-build.json", buildIdentity); err != nil {
		return err
	}
	if _, err := sink.WriteJSON("resolved-scenario.json", evidenceSafeResolvedScenario(scenario)); err != nil {
		return err
	}
	if scenario.ProviderProfile != nil {
		if _, err := sink.WriteJSON("provider/provider-profile-resolved.json", scenario.ProviderProfile); err != nil {
			return err
		}
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

func currentHarnessBuildIdentity() harnessBuildIdentity {
	identity := harnessBuildIdentity{ExecutableSHA256: currentExecutableSHA256()}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return identity
	}
	identity.BuildInfoAvailable = true
	identity.GoVersion = info.GoVersion
	identity.ModulePath = info.Main.Path
	identity.ModuleVersion = info.Main.Version
	identity.ModuleSum = info.Main.Sum
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs", "vcs.revision", "vcs.time", "vcs.modified":
			if identity.VCS == nil {
				identity.VCS = map[string]string{}
			}
			identity.VCS[setting.Key] = setting.Value
		}
	}
	return identity
}

func requiresImmutableBuildProvenance(scenario spec.ResolvedScenario) bool {
	return scenario.ProcedureInference != nil
}

func validHarnessBuildIdentity(identity harnessBuildIdentity) bool {
	if len(identity.ExecutableSHA256) != sha256.Size*2 {
		return false
	}
	if _, err := hex.DecodeString(identity.ExecutableSHA256); err != nil {
		return false
	}
	revision := strings.TrimSpace(identity.VCS["vcs.revision"])
	if len(revision) != 40 {
		return false
	}
	if _, err := hex.DecodeString(revision); err != nil {
		return false
	}
	return identity.VCS["vcs"] == "git" && identity.VCS["vcs.modified"] == "false"
}

func currentExecutableSHA256() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ""
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func writeFinalOracleReport(ctx context.Context, sink *artifacts.Sink, recorder *runRecorder, resources provision.ScenarioResources, assertions map[string]any, live bool) error {
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
		RequiredAssertions:       finalOracleAssertionOptionsFrom(assertions),
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
	evaluateFinalOracleAssertions(&report, resources, live)
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
			fmt.Fprintf(&b, "- `%s`: expected nodes=%d, actual nodes=%d, expected edges=%d, actual edges=%d, ok=%t", check.ActorID, check.Expected.Nodes, check.Actual.Nodes, check.Expected.Edges, check.Actual.Edges, check.OK)
			if check.Error != "" {
				fmt.Fprintf(&b, ", error=%s", check.Error)
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	fmt.Fprintf(&b, "\n- Observation OK: %t\n", report.OK)
	fmt.Fprintf(&b, "- Required assertions OK: %t\n", report.AssertionsOK)
	if report.AssertionsSkipped {
		fmt.Fprintf(&b, "- Required assertions skipped: live checks are disabled\n")
	}
	for _, assertionErr := range report.AssertionErrors {
		fmt.Fprintf(&b, "- Assertion error: %s\n", assertionErr)
	}
	if _, err := sink.WriteText("oracle/final-report.md", b.String()); err != nil {
		return err
	}
	if live && !report.AssertionsOK {
		return fmt.Errorf("final oracle assertions failed: %s", strings.Join(report.AssertionErrors, "; "))
	}
	return nil
}

type finalOracleReport struct {
	AcknowledgedTransactions int                         `json:"acknowledgedTransactions"`
	ExpectedCounts           oracle.Counts               `json:"expectedCounts"`
	ExpectedCountsByActor    map[string]graphCounts      `json:"expectedCountsByActor,omitempty"`
	LiveChecks               []finalOracleLiveCheck      `json:"liveChecks,omitempty"`
	ReadChecks               int                         `json:"readChecks"`
	FailedReads              int                         `json:"failedReads"`
	FailedWrites             int                         `json:"failedWrites"`
	OK                       bool                        `json:"ok"`
	RequiredAssertions       finalOracleAssertionOptions `json:"requiredAssertions"`
	AssertionsOK             bool                        `json:"assertionsOk"`
	AssertionsSkipped        bool                        `json:"assertionsSkipped,omitempty"`
	AssertionErrors          []string                    `json:"assertionErrors,omitempty"`
}

type finalOracleAssertionOptions struct {
	RequireConvergedCounts                    bool `json:"requireConvergedCounts"`
	RequireNoAcknowledgedGraphTransactionLoss bool `json:"requireNoAcknowledgedGraphTransactionLoss"`
}

func finalOracleAssertionOptionsFrom(assertions map[string]any) finalOracleAssertionOptions {
	final, _ := assertions["final"].(map[string]any)
	return finalOracleAssertionOptions{
		RequireConvergedCounts:                    boolFromMap(final, "requireConvergedCounts"),
		RequireNoAcknowledgedGraphTransactionLoss: boolFromMap(final, "requireNoAcknowledgedGraphTransactionLoss"),
	}
}

func (o finalOracleAssertionOptions) required() bool {
	return o.RequireConvergedCounts || o.RequireNoAcknowledgedGraphTransactionLoss
}

func evaluateFinalOracleAssertions(report *finalOracleReport, resources provision.ScenarioResources, live bool) {
	report.AssertionsOK = true
	if !report.RequiredAssertions.required() {
		return
	}
	if !live {
		report.AssertionsSkipped = true
		return
	}
	expectedChecks := 0
	for _, assignment := range resources.Assignments {
		if assignment.BehaviorType == "graph-transaction" && assignment.SpaceID != "" && assignment.DomainID != "" && assignment.DaemonAddr != "" {
			expectedChecks++
		}
	}
	if len(report.LiveChecks) != expectedChecks || expectedChecks == 0 {
		report.AssertionErrors = append(report.AssertionErrors, fmt.Sprintf("live graph checks=%d, expected graph assignments=%d", len(report.LiveChecks), expectedChecks))
	}
	for _, check := range report.LiveChecks {
		if check.OK && check.Error == "" && check.Actual == check.Expected {
			continue
		}
		message := fmt.Sprintf("actor %s graph counts differ: expected nodes=%d edges=%d, actual nodes=%d edges=%d", check.ActorID, check.Expected.Nodes, check.Expected.Edges, check.Actual.Nodes, check.Actual.Edges)
		if check.Error != "" {
			message += ": " + check.Error
		}
		report.AssertionErrors = append(report.AssertionErrors, message)
	}
	report.AssertionsOK = len(report.AssertionErrors) == 0
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
		edges, edgeErr := queryEdgeCount(ctx, client, assignment.SpaceID, assignment.DomainID, expected.Edges)
		_ = client.Close()
		check.Actual = graphCounts{Nodes: int(nodes), Edges: int(edges)}
		switch {
		case nodeErr != nil && edgeErr != nil:
			check.Error = fmt.Sprintf("node count: %v; edge count: %v", nodeErr, edgeErr)
		case nodeErr != nil:
			check.Error = "node count: " + nodeErr.Error()
		case edgeErr != nil:
			check.Error = "edge count: " + edgeErr.Error()
		default:
			check.OK = check.Actual == check.Expected
		}
		checks = append(checks, check)
	}
	return checks
}

func queryEdgeCount(ctx context.Context, client *mycel.Client, spaceID, domainID string, expected int) (int64, error) {
	sessionID, err := client.OpenSession(ctx, spaceID, domainID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = client.CloseSession(ctx, sessionID) }()
	txID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = client.CloseTransaction(ctx, txID)
		}
	}()
	var count int64
	pageToken := ""
	seenTokens := map[string]struct{}{}
	for {
		remaining := int64(expected+1) - count
		if remaining <= 0 {
			break
		}
		pageSize := int32(1000)
		if remaining < int64(pageSize) {
			pageSize = int32(remaining)
		}
		callCtx, cancel := client.AuthCallContext(ctx)
		response, listErr := client.Graph.ListEdges(callCtx, &clientv1.ListEdgesRequest{TransactionId: txID, PageSize: pageSize, PageToken: pageToken})
		cancel()
		if listErr != nil {
			return 0, listErr
		}
		count += int64(len(response.GetEdges()))
		next := response.GetNextPageToken()
		if next == "" {
			break
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return 0, fmt.Errorf("list edges returned repeated page token")
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	if err := client.CloseTransaction(ctx, txID); err != nil {
		return 0, err
	}
	closed = true
	return count, nil
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
		if result.Phases[i].Status == PhasePending || result.Phases[i].Status == PhaseRunning {
			_ = Transition(&result.Phases[i], PhaseAborted, result.FinishedAt)
		}
	}
	if runErr != nil {
		if redactor, ok := sink.Sanitizer().(*provider.Redactor); ok {
			runErr = errors.New(redactor.SanitizeText(runErr.Error()))
		}
	}
	var artifactPaths []string
	writeResultAndSummary := func() error {
		artifactPaths = artifactPaths[:0]
		path, err := sink.WriteJSON("result.json", result)
		if err != nil {
			return fmt.Errorf("write result artifact: %w", err)
		}
		artifactPaths = append(artifactPaths, path)
		summary := report.RunSummary{RunID: result.RunID, ScenarioName: result.ScenarioName, Status: string(result.Status), StartedAt: result.StartedAt, FinishedAt: result.FinishedAt, DryRun: result.DryRun, Artifacts: append([]string(nil), artifactPaths...)}
		for _, phase := range result.Phases {
			actualDuration := time.Duration(0)
			if !phase.StartedAt.IsZero() && !phase.FinishedAt.IsZero() {
				actualDuration = phase.FinishedAt.Sub(phase.StartedAt).Round(time.Millisecond)
			}
			summary.Phases = append(summary.Phases, report.PhaseSummary{Name: phase.Name, Status: string(phase.Status), PlannedDuration: phase.Duration, ActualDuration: actualDuration})
		}
		path, err = sink.WriteText("summary.md", report.Markdown(summary))
		if err != nil {
			return fmt.Errorf("write summary artifact: %w", err)
		}
		artifactPaths = append(artifactPaths, path)
		return nil
	}
	if err := writeResultAndSummary(); err != nil {
		runErr = errors.Join(runErr, err)
		result.Status = RunFailed
	}
	redactor, _ := sink.Sanitizer().(*provider.Redactor)
	scanner, scannerErr := provider.NewScanner(redactor, provider.DefaultScanLimits())
	var scanReport provider.ScanReport
	if scannerErr == nil {
		scanReport, scannerErr = scanner.Scan(sink.Root())
	}
	if scannerErr != nil {
		scanReport.OK = false
		if quarantineErr := quarantineArtifactRoot(sink); quarantineErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("quarantine unscannable artifact root: %w", quarantineErr))
		}
		runErr = errors.Join(runErr, fmt.Errorf("artifact redaction scan failed: %w", scannerErr))
		result.Status = RunFailed
	} else if !scanReport.OK {
		if quarantineErr := quarantineUnsafeArtifacts(sink, scanReport.Findings); quarantineErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("quarantine unsafe artifacts: %w", quarantineErr))
		}
		runErr = errors.Join(runErr, fmt.Errorf("artifact redaction scan found %d prohibited value(s)", len(scanReport.Findings)))
		result.Status = RunFailed
	}
	var scanArtifactPath string
	if path, err := sink.WriteJSON("redaction-scan.json", scanReport); err == nil {
		scanArtifactPath = path
		artifactPaths = append(artifactPaths, path)
	} else {
		runErr = errors.Join(runErr, fmt.Errorf("write redaction scan report: %w", err))
		result.Status = RunFailed
	}
	if result.Status != status {
		if err := writeResultAndSummary(); err != nil {
			runErr = errors.Join(runErr, err)
		}
		if scanArtifactPath != "" {
			artifactPaths = append(artifactPaths, scanArtifactPath)
		}
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

type phaseOracleSnapshot struct {
	AcknowledgedTransactions int
	SuccessfulReads          int
}

func assertPhaseOracleProgress(assertions map[string]any, before, after phaseOracleSnapshot, live bool) error {
	if !boolFromMap(assertions, "requireWorkloadProgress") || !live {
		return nil
	}
	var failures []string
	if delta := after.AcknowledgedTransactions - before.AcknowledgedTransactions; delta <= 0 {
		failures = append(failures, "no graph transactions were acknowledged during the phase")
	}
	if delta := after.SuccessfulReads - before.SuccessfulReads; delta <= 0 {
		failures = append(failures, "no graph reads succeeded during the phase")
	}
	if len(failures) > 0 {
		return fmt.Errorf("phase workload progress assertion failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

type runRecorder struct {
	store        store.Store
	sink         *artifacts.Sink
	runID        string
	budget       *provider.Ledger
	budgetCancel context.CancelFunc

	mu              sync.Mutex
	budgetErr       error
	acknowledged    []oracle.Transaction
	expectedByActor map[string]graphCounts
	readChecks      []map[string]any
	successfulReads int
	failedWrites    int
}

func reserveProviderOperation(ctx context.Context, ledger *provider.Ledger) (context.Context, func() error, error) {
	if ledger == nil {
		return ctx, func() error { return nil }, nil
	}
	reservation, err := ledger.Reserve(ctx, provider.Demand{Operations: 1})
	if err != nil {
		return nil, nil, err
	}
	requestCtx, cancel := ledger.RequestContext(ctx)
	return requestCtx, func() error {
		cancel()
		cause := context.Cause(requestCtx)
		reservation.Release()
		switch {
		case errors.Is(cause, provider.ErrRequestTimeout):
			ledger.Close("operation deadline reached")
			return fmt.Errorf("%w: operation deadline reached", provider.ErrBudgetExhausted)
		case errors.Is(cause, provider.ErrTotalTimeout):
			ledger.Close("total timeout reached")
			return fmt.Errorf("%w: total timeout reached", provider.ErrBudgetExhausted)
		default:
			return nil
		}
	}, nil
}

func (r *runRecorder) reserveOperation(ctx context.Context) (context.Context, func(), error) {
	operationCtx, release, err := reserveProviderOperation(ctx, r.budget)
	if err != nil {
		r.recordBudgetError(err)
		return nil, nil, err
	}
	return operationCtx, func() {
		if err := release(); err != nil {
			r.recordBudgetError(err)
		}
	}, nil
}

func (r *runRecorder) recordBudgetError(err error) {
	r.mu.Lock()
	if r.budgetErr == nil {
		r.budgetErr = err
	}
	r.mu.Unlock()
	if r.budgetCancel != nil {
		r.budgetCancel()
	}
}

func (r *runRecorder) ReserveActorOperation(ctx context.Context, _, _ string) (context.Context, func(), error) {
	return r.reserveOperation(ctx)
}

func (r *runRecorder) ReserveRuntimeOperation(ctx context.Context, _, _ string) (context.Context, func(), error) {
	return r.reserveOperation(ctx)
}

func (r *runRecorder) budgetError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.budgetErr
}

func (r *runRecorder) oracleSnapshot() phaseOracleSnapshot {
	if r == nil {
		return phaseOracleSnapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return phaseOracleSnapshot{AcknowledgedTransactions: len(r.acknowledged), SuccessfulReads: r.successfulReads}
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
		if ok, _ := payload["ok"].(bool); ok {
			r.successfulReads++
		}
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

func quarantineArtifactRoot(sink *artifacts.Sink) error {
	if err := os.RemoveAll(sink.Root()); err != nil {
		return err
	}
	return os.MkdirAll(sink.Root(), 0o700)
}

func quarantineUnsafeArtifacts(sink *artifacts.Sink, findings []provider.Finding) error {
	seen := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		name := filepath.FromSlash(finding.Path)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		path := filepath.Join(sink.Root(), name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if _, err := sink.WriteText(name, "[REDACTED: unsafe artifact removed]\n"); err != nil {
			return err
		}
	}
	return nil
}

func appendMetric(ctx context.Context, st store.Store, sink *artifacts.Sink, runID, name string, value float64, labels map[string]any) error {
	if labels == nil {
		labels = map[string]any{}
	}
	clean, err := sink.Sanitize(labels)
	if err != nil {
		return err
	}
	labels, _ = clean.(map[string]any)
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

func runBoundedCleanupAttempts(maxAttempts int, timeout time.Duration, cleanup func(context.Context) error) (int, error) {
	if maxAttempts < 1 {
		return 0, errors.New("cleanup attempts must be positive")
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), timeout)
		lastErr = cleanup(attemptCtx)
		cancel()
		if lastErr == nil {
			return attempt, nil
		}
	}
	return maxAttempts, lastErr
}

func appendEvent(ctx context.Context, st store.Store, sink *artifacts.Sink, runID string, event artifacts.Event) error {
	clean, err := sink.Sanitize(event.Payload)
	if err != nil {
		return err
	}
	event.Payload = clean
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

func sanitizedJSON(sink *artifacts.Sink, value any) ([]byte, error) {
	clean, err := sink.Sanitize(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(clean)
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
