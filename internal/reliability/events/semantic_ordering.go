package events

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
)

type SemanticOrderingEvidence struct {
	SchemaVersion int                         `json:"schemaVersion"`
	OlderRevision uint64                      `json:"olderRevision"`
	NewerCommit   SemanticGraphMutationResult `json:"newerCommit"`
	GateSequence  int                         `json:"gateSequence"`
	OlderSkipped  int32                       `json:"olderSkipped"`
	BeforeRelease SemanticSearchEvidence      `json:"beforeRelease"`
	AfterRelease  SemanticSearchEvidence      `json:"afterRelease"`
}

type orderingGate func(context.Context, string) (emulator.GateState, error)

// runSemanticOrdering requires a real happens-before witness: an older request
// must be waiting while a newer publication is proved. Sleeps are not evidence.
// All callbacks share one deadline. Failed/ambiguous mutations are never retried.
func runSemanticOrdering(ctx context.Context, poll time.Duration, gate orderingGate, older func(context.Context) error, newer func(context.Context) error) (sequence int, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- older(ctx) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
				err = errors.Join(err, errors.New("older publication did not stop within cleanup bound"))
			}
		}
	}()
	for observations := 0; ; observations++ {
		if observations >= maxSemanticMaintenanceObservations {
			return 0, errors.New("gate observation limit exceeded")
		}
		select {
		case <-done:
			joined = true
			return 0, errors.New("older request finished before its controlled boundary")
		default:
		}
		state, controlErr := gate(ctx, "get")
		if controlErr != nil {
			return 0, controlErr
		}
		if state.State == "waiting" && state.Sequence > 0 {
			sequence = state.Sequence
			break
		}
		if state.State != "idle" || state.Sequence != 0 {
			return 0, errors.New("gate is not a valid pending boundary")
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	if err := newer(ctx); err != nil {
		return sequence, err
	}
	// Detect cancellation/timeout/release during the newer publication rather
	// than accepting an older request which was no longer held.
	state, err := gate(ctx, "get")
	if err != nil {
		return sequence, err
	}
	if state.State != "waiting" || state.Sequence != sequence {
		return sequence, errors.New("older response escaped before newer publication was proved")
	}
	state, err = gate(ctx, "release")
	if err != nil {
		return sequence, err
	}
	if state.State != "released" || state.Sequence != sequence {
		return sequence, errors.New("gate release identity changed")
	}
	select {
	case err = <-done:
		joined = true
		return sequence, err
	case <-ctx.Done():
		return sequence, ctx.Err()
	}
}

func controlledProviderGate(controller env.ProviderEmulatorController, environment env.Environment, name string) orderingGate {
	return func(ctx context.Context, action string) (emulator.GateState, error) {
		output, err := controller.ProviderEmulatorGate(ctx, environment, name, action)
		if err != nil || len(output.Stdout) > 4096 {
			return emulator.GateState{}, errors.New("provider gate control failed")
		}
		decoder := json.NewDecoder(strings.NewReader(output.Stdout))
		decoder.DisallowUnknownFields()
		var state emulator.GateState
		if decoder.Decode(&state) != nil {
			return state, errors.New("provider gate evidence is malformed")
		}
		var trailing any
		if !errors.Is(decoder.Decode(&trailing), io.EOF) || state.SchemaVersion != 1 || state.Name != name || state.Sequence < 0 {
			return state, errors.New("provider gate evidence has invalid version, identity, or boundary")
		}
		// Re-encoding rejects duplicate fields, aliases, and hidden evidence.
		canonical, _ := json.Marshal(state)
		// Gate CLI emits exactly this canonical object plus a newline.
		if strings.TrimSpace(output.Stdout) != string(canonical) {
			return state, errors.New("provider gate evidence is noncanonical")
		}
		return state, nil
	}
}

func executeSemanticOrdering(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (SemanticOrderingEvidence, error) {
	result := SemanticOrderingEvidence{SchemaVersion: 1}
	if err := spec.ValidateSemanticEventTarget("semantic-ordering-assert", target); err != nil {
		return result, err
	}
	controller, ok := driver.(env.ProviderEmulatorController)
	if !ok || environment.Metadata["providerEmulator"] != "true" || resources == nil || resources.Semantic == nil || resources.Semantic.LiveProvider {
		return result, errors.New("ordering requires a credential-free controlled emulator")
	}
	semantic := resources.Semantic
	timeout := durationTarget(target, "timeout", semantic.MaintenanceTimeout)
	poll := durationTarget(target, "pollInterval", semantic.MaintenancePollInterval)
	if timeout <= 0 || poll <= 0 || poll > timeout {
		return result, errors.New("ordering wait must be bounded")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	revision, err := capturedSemanticWorkRevision(semantic, targetString(target, "expectRevisionFrom"))
	if err != nil {
		return result, err
	}
	result.OlderRevision = revision
	recordCapture := targetString(target, "captureRecordAs")
	if _, exists := semantic.EvidenceValues[recordCapture]; exists {
		return result, errors.New("ordering capture must be new")
	}
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: resources.AdminAddr, Username: firstNonEmpty(resources.AdminUsername, provision.DefaultAdminUsername), Password: firstNonEmpty(resources.AdminPassword, provision.DefaultAdminPassword), ClientName: "mycel-lab-ordering"})
	if err != nil {
		return result, errors.New("ordering admin authentication failed")
	}
	defer admin.Close()
	callCtx, stop := admin.AuthCallContext(ctx)
	status, err := admin.SemanticMaintenance.GetSemanticReliabilityStatus(callCtx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: semantic.SpaceID, SemanticRuleId: semantic.SemanticRuleID, EmbeddingBindingKey: semantic.EmbeddingBindingKey})
	stop()
	if err != nil || status == nil || status.GetSpaceId() != semantic.SpaceID || status.GetSemanticRuleId() != semantic.SemanticRuleID || status.GetEmbeddingBindingKey() != semantic.EmbeddingBindingKey || status.GetLatestObservedGraphRevision() != revision || status.GetFreshThroughGraphRevision() != revision || status.GetSemanticGeneration() == 0 || status.GetCapacityState() != "ok" || status.GetDegraded() || status.GetRebuildRequired() {
		return result, errors.New("ordering requires a certified fresh baseline at the captured revision")
	}
	older := func(callCtx context.Context) error {
		authenticated, stop := admin.AuthCallContext(callCtx)
		defer stop()
		response, err := admin.SemanticMaintenance.BackfillSemanticRule(authenticated, &adminv1.BackfillSemanticRuleRequest{SpaceId: semantic.SpaceID, SemanticRuleId: semantic.SemanticRuleID, EmbeddingBindingKey: semantic.EmbeddingBindingKey, NodeIds: []string{targetString(target, "nodeId")}, Force: true, Limit: 1, ContinueOnError: false})
		if err != nil {
			return errors.New("older backfill RPC failed")
		}
		if response == nil || response.GetSemanticRuleId() != semantic.SemanticRuleID || response.GetEmbeddingBindingKey() != semantic.EmbeddingBindingKey || response.GetSelectedCount() != 1 || response.GetGeneratedCount() != 0 || response.GetFailedCount() != 0 || response.GetSkippedCount() != 1 || len(response.GetSkipped()) != 1 || response.GetSkipped()[0].GetNodeId() != targetString(target, "nodeId") || response.GetSkipped()[0].GetReason() != "newer semantic publication already exists" {
			return errors.New("older backfill was not explicitly superseded by the newer publication")
		}
		return nil
	}
	searchTarget := map[string]any{"query": targetString(target, "text"), "expectedResultCount": 1, "expectedNodeIds": []any{targetString(target, "nodeId")}, "minimumScore": 0.99, "requireProvenance": true, "expectSourceRevisionFrom": targetString(target, "captureRevisionAs"), "timeout": timeout.String(), "pollInterval": poll.String()}
	newer := func(callCtx context.Context) error {
		commit, err := executeSemanticGraphMutation(callCtx, resources, "semantic-node-update", map[string]any{"nodeId": targetString(target, "nodeId"), "text": targetString(target, "text"), "captureRevisionAs": targetString(target, "captureRevisionAs")})
		if err != nil {
			return errors.New("newer graph commit failed")
		}
		result.NewerCommit = commit
		if commit.CommittedRevision <= 0 || uint64(commit.CommittedRevision) <= revision {
			return errors.New("newer commit did not advance the graph")
		}
		if _, err := awaitSemanticMaintenance(callCtx, admin, semantic, map[string]any{"timeout": timeout.String(), "pollInterval": poll.String()}); err != nil {
			return errors.New("newer maintenance did not converge while older response was held")
		}
		searchTarget["captureRecordAs"] = recordCapture
		result.BeforeRelease, err = executeSemanticSearchAssertion(callCtx, resources, searchTarget)
		if err != nil {
			return errors.New("newer publication was not proved before gate release")
		}
		if len(result.BeforeRelease.Results) != 1 || result.BeforeRelease.Results[0].SemanticGeneration != status.GetSemanticGeneration() {
			return errors.New("newer publication changed semantic generation")
		}
		return nil
	}
	result.GateSequence, err = runSemanticOrdering(ctx, poll, controlledProviderGate(controller, environment, targetString(target, "gate")), older, newer)
	if err != nil {
		return result, err
	}
	result.OlderSkipped = 1
	delete(searchTarget, "captureRecordAs")
	searchTarget["expectRecordFrom"] = recordCapture
	result.AfterRelease, err = executeSemanticSearchAssertion(ctx, resources, searchTarget)
	if err != nil {
		return result, errors.New("late older completion changed the newer semantic record")
	}
	return result, nil
}
