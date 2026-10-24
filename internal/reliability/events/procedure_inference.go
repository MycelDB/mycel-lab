package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

const (
	procedureInferenceRequested = 1
	procedureInferenceSucceeded = 4
	procedureInferenceFailed    = 5
	procedureInferenceDenied    = 6
	procedureInferenceCanceled  = 7
	procedureInferenceAmbiguous = 8
)

type ProcedureInferenceResult struct {
	Operation                      string   `json:"operation"`
	ProcedureID                    string   `json:"procedureId"`
	BindingID                      string   `json:"bindingId"`
	Configured                     bool     `json:"configured,omitempty"`
	NodeID                         string   `json:"nodeId,omitempty"`
	CommitID                       string   `json:"commitId,omitempty"`
	CommittedRevision              int64    `json:"committedRevision,omitempty"`
	AttemptCount                   int      `json:"attemptCount"`
	TerminalCount                  int      `json:"terminalCount"`
	SucceededCount                 int      `json:"succeededCount"`
	FailedCount                    int      `json:"failedCount"`
	DeniedCount                    int      `json:"deniedCount"`
	CanceledCount                  int      `json:"canceledCount"`
	AmbiguousCount                 int      `json:"ambiguousCount"`
	SucceededOutcomeCount          int      `json:"succeededOutcomeCount"`
	AuthorizationDeniedCount       int      `json:"authorizationDeniedOutcomeCount"`
	CanceledBeforeProviderCount    int      `json:"canceledBeforeProviderOutcomeCount"`
	FailedBeforeProviderCount      int      `json:"failedBeforeProviderOutcomeCount"`
	ProviderUnknownCount           int      `json:"providerOutcomeUnknownCount"`
	MissingOutcomeCount            int      `json:"missingOutcomeCount"`
	UnknownOutcomeCount            int      `json:"unknownOutcomeCount"`
	ChangeCount                    int      `json:"changeCount"`
	HighWaterSequence              uint64   `json:"highWaterSequence,omitempty"`
	EarliestRetainedSequence       uint64   `json:"earliestRetainedSequence,omitempty"`
	EarliestStateSequence          uint64   `json:"earliestRetainedStateSequence,omitempty"`
	SnapshotContinuityProved       bool     `json:"snapshotContinuityProved,omitempty"`
	StableEventIDsProved           bool     `json:"stableEventIdsProved,omitempty"`
	RunReferencesProved            bool     `json:"runReferencesProved,omitempty"`
	SanitizedEvidenceProved        bool     `json:"sanitizedEvidenceProved,omitempty"`
	ControlAction                  string   `json:"controlAction,omitempty"`
	ControlEpoch                   uint64   `json:"controlEpoch,omitempty"`
	AffectedPartitions             []uint32 `json:"affectedPartitions,omitempty"`
	AcknowledgedPartitions         []uint32 `json:"acknowledgedPartitions,omitempty"`
	ControlCompleted               bool     `json:"controlCompleted,omitempty"`
	AuthorizationDenialsProved     bool     `json:"authorizationDenialsProved,omitempty"`
	CapacityBoundsProved           bool     `json:"capacityBoundsProved,omitempty"`
	PressureDiagnosticsProved      bool     `json:"pressureDiagnosticsProved,omitempty"`
	DiagnosticRecordCount          int      `json:"diagnosticRecordCount,omitempty"`
	ConvergenceCategory            string   `json:"convergenceCategory,omitempty"`
	AttemptValidationCategory      string   `json:"attemptValidationCategory,omitempty"`
	ObservedPublicationDisposition string   `json:"observedPublicationDisposition,omitempty"`
	CheckpointProved               bool     `json:"checkpointProved,omitempty"`
}

type procedureInferenceCheckpoint struct {
	Attempts                      map[string]string
	Events                        map[uint64]string
	SnapshotHighWaterSequence     uint64
	ChangeHighWaterSequence       uint64
	EarliestRetainedSequence      uint64
	EarliestRetainedStateSequence uint64
}

type procedureInferenceAttempt struct {
	ID                     string          `json:"id"`
	RecordVersion          uint64          `json:"record_version"`
	SpaceID                string          `json:"space_id"`
	DomainID               string          `json:"domain_id"`
	ProcedureID            string          `json:"procedure_id"`
	ProcedureVersion       int32           `json:"procedure_version"`
	BindingID              string          `json:"binding_id"`
	BindingVersion         int32           `json:"binding_version"`
	InvocationID           string          `json:"invocation_id"`
	RunID                  string          `json:"run_id"`
	RunAttemptNumber       int32           `json:"run_attempt_number"`
	WorkflowStepID         string          `json:"workflow_step_id"`
	ProviderAttemptNumber  int32           `json:"provider_attempt_number"`
	SourceEventID          string          `json:"source_event_id"`
	SourceTransactionID    string          `json:"source_transaction_id"`
	SourceGraphRevision    uint64          `json:"source_graph_revision"`
	EventOriginPrincipalID string          `json:"event_origin_principal_id"`
	ActorPrincipalID       string          `json:"actor_principal_id"`
	OwnerPrincipalID       string          `json:"owner_principal_id"`
	OnBehalfOfPrincipalID  string          `json:"on_behalf_of_principal_id"`
	Operation              int32           `json:"operation"`
	ProfileID              string          `json:"profile_id"`
	EndpointID             string          `json:"endpoint_id"`
	ModelID                string          `json:"model_id"`
	CapabilityID           string          `json:"capability_id"`
	PolicyDecisionID       string          `json:"policy_decision_id"`
	CredentialGrantID      string          `json:"credential_grant_id"`
	AuthorizationRevision  uint64          `json:"authorization_revision"`
	ConfigurationRevision  uint64          `json:"configuration_revision"`
	State                  int32           `json:"state"`
	OutcomeCategory        string          `json:"outcome_category"`
	InputTokens            int64           `json:"input_tokens"`
	OutputTokens           int64           `json:"output_tokens"`
	TotalTokens            int64           `json:"total_tokens"`
	TokenCountAuthority    string          `json:"token_count_authority"`
	ProviderDurationMillis int64           `json:"provider_duration_millis"`
	PublicationDisposition string          `json:"publication_disposition"`
	RequestedAt            json.RawMessage `json:"requested_at"`
	AuthorizedAt           json.RawMessage `json:"authorized_at"`
	StartedAt              json.RawMessage `json:"started_at"`
	CompletedAt            json.RawMessage `json:"completed_at"`
}

type procedureInferenceList struct {
	Attempts                      []procedureInferenceAttempt `json:"attempts"`
	NextPageToken                 string                      `json:"next_page_token"`
	SnapshotHighWaterSequence     uint64                      `json:"snapshot_high_water_sequence"`
	EarliestRetainedStateSequence uint64                      `json:"earliest_retained_state_sequence"`
	SnapshotCursor                string                      `json:"snapshot_cursor"`
}

type procedureInferenceChange struct {
	EventID       string                    `json:"event_id"`
	SchemaVersion string                    `json:"schema_version"`
	SpaceID       string                    `json:"space_id"`
	Sequence      uint64                    `json:"sequence"`
	ChangeKind    string                    `json:"change_kind"`
	AttemptID     string                    `json:"attempt_id"`
	RecordVersion uint64                    `json:"record_version"`
	Attempt       procedureInferenceAttempt `json:"attempt"`
	OccurredAt    json.RawMessage           `json:"occurred_at"`
	CommittedAt   json.RawMessage           `json:"committed_at"`
}

type procedureInferenceChanges struct {
	Changes                       []procedureInferenceChange `json:"changes"`
	NextCursor                    string                     `json:"next_cursor"`
	HighWaterSequence             uint64                     `json:"high_water_sequence"`
	EarliestRetainedSequence      uint64                     `json:"earliest_retained_sequence"`
	EarliestRetainedStateSequence uint64                     `json:"earliest_retained_state_sequence"`
	HistoryGap                    bool                       `json:"history_gap"`
}

type procedureInferenceRunResponse struct {
	// Legacy fields are accepted at the transport boundary but never copied to
	// artifacts; assertions use only the typed sanitized run projection.
	RunJSON                string   `json:"run_json"`
	InferenceUsageEventIDs []string `json:"inference_usage_event_ids"`
	Run                    struct {
		ID                  string          `json:"id"`
		DomainID            string          `json:"domain_id"`
		InvocationID        string          `json:"invocation_id"`
		BindingID           string          `json:"binding_id"`
		BindingVersion      int32           `json:"binding_version"`
		ProcedureID         string          `json:"procedure_id"`
		ProcedureVersion    int32           `json:"procedure_version"`
		AttemptNumber       int32           `json:"attempt_number"`
		State               int32           `json:"state"`
		OutcomeCategory     string          `json:"outcome_category"`
		ActorPrincipalID    string          `json:"actor_principal_id"`
		OwnerPrincipalID    string          `json:"owner_principal_id"`
		OnBehalfPrincipalID string          `json:"on_behalf_of_principal_id"`
		EventOriginID       string          `json:"event_origin_principal_id"`
		InferenceProfile    string          `json:"inference_profile"`
		InferenceProfileID  string          `json:"inference_profile_id"`
		PolicyDecisionID    string          `json:"policy_decision_id"`
		EndpointID          string          `json:"endpoint_id"`
		ModelID             string          `json:"model_id"`
		CapabilityID        string          `json:"capability_id"`
		InputTokens         int64           `json:"input_tokens"`
		OutputTokens        int64           `json:"output_tokens"`
		TotalTokens         int64           `json:"total_tokens"`
		TokenAuthority      string          `json:"token_count_authority"`
		InferenceAttemptIDs []string        `json:"inference_attempt_ids"`
		StartedAt           json.RawMessage `json:"started_at"`
		CompletedAt         json.RawMessage `json:"completed_at"`
	} `json:"run"`
}

type procedureControlResponse struct {
	ProcedureJSON string                  `json:"procedure_json"`
	Control       procedureControlBarrier `json:"control"`
}

type procedureControlBarrier struct {
	SpaceID                string          `json:"space_id"`
	DomainID               string          `json:"domain_id"`
	ProcedureID            string          `json:"procedure_id"`
	ProcedureVersion       int32           `json:"procedure_version"`
	TargetStatus           int32           `json:"target_status"`
	ControlEpoch           uint64          `json:"control_epoch"`
	ControllingPrincipalID string          `json:"controlling_principal_id"`
	AffectedPartitions     []uint32        `json:"affected_partitions"`
	AcknowledgedPartitions []uint32        `json:"acknowledged_partitions"`
	Completed              bool            `json:"completed"`
	CompletionRevision     uint64          `json:"completion_revision"`
	CommittedAt            json.RawMessage `json:"committed_at"`
	CompletedAt            json.RawMessage `json:"completed_at"`
}

func executeProcedureInferenceEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, checkpoints map[string]procedureInferenceCheckpoint, eventType string, target map[string]any) (ProcedureInferenceResult, error) {
	if resources == nil || resources.ProcedureInference == nil {
		return ProcedureInferenceResult{}, errors.New("procedure inference resources are not provisioned")
	}
	configured := resources.ProcedureInference
	result := ProcedureInferenceResult{Operation: eventType, ProcedureID: configured.ProcedureID, BindingID: configured.BindingID}
	switch eventType {
	case "procedure-inference-setup":
		if err := setupProcedureInference(ctx, driver, environment, resources, target); err != nil {
			return result, err
		}
		result.Configured = true
		result.AuthorizationDenialsProved = true
		return result, nil
	case "procedure-inference-node-create":
		mutation, err := executeSemanticGraphMutation(ctx, resources, "semantic-node-create", map[string]any{
			"actorGroup": procedureInferenceTargetGroup(resources), "nodeId": target["nodeId"], "text": target["text"], "labels": stringSliceAny(configured.TargetLabels),
		})
		if err != nil {
			return result, err
		}
		// Node and commit identities are graph content and are intentionally not
		// copied into procedure-inference acceptance evidence.
		result.CommittedRevision = mutation.CommittedRevision
		return result, nil
	case "procedure-inference-await-terminal":
		return awaitProcedureInference(ctx, driver, environment, resources, checkpoints, target)
	case "procedure-inference-control":
		return controlProcedureInference(ctx, driver, environment, resources, target)
	case "procedure-inference-capacity-diagnostics-assert":
		return assertProcedureInferenceCapacityDiagnostics(ctx, environment, resources, target)
	default:
		return result, fmt.Errorf("unsupported procedure inference event %q", eventType)
	}
}

func setupProcedureInference(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) error {
	configured := resources.ProcedureInference
	writer, ok := procedureInferenceAssignment(resources, configured.TargetActorID)
	if !ok || writer.PrincipalID == "" {
		return errors.New("procedure inference writer assignment is incomplete")
	}
	auditor, ok := procedureInferenceAssignment(resources, configured.AuditActorID)
	if !ok || auditor.PrincipalID == "" || auditor.Username == "" || auditor.Password == "" {
		return errors.New("procedure inference audit assignment is incomplete")
	}
	procedure := map[string]any{
		"id": configured.ProcedureID, "name": "Mycel Lab procedure inference", "version": 1, "status": "enabled",
		"input":     map[string]any{"target": "changed", "mode": "fields", "fields": []string{"payload.text"}},
		"inference": map[string]any{"operation": "chat", "profile_id": configured.IntelligenceProfileID, "parameters": map[string]any{"responseFormat": "text", "maxInputTokens": 4096, "maxOutputTokens": 256}},
		"prompt":    "Return a bounded deterministic fixture result.",
		"output":    map[string]any{"mode": "text", "actions": []map[string]any{{"update_node": map[string]any{"target": "changed", "set": map[string]string{"payload.procedure_result": "$result.text"}}}}},
	}
	binding := map[string]any{
		"id": configured.BindingID, "name": "Mycel Lab procedure inference graph trigger", "version": 1,
		"procedure_id": configured.ProcedureID, "procedure_version": 1, "status": "enabled",
		"scope":       map[string]any{"space_id": configured.SpaceID, "domain_id": configured.DomainID},
		"trigger":     map[string]any{"type": "graph_event", "events": []string{"node.created"}, "labels": configured.TargetLabels},
		"runtime":     map[string]any{"actor_principal_id": "automation", "owner_principal_id": writer.PrincipalID, "on_behalf_of_principal_id": writer.PrincipalID, "inference_profile_id": configured.IntelligenceProfileID, "event_origin_override": "if_present"},
		"idempotency": map[string]any{"scope": "target", "target": "changed", "inputHashFields": []string{"payload.text"}, "skipIfOutputUnchanged": true},
	}
	procedureJSON, _ := json.Marshal(procedure)
	bindingJSON, _ := json.Marshal(binding)
	procedurePath := "/tmp/mycel-lab-procedure-inference-procedure.json"
	bindingPath := "/tmp/mycel-lab-procedure-inference-binding.json"
	base := procedureInferenceAdminCLI()
	controlBase := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", auditor.Username, "--password", auditor.Password, "--output", "json"}
	writeScript := "cat > " + shellQuote(procedurePath) + " <<'JSON'\n" + string(procedureJSON) + "\nJSON\n" +
		"cat > " + shellQuote(bindingPath) + " <<'JSON'\n" + string(bindingJSON) + "\nJSON\n"
	operationCtx, cancel := procedureInferenceTimeout(ctx, target, 30*time.Second)
	defer cancel()
	steps := []struct {
		category string
		command  []string
	}{
		{category: "fixture_write_failed", command: []string{"sh", "-ec", writeScript}},
		{category: "procedure_put_failed", command: append(append([]string(nil), base...), "automation", "procedure", "put", "--domain-id", configured.DomainID, procedurePath)},
		{category: "binding_put_failed", command: append(append([]string(nil), base...), "automation", "binding", "put", "--domain-id", configured.DomainID, bindingPath)},
		{category: "procedure_enable_failed", command: append(controlBase, "automation", "procedure", "enable", configured.ProcedureID, "--space-id", configured.SpaceID, "--domain-id", configured.DomainID)},
	}
	for _, step := range steps {
		if _, err := driver.Exec(operationCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: step.command}); err != nil {
			return fmt.Errorf("configure procedure inference fixture: %s", step.category)
		}
	}
	writerList := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", writer.Username, "--password", writer.Password, "--output", "json", "automation", "inference-attempt", "list", "--space-id", configured.SpaceID, "--page-size", "1"}
	denialCtx, denialCancel := context.WithTimeout(ctx, 10*time.Second)
	denied, denialErr := driver.Exec(denialCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: writerList})
	denialCancel()
	if !procedureInferencePermissionDenied(denied, denialErr) {
		return errors.New("ordinary graph writer attempt-read probe did not return permission denied")
	}
	writerControl := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", writer.Username, "--password", writer.Password, "--output", "json", "automation", "procedure", "disable", configured.ProcedureID, "--space-id", configured.SpaceID, "--domain-id", configured.DomainID}
	denialCtx, denialCancel = context.WithTimeout(ctx, 10*time.Second)
	denied, denialErr = driver.Exec(denialCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: writerControl})
	denialCancel()
	if !procedureInferencePermissionDenied(denied, denialErr) {
		return errors.New("ordinary graph writer control probe did not return permission denied")
	}
	return nil
}

func awaitProcedureInference(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, checkpoints map[string]procedureInferenceCheckpoint, target map[string]any) (ProcedureInferenceResult, error) {
	configured := resources.ProcedureInference
	result := ProcedureInferenceResult{Operation: "procedure-inference-await-terminal", ProcedureID: configured.ProcedureID, BindingID: configured.BindingID}
	auditor, ok := procedureInferenceAssignment(resources, configured.AuditActorID)
	if !ok || auditor.Username == "" || auditor.Password == "" {
		return result, errors.New("procedure inference audit assignment is incomplete")
	}
	timeout, err := targetDuration(target, "timeout")
	if err != nil {
		return result, err
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	interval, err := targetDuration(target, "interval")
	if err != nil {
		return result, err
	}
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	stableFor, err := targetDuration(target, "stableFor")
	if err != nil {
		return result, err
	}
	minimum := intTarget(target, "minimumAttempts", 1)
	maximum := intTarget(target, "maximumAttempts", 64)
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var stableSince time.Time
	var previousChangeDigests map[uint64]string
	for {
		iterationStable := false
		listRaw, commandErr := procedureInferenceCLIExec(waitCtx, driver, environment, auditor, []string{"automation", "inference-attempt", "list", "--space-id", configured.SpaceID, "--domain-id", configured.DomainID, "--procedure-id", configured.ProcedureID, "--page-size", "100"})
		if commandErr != nil && result.ConvergenceCategory == "" {
			result.ConvergenceCategory = "list_command_failed"
		}
		if commandErr == nil {
			var list procedureInferenceList
			decodeErr := decodeProcedureInferenceJSON(listRaw, &list)
			if decodeErr != nil && result.ConvergenceCategory == "" {
				result.ConvergenceCategory = "list_decode_failed"
			}
			if decodeErr == nil {
				terminal, succeeded, failed, denied, canceled, ambiguous := 0, 0, 0, 0, 0, 0
				succeededOutcome, authorizationDenied, canceledBeforeProvider, failedBeforeProvider, providerUnknown, missingOutcome, unknownOutcome := 0, 0, 0, 0, 0, 0, 0
				result.AttemptCount = len(list.Attempts)
				result.ConvergenceCategory = "snapshot_invalid"
				snapshotValid := validateProcedureInferenceSnapshot(list) == nil
				countValid := len(list.Attempts) >= minimum && len(list.Attempts) <= maximum
				valid := snapshotValid && countValid
				if snapshotValid {
					result.ConvergenceCategory = "attempt_count_out_of_bounds"
				}
				attemptsValid := true
				result.AttemptValidationCategory = ""
				result.ObservedPublicationDisposition = ""
				for _, attempt := range list.Attempts {
					if err := validateProcedureInferenceAttempt(attempt, configured, auditor, resources, true); err != nil {
						attemptsValid = false
						if result.AttemptValidationCategory == "" {
							result.AttemptValidationCategory = procedureInferenceAttemptValidationCategory(err)
						}
					}
					if attempt.State == procedureInferenceSucceeded && attempt.PublicationDisposition != "published" && result.ObservedPublicationDisposition == "" {
						result.ObservedPublicationDisposition = procedureInferenceObservedPublicationDisposition(attempt.PublicationDisposition)
					}
					if attempt.State >= procedureInferenceSucceeded && attempt.State <= procedureInferenceAmbiguous {
						terminal++
					}
					switch attempt.State {
					case procedureInferenceSucceeded:
						succeeded++
					case procedureInferenceFailed:
						failed++
					case procedureInferenceDenied:
						denied++
					case procedureInferenceCanceled:
						canceled++
					case procedureInferenceAmbiguous:
						ambiguous++
					}
					switch attempt.OutcomeCategory {
					case "":
						missingOutcome++
					case "succeeded":
						succeededOutcome++
					case "authorization_denied":
						authorizationDenied++
					case "canceled_before_provider":
						canceledBeforeProvider++
					case "failed_before_provider":
						failedBeforeProvider++
					case "provider_outcome_unknown":
						providerUnknown++
					default:
						unknownOutcome++
					}
				}
				if !attemptsValid {
					valid = false
					result.ConvergenceCategory = "attempt_invalid"
				}
				result.TerminalCount, result.SucceededCount, result.FailedCount, result.DeniedCount, result.CanceledCount, result.AmbiguousCount = terminal, succeeded, failed, denied, canceled, ambiguous
				result.SucceededOutcomeCount, result.AuthorizationDeniedCount, result.CanceledBeforeProviderCount, result.FailedBeforeProviderCount = succeededOutcome, authorizationDenied, canceledBeforeProvider, failedBeforeProvider
				result.ProviderUnknownCount, result.MissingOutcomeCount, result.UnknownOutcomeCount = providerUnknown, missingOutcome, unknownOutcome
				if valid && terminal < minimum {
					result.ConvergenceCategory = "terminal_pending"
				} else if valid && boolTarget(target, "requireSucceeded", true) && succeeded < minimum {
					result.ConvergenceCategory = "success_pending"
				}
				if valid && terminal >= minimum && (!boolTarget(target, "requireSucceeded", true) || succeeded >= minimum) {
					result.ConvergenceCategory = "lifecycle_validation_pending"
					runReferencesProved := true
					for _, attempt := range list.Attempts {
						runRaw, err := procedureInferenceCLIExec(waitCtx, driver, environment, auditor, []string{"automation", "run", "get", attempt.RunID, "--space-id", configured.SpaceID, "--domain-id", configured.DomainID})
						if err != nil {
							return result, err
						}
						var run procedureInferenceRunResponse
						if decodeProcedureInferenceJSON(runRaw, &run) != nil || !procedureInferenceRunReferencesAttempt(run, attempt, configured) {
							runReferencesProved = false
							break
						}
					}
					if !runReferencesProved {
						return result, errors.New("typed automation run does not preserve inference attempt references")
					}
					result.ConvergenceCategory = "change_feed_validation_pending"
					changesRaw, err := procedureInferenceCLIExec(waitCtx, driver, environment, auditor, []string{"automation", "inference-attempt", "changes", "--space-id", configured.SpaceID, "--domain-id", configured.DomainID, "--procedure-id", configured.ProcedureID, "--page-size", "500"})
					if err != nil {
						return result, err
					}
					var changes procedureInferenceChanges
					if err := decodeProcedureInferenceJSON(changesRaw, &changes); err != nil {
						return result, errors.New("decode sanitized procedure inference changes")
					}
					lifecycleComplete, changeErr := validateProcedureInferenceChanges(changes, configured, auditor, resources, boolTarget(target, "requireChanges", true))
					if changeErr != nil {
						return result, changeErr
					}
					if err := reconcileProcedureInferenceSnapshot(list, changes); err != nil {
						return result, err
					}
					changeDigests, digestErr := procedureInferenceChangeDigests(changes.Changes)
					if digestErr != nil {
						return result, digestErr
					}
					stableEventIDs := sameProcedureInferenceChangeDigests(previousChangeDigests, changeDigests)
					for sequence, previousDigest := range previousChangeDigests {
						if currentDigest, retained := changeDigests[sequence]; retained && currentDigest != previousDigest {
							return result, errors.New("immutable procedure inference change mutated across polls")
						}
					}
					if !stableEventIDs {
						previousChangeDigests = changeDigests
						stableSince = time.Now()
					}
					continuationRaw, err := procedureInferenceCLIExec(waitCtx, driver, environment, auditor, []string{"automation", "inference-attempt", "changes", "--space-id", configured.SpaceID, "--domain-id", configured.DomainID, "--procedure-id", configured.ProcedureID, "--page-size", "100", "--after-cursor", list.SnapshotCursor})
					if err != nil {
						return result, err
					}
					var continuation procedureInferenceChanges
					if err := decodeProcedureInferenceJSON(continuationRaw, &continuation); err != nil || continuation.HistoryGap {
						return result, errors.New("snapshot-to-poll continuation failed")
					}
					if continuation.NextCursor == "" || continuation.HighWaterSequence < list.SnapshotHighWaterSequence || continuation.EarliestRetainedStateSequence != list.EarliestRetainedStateSequence {
						return result, errors.New("snapshot-to-poll continuation watermarks are inconsistent")
					}
					if _, err := validateProcedureInferenceChanges(continuation, configured, auditor, resources, false); err != nil {
						return result, errors.New("snapshot-to-poll continuation contains invalid changes")
					}
					for _, change := range continuation.Changes {
						if change.Sequence <= list.SnapshotHighWaterSequence {
							return result, errors.New("snapshot-to-poll continuation repeated a pre-snapshot sequence")
						}
					}
					checkpoint := buildProcedureInferenceCheckpoint(list, changes)
					if name := stringTarget(target, "compareCheckpoint"); name != "" {
						expected, ok := checkpoints[name]
						if !ok || !sameProcedureInferenceCheckpoint(expected, checkpoint) {
							return result, errors.New("procedure inference checkpoint changed across the recovery boundary")
						}
						result.CheckpointProved = true
					}
					if name := stringTarget(target, "checkpoint"); name != "" {
						checkpoints[name] = checkpoint
					}
					result.AttemptCount, result.TerminalCount, result.SucceededCount, result.AmbiguousCount = len(list.Attempts), terminal, succeeded, ambiguous
					result.ChangeCount, result.HighWaterSequence = len(changes.Changes), changes.HighWaterSequence
					result.ConvergenceCategory = "stable_history_pending"
					result.EarliestRetainedSequence, result.EarliestStateSequence = changes.EarliestRetainedSequence, changes.EarliestRetainedStateSequence
					result.SnapshotContinuityProved, result.StableEventIDsProved, result.RunReferencesProved, result.SanitizedEvidenceProved = true, stableEventIDs, runReferencesProved, true
					iterationStable = lifecycleComplete
					if stableFor <= 0 {
						if !lifecycleComplete {
							return result, errors.New("procedure inference change history does not contain a complete lifecycle")
						}
						result.ConvergenceCategory = "converged"
						return result, nil
					}
					if lifecycleComplete && stableEventIDs && time.Since(stableSince) >= stableFor {
						result.ConvergenceCategory = "converged"
						return result, nil
					}
				} else {
					stableSince = time.Time{}
				}
			}
		}
		if !iterationStable {
			stableSince = time.Time{}
		}
		if err := sleepContext(waitCtx, interval); err != nil {
			return result, fmt.Errorf("procedure inference attempts did not converge within %s", timeout)
		}
	}
}

func controlProcedureInference(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (ProcedureInferenceResult, error) {
	configured := resources.ProcedureInference
	result := ProcedureInferenceResult{Operation: "procedure-inference-control", ProcedureID: configured.ProcedureID, BindingID: configured.BindingID, ControlAction: stringTarget(target, "action")}
	operationCtx, cancel := procedureInferenceTimeout(ctx, target, 30*time.Second)
	defer cancel()
	auditor, ok := procedureInferenceAssignment(resources, configured.AuditActorID)
	if !ok || auditor.Username == "" || auditor.Password == "" {
		return result, errors.New("procedure inference control actor assignment is incomplete")
	}
	args := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", auditor.Username, "--password", auditor.Password, "--output", "json", "automation", "procedure", result.ControlAction, configured.ProcedureID, "--space-id", configured.SpaceID, "--domain-id", configured.DomainID}
	response, err := driver.Exec(operationCtx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: args})
	if err != nil {
		return result, fmt.Errorf("apply procedure control: %s", sanitizedCommandFailure(response.Stderr))
	}
	var decoded procedureControlResponse
	if err := decodeProcedureInferenceJSON(response.Stdout, &decoded); err != nil {
		return result, errors.New("decode procedure control response")
	}
	control := decoded.Control
	affected, acknowledged, err := validateProcedureControlBarrier(control, result.ControlAction, configured, auditor)
	if err != nil {
		return result, err
	}
	result.ControlEpoch, result.AffectedPartitions, result.AcknowledgedPartitions, result.ControlCompleted = control.ControlEpoch, affected, acknowledged, true
	return result, nil
}

func validateProcedureControlBarrier(control procedureControlBarrier, action string, configured *provision.ProcedureInferenceResources, auditor provision.ActorAssignment) ([]uint32, []uint32, error) {
	expectedStatus := int32(1)
	if action == "disable" {
		expectedStatus = 2
	}
	if control.SpaceID != configured.SpaceID || control.DomainID != configured.DomainID || control.ProcedureID != configured.ProcedureID || control.ProcedureVersion != 1 || control.ControllingPrincipalID != auditor.PrincipalID {
		return nil, nil, errors.New("procedure control barrier attribution does not match the requested procedure and audit actor")
	}
	if control.TargetStatus != expectedStatus {
		return nil, nil, errors.New("procedure control barrier target status does not match the requested action")
	}
	if !control.Completed || control.ControlEpoch == 0 || control.CompletionRevision == 0 || len(control.AffectedPartitions) == 0 || len(control.AcknowledgedPartitions) != len(control.AffectedPartitions) {
		return nil, nil, errors.New("procedure control barrier is incomplete")
	}
	affected, acknowledged := append([]uint32(nil), control.AffectedPartitions...), append([]uint32(nil), control.AcknowledgedPartitions...)
	sort.Slice(affected, func(i, j int) bool { return affected[i] < affected[j] })
	sort.Slice(acknowledged, func(i, j int) bool { return acknowledged[i] < acknowledged[j] })
	for index := range affected {
		if affected[index] != acknowledged[index] {
			return nil, nil, errors.New("procedure control barrier acknowledgements do not match affected partitions")
		}
	}
	return affected, acknowledged, nil
}

func procedureInferenceRunReferencesAttempt(response procedureInferenceRunResponse, attempt procedureInferenceAttempt, configured *provision.ProcedureInferenceResources) bool {
	run := response.Run
	if run.ID != attempt.RunID || run.DomainID != configured.DomainID || run.InvocationID != attempt.InvocationID || run.BindingID != configured.BindingID || run.BindingVersion != 1 || run.ProcedureID != configured.ProcedureID || run.ProcedureVersion != 1 || run.InferenceProfileID != configured.IntelligenceProfileID || run.EndpointID != configured.EndpointID || run.ModelID != configured.ModelID || run.CapabilityID != configured.CapabilityID {
		return false
	}
	for _, id := range run.InferenceAttemptIDs {
		if id == attempt.ID {
			return true
		}
	}
	return false
}

func assertProcedureInferenceCapacityDiagnostics(ctx context.Context, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (ProcedureInferenceResult, error) {
	configured := resources.ProcedureInference
	result := ProcedureInferenceResult{Operation: "procedure-inference-capacity-diagnostics-assert", ProcedureID: configured.ProcedureID, BindingID: configured.BindingID}
	timeout, err := targetDuration(target, "timeout")
	if err != nil {
		return result, err
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tail := intTarget(target, "tail", 5000)
	var command *exec.Cmd
	switch environment.Driver {
	case "compose":
		args := []string{"compose"}
		for _, file := range filepath.SplitList(environment.Metadata["composeFiles"]) {
			if strings.TrimSpace(file) != "" {
				args = append(args, "-f", file)
			}
		}
		if project := strings.TrimSpace(environment.Metadata["projectName"]); project != "" {
			args = append(args, "-p", project)
		}
		args = append(args, "logs", "--no-color", "--tail", strconv.Itoa(tail))
		if services := strings.TrimSpace(environment.Metadata["serviceNames"]); services != "" {
			args = append(args, strings.Split(services, ",")...)
		}
		command = exec.CommandContext(commandCtx, "docker", args...)
	case "k3d":
		args := []string{}
		if environment.Context != "" {
			args = append(args, "--context", environment.Context)
		}
		if environment.Namespace != "" {
			args = append(args, "-n", environment.Namespace)
		}
		args = append(args, "logs", "statefulset/myceld", "--all-containers=true", "--tail="+strconv.Itoa(tail))
		command = exec.CommandContext(commandCtx, "kubectl", args...)
	default:
		return result, fmt.Errorf("procedure inference diagnostics do not support environment driver %q", environment.Driver)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		return result, errors.New("collect procedure inference capacity diagnostics")
	}
	boundsLines, pressureLines := 0, 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, "procedure_inference_capacity_bounds") {
			boundsLines++
			for key, value := range map[string]string{
				"attempt_capacity_per_space": "10000", "change_capacity_per_space": "50000",
				"state_byte_capacity_per_space": "67108864", "history_byte_capacity_per_space": "67108864",
				"retention_age_seconds": "2592000", "attempt_byte_capacity": "65536", "evidence_string_byte_capacity": "1024",
			} {
				if !diagnosticFieldMatches(line, key, value) {
					return result, fmt.Errorf("procedure inference capacity bound %s is missing or incorrect", key)
				}
			}
			if containsUnsafeProcedureInferenceDiagnostic(line) {
				return result, errors.New("procedure inference capacity bound log contains forbidden evidence")
			}
		}
		if strings.Contains(line, "procedure_inference_capacity_diagnostics") {
			pressureLines++
			if containsUnsafeProcedureInferenceDiagnostic(line) {
				return result, errors.New("procedure inference pressure log contains forbidden evidence")
			}
		}
	}
	if boundsLines == 0 {
		return result, errors.New("procedure inference capacity bounds log was not observed")
	}
	if boolTarget(target, "requirePressure", false) && pressureLines == 0 {
		return result, errors.New("procedure inference pressure diagnostics log was not observed")
	}
	result.CapacityBoundsProved = true
	result.PressureDiagnosticsProved = pressureLines > 0
	result.DiagnosticRecordCount = boundsLines + pressureLines
	return result, nil
}

func diagnosticFieldMatches(line, key, value string) bool {
	return strings.Contains(line, key+"="+value) || strings.Contains(line, `"`+key+`":`+value)
}

func containsUnsafeProcedureInferenceDiagnostic(line string) bool {
	lower := strings.ToLower(line)
	for _, value := range []string{"prompt=", `"prompt":`, "output=", `"output":`, "provider_request_id", "authorization_secret", "raw_error", "graph_content", "vector="} {
		if strings.Contains(lower, value) {
			return true
		}
	}
	return false
}

func validateProcedureInferenceSnapshot(list procedureInferenceList) error {
	if list.NextPageToken != "" || list.SnapshotCursor == "" || list.SnapshotHighWaterSequence == 0 || list.EarliestRetainedStateSequence == 0 || list.EarliestRetainedStateSequence > list.SnapshotHighWaterSequence {
		return errors.New("procedure inference snapshot cursor or watermarks are invalid")
	}
	seen := make(map[string]struct{}, len(list.Attempts))
	for _, attempt := range list.Attempts {
		if attempt.ID == "" {
			return errors.New("procedure inference snapshot contains an empty attempt ID")
		}
		if _, duplicate := seen[attempt.ID]; duplicate {
			return errors.New("procedure inference snapshot contains a duplicate attempt ID")
		}
		seen[attempt.ID] = struct{}{}
	}
	return nil
}

func validateProcedureInferenceAttempt(attempt procedureInferenceAttempt, configured *provision.ProcedureInferenceResources, auditor provision.ActorAssignment, resources *provision.ScenarioResources, requireFinalPublication bool) error {
	writer, _ := procedureInferenceAssignment(resources, configured.TargetActorID)
	if attempt.ID == "" || attempt.RecordVersion == 0 || attempt.SpaceID != configured.SpaceID || attempt.DomainID != configured.DomainID || attempt.ProcedureID != configured.ProcedureID || attempt.ProcedureVersion != 1 || attempt.BindingID != configured.BindingID || attempt.BindingVersion != 1 {
		return errors.New("procedure inference attempt identity/version mismatch")
	}
	if attempt.InvocationID == "" || attempt.RunID == "" || attempt.RunAttemptNumber <= 0 || attempt.ProviderAttemptNumber <= 0 || attempt.SourceEventID == "" {
		return errors.New("procedure inference attempt durable references are incomplete")
	}
	if (attempt.SourceTransactionID == "") != (attempt.SourceGraphRevision == 0) {
		return errors.New("procedure inference attempt source transaction correlation is partial")
	}
	if attempt.ActorPrincipalID != "automation" {
		return errors.New("procedure inference attempt actor attribution mismatch")
	}
	if attempt.OwnerPrincipalID != writer.PrincipalID {
		return errors.New("procedure inference attempt owner attribution mismatch")
	}
	if attempt.OnBehalfOfPrincipalID != writer.PrincipalID {
		return errors.New("procedure inference attempt on-behalf-of attribution mismatch")
	}
	if strings.TrimSpace(attempt.EventOriginPrincipalID) == "" {
		return errors.New("procedure inference attempt event-origin attribution is missing")
	}
	if attempt.EventOriginPrincipalID != writer.PrincipalID {
		return errors.New("procedure inference attempt event-origin attribution mismatch")
	}
	if attempt.Operation != 2 || attempt.ProfileID != configured.IntelligenceProfileID || attempt.EndpointID != configured.EndpointID || attempt.ModelID != configured.ModelID || attempt.CapabilityID != configured.CapabilityID || attempt.PolicyDecisionID == "" || attempt.CredentialGrantID != configured.CredentialGrantID || attempt.AuthorizationRevision == 0 || attempt.ConfigurationRevision == 0 {
		return errors.New("procedure inference attempt authorization evidence mismatch")
	}
	if attempt.State < procedureInferenceRequested || attempt.State > procedureInferenceAmbiguous {
		return errors.New("procedure inference attempt state is invalid")
	}
	if attempt.TokenCountAuthority != "" && attempt.TokenCountAuthority != "provider_reported" && attempt.TokenCountAuthority != "unavailable" {
		return errors.New("procedure inference attempt token authority is not allowlisted")
	}
	switch attempt.OutcomeCategory {
	case "", "succeeded", "provider_outcome_unknown", "authorization_denied", "canceled_before_provider", "failed_before_provider":
	default:
		return errors.New("procedure inference attempt outcome category is not allowlisted")
	}
	switch attempt.PublicationDisposition {
	case "", "published", "not_published_provider_failure", "suppressed_procedure_control", "rejected_invalid_output", "publication_precondition_failed", "publication_pending", "publication_failed", "suppressed_authorization_or_control", "not_changed_or_duplicate":
	default:
		return errors.New("procedure inference publication disposition is not allowlisted")
	}
	if attempt.State >= procedureInferenceSucceeded && strings.TrimSpace(attempt.OutcomeCategory) == "" {
		return errors.New("terminal procedure inference attempt is missing an outcome category")
	}
	if requireFinalPublication && attempt.State == procedureInferenceSucceeded && attempt.PublicationDisposition != "published" {
		return errors.New("successful procedure inference attempt was not published")
	}
	_ = auditor
	return nil
}

func procedureInferenceObservedPublicationDisposition(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return "unset"
	case "published", "not_published_provider_failure", "suppressed_procedure_control", "rejected_invalid_output", "publication_precondition_failed", "publication_pending", "publication_failed", "suppressed_authorization_or_control", "not_changed_or_duplicate":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "invalid"
	}
}

func procedureInferenceAttemptValidationCategory(err error) string {
	if err == nil {
		return ""
	}
	switch err.Error() {
	case "procedure inference attempt identity/version mismatch":
		return "identity_version_mismatch"
	case "procedure inference attempt durable references are incomplete":
		return "durable_references_incomplete"
	case "procedure inference attempt source transaction correlation is partial":
		return "source_correlation_partial"
	case "procedure inference attempt actor attribution mismatch":
		return "actor_attribution_mismatch"
	case "procedure inference attempt owner attribution mismatch":
		return "owner_attribution_mismatch"
	case "procedure inference attempt on-behalf-of attribution mismatch":
		return "on_behalf_of_attribution_mismatch"
	case "procedure inference attempt event-origin attribution is missing":
		return "event_origin_attribution_missing"
	case "procedure inference attempt event-origin attribution mismatch":
		return "event_origin_attribution_mismatch"
	case "procedure inference attempt authorization evidence mismatch":
		return "authorization_evidence_mismatch"
	case "procedure inference attempt state is invalid":
		return "state_invalid"
	case "procedure inference attempt token authority is not allowlisted":
		return "token_authority_invalid"
	case "procedure inference attempt outcome category is not allowlisted":
		return "outcome_category_invalid"
	case "procedure inference publication disposition is not allowlisted":
		return "publication_disposition_invalid"
	case "terminal procedure inference attempt is missing an outcome category":
		return "terminal_outcome_missing"
	case "successful procedure inference attempt was not published":
		return "successful_publication_pending_or_invalid"
	default:
		return "validation_failed"
	}
}

func validateProcedureInferenceChangeAttempt(attempt procedureInferenceAttempt, configured *provision.ProcedureInferenceResources, auditor provision.ActorAssignment, resources *provision.ScenarioResources) error {
	writer, _ := procedureInferenceAssignment(resources, configured.TargetActorID)
	if attempt.ID == "" || attempt.RecordVersion == 0 || attempt.SpaceID != configured.SpaceID || attempt.DomainID != configured.DomainID || attempt.ProcedureID != configured.ProcedureID || attempt.ProcedureVersion != 1 || attempt.BindingID != configured.BindingID || attempt.BindingVersion != 1 {
		return errors.New("procedure inference change attempt identity/version mismatch")
	}
	if attempt.State < procedureInferenceRequested || attempt.State > procedureInferenceAmbiguous {
		return errors.New("procedure inference change attempt state is invalid")
	}
	if attempt.ActorPrincipalID != "automation" || attempt.OwnerPrincipalID != writer.PrincipalID || attempt.OnBehalfOfPrincipalID != writer.PrincipalID || attempt.EventOriginPrincipalID != writer.PrincipalID {
		return errors.New("procedure inference change attempt attribution mismatch")
	}
	if attempt.State >= procedureInferenceSucceeded {
		return validateProcedureInferenceAttempt(attempt, configured, auditor, resources, false)
	}
	if attempt.PublicationDisposition != "" {
		return errors.New("nonterminal procedure inference change has a publication disposition")
	}
	return nil
}

func validateProcedureInferenceChanges(changes procedureInferenceChanges, configured *provision.ProcedureInferenceResources, auditor provision.ActorAssignment, resources *provision.ScenarioResources, required bool) (bool, error) {
	if changes.HistoryGap {
		return false, errors.New("unexpected procedure inference history gap")
	}
	if required && len(changes.Changes) == 0 {
		return false, errors.New("procedure inference change history is empty")
	}
	seen := map[string]struct{}{}
	type lifecycle struct {
		version     uint64
		state       int32
		publication string
		requested   bool
		terminal    bool
	}
	lifecycles := map[string]lifecycle{}
	var previous uint64
	for _, change := range changes.Changes {
		if change.EventID == "" || change.SchemaVersion == "" || change.SpaceID != configured.SpaceID || change.AttemptID == "" || change.RecordVersion == 0 || change.Sequence == 0 || change.Sequence <= previous || (changes.EarliestRetainedSequence > 0 && change.Sequence < changes.EarliestRetainedSequence) {
			return false, errors.New("procedure inference change ordering or identity is invalid")
		}
		if _, duplicate := seen[change.EventID]; duplicate {
			return false, errors.New("procedure inference change event ID is duplicated")
		}
		if change.Attempt.ID != change.AttemptID || change.Attempt.RecordVersion != change.RecordVersion || change.Attempt.SpaceID != configured.SpaceID || change.Attempt.ProcedureID != configured.ProcedureID || change.Attempt.BindingID != configured.BindingID {
			return false, errors.New("procedure inference change snapshot identity mismatch")
		}
		if err := validateProcedureInferenceChangeAttempt(change.Attempt, configured, auditor, resources); err != nil {
			return false, err
		}
		current := lifecycles[change.AttemptID]
		if current.version > 0 && (change.RecordVersion <= current.version || change.Attempt.State < current.state) {
			return false, errors.New("procedure inference change lifecycle regressed")
		}
		if current.version > 0 {
			previousRank := procedureInferencePublicationRank(current.publication)
			nextRank := procedureInferencePublicationRank(change.Attempt.PublicationDisposition)
			if previousRank == 2 && change.Attempt.PublicationDisposition != current.publication {
				return false, errors.New("procedure inference publication disposition changed after finalization")
			}
			if current.terminal && (change.Attempt.State != current.state || nextRank < previousRank) {
				return false, errors.New("procedure inference terminal state or publication disposition regressed")
			}
		}
		current.version, current.state, current.publication = change.RecordVersion, change.Attempt.State, change.Attempt.PublicationDisposition
		current.requested = current.requested || change.Attempt.State == procedureInferenceRequested
		current.terminal = change.Attempt.State >= procedureInferenceSucceeded && change.Attempt.State <= procedureInferenceAmbiguous
		lifecycles[change.AttemptID] = current
		seen[change.EventID] = struct{}{}
		previous = change.Sequence
	}
	if previous > changes.HighWaterSequence || (changes.EarliestRetainedSequence > 0 && changes.EarliestRetainedSequence > changes.HighWaterSequence) || (changes.EarliestRetainedStateSequence > 0 && changes.EarliestRetainedStateSequence > changes.HighWaterSequence) {
		return false, errors.New("procedure inference change watermarks are inconsistent")
	}
	complete := len(lifecycles) > 0
	for _, current := range lifecycles {
		complete = complete && current.requested && current.terminal
	}
	return complete, nil
}

func reconcileProcedureInferenceSnapshot(list procedureInferenceList, changes procedureInferenceChanges) error {
	latest := make(map[string]procedureInferenceAttempt, len(list.Attempts))
	for _, change := range changes.Changes {
		if change.Sequence <= list.SnapshotHighWaterSequence {
			latest[change.AttemptID] = change.Attempt
		}
	}
	if len(latest) != len(list.Attempts) {
		return errors.New("materialized procedure inference snapshot does not match change history")
	}
	for _, attempt := range list.Attempts {
		changeAttempt, ok := latest[attempt.ID]
		if !ok || procedureInferenceRecordDigest(changeAttempt) != procedureInferenceRecordDigest(attempt) {
			return errors.New("materialized procedure inference attempt differs from its latest change")
		}
	}
	return nil
}

func procedureInferencePublicationRank(disposition string) int {
	switch disposition {
	case "":
		return 0
	case "publication_pending":
		return 1
	case "published", "not_published_provider_failure", "suppressed_procedure_control", "rejected_invalid_output", "publication_precondition_failed", "publication_failed", "suppressed_authorization_or_control", "not_changed_or_duplicate":
		return 2
	default:
		return -1
	}
}

func buildProcedureInferenceCheckpoint(list procedureInferenceList, changes procedureInferenceChanges) procedureInferenceCheckpoint {
	checkpoint := procedureInferenceCheckpoint{
		Attempts:                      make(map[string]string, len(list.Attempts)),
		Events:                        make(map[uint64]string, len(changes.Changes)),
		SnapshotHighWaterSequence:     list.SnapshotHighWaterSequence,
		ChangeHighWaterSequence:       changes.HighWaterSequence,
		EarliestRetainedSequence:      changes.EarliestRetainedSequence,
		EarliestRetainedStateSequence: changes.EarliestRetainedStateSequence,
	}
	for _, attempt := range list.Attempts {
		checkpoint.Attempts[attempt.ID] = procedureInferenceRecordDigest(attempt)
	}
	for _, change := range changes.Changes {
		checkpoint.Events[change.Sequence] = procedureInferenceRecordDigest(change)
	}
	return checkpoint
}

func sameProcedureInferenceCheckpoint(expected, actual procedureInferenceCheckpoint) bool {
	if expected.SnapshotHighWaterSequence != actual.SnapshotHighWaterSequence || expected.ChangeHighWaterSequence != actual.ChangeHighWaterSequence || expected.EarliestRetainedSequence != actual.EarliestRetainedSequence || expected.EarliestRetainedStateSequence != actual.EarliestRetainedStateSequence || len(expected.Attempts) != len(actual.Attempts) || len(expected.Events) != len(actual.Events) {
		return false
	}
	for id, identity := range expected.Attempts {
		if actual.Attempts[id] != identity {
			return false
		}
	}
	for sequence, eventID := range expected.Events {
		if actual.Events[sequence] != eventID {
			return false
		}
	}
	return true
}

func procedureInferenceRecordDigest(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func procedureInferenceChangeDigests(changes []procedureInferenceChange) (map[uint64]string, error) {
	out := make(map[uint64]string, len(changes))
	for _, change := range changes {
		digest := procedureInferenceRecordDigest(change)
		if digest == "" {
			return nil, errors.New("encode immutable procedure inference change")
		}
		out[change.Sequence] = digest
	}
	return out, nil
}

func sameProcedureInferenceChangeDigests(previous, current map[uint64]string) bool {
	if len(previous) == 0 || len(previous) != len(current) {
		return false
	}
	for sequence, digest := range previous {
		if current[sequence] != digest {
			return false
		}
	}
	return true
}

func procedureInferenceCLIExec(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, assignment provision.ActorAssignment, command []string) (string, error) {
	base := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", assignment.Username, "--password", assignment.Password, "--output", "json"}
	response, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: 0}, env.ExecRequest{Command: append(base, command...)})
	if err != nil {
		return "", fmt.Errorf("procedure inference audit command failed: %s", sanitizedCommandFailure(response.Stderr))
	}
	if hasForbiddenProcedureInferenceEvidence([]byte(response.Stdout)) {
		return "", errors.New("procedure inference API returned forbidden evidence fields")
	}
	return response.Stdout, nil
}

func decodeProcedureInferenceJSON(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("procedure inference evidence does not match the closed schema")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("procedure inference evidence contains trailing values")
	}
	return nil
}

func hasForbiddenProcedureInferenceEvidence(raw []byte) bool {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	forbidden := map[string]struct{}{"prompt": {}, "output": {}, "vector": {}, "provider_request_id": {}, "provider_body": {}, "raw_error": {}, "authorization_secret": {}, "metadata": {}}
	var walk func(any) bool
	walk = func(current any) bool {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if _, blocked := forbidden[strings.ToLower(strings.TrimSpace(key))]; blocked {
					return true
				}
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	return walk(value)
}

func procedureInferenceAssignment(resources *provision.ScenarioResources, actorID string) (provision.ActorAssignment, bool) {
	for _, assignment := range resources.Assignments {
		if assignment.ActorID == actorID {
			return assignment, true
		}
	}
	return provision.ActorAssignment{}, false
}

func procedureInferenceTargetGroup(resources *provision.ScenarioResources) string {
	assignment, _ := procedureInferenceAssignment(resources, resources.ProcedureInference.TargetActorID)
	return assignment.GroupName
}

func procedureInferenceAdminCLI() []string {
	return []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", provision.DefaultAdminPassword, "--output", "json"}
}

func procedureInferenceTimeout(ctx context.Context, target map[string]any, fallback time.Duration) (context.Context, context.CancelFunc) {
	timeout, err := targetDuration(target, "timeout")
	if err != nil || timeout <= 0 {
		timeout = fallback
	}
	return context.WithTimeout(ctx, timeout)
}

func procedureInferencePermissionDenied(result env.ExecResult, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(result.Stderr)
	return strings.Contains(message, "code = permissiondenied") || strings.Contains(message, `"code":"permission_denied"`)
}

func sanitizedCommandFailure(_ string) string {
	return "command_failed"
}

func stringSliceAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
