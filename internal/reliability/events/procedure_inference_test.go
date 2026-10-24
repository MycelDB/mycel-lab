package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestProcedureInferenceSetupUsesStagedSanitizedCommands(t *testing.T) {
	resources := &provision.ScenarioResources{
		ProcedureInference: &provision.ProcedureInferenceResources{SpaceID: "space", DomainID: "domain", ProcedureID: "procedure", BindingID: "binding", TargetActorID: "writer", AuditActorID: "auditor", IntelligenceProfileID: "profile", TargetLabels: []string{"Fixture"}},
		Assignments: []provision.ActorAssignment{
			{ActorID: "writer", PrincipalID: "writer-principal", Username: "writer-user", Password: "writer-password"},
			{ActorID: "auditor", PrincipalID: "auditor-principal", Username: "auditor-user", Password: "auditor-password"},
		},
	}
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "--username writer-user") {
			return env.ExecResult{Stderr: "rpc error: code = PermissionDenied desc = denied"}, errors.New("denied")
		}
		return env.ExecResult{Stdout: "{}\n"}, nil
	}}
	if err := setupProcedureInference(context.Background(), driver, env.Environment{}, resources, map[string]any{"timeout": "1s"}); err != nil {
		t.Fatalf("setupProcedureInference() error=%v", err)
	}
	if len(driver.execCommands) != 6 {
		t.Fatalf("commands=%d, want 6: %v", len(driver.execCommands), driver.execCommands)
	}
	for index, want := range []string{"sh -ec", "automation procedure put", "automation binding put", "--username auditor-user --password auditor-password --output json automation procedure enable"} {
		if !strings.Contains(driver.execCommands[index], want) {
			t.Fatalf("command[%d]=%q, want %q", index, driver.execCommands[index], want)
		}
	}
	const bindingMarker = "cat > '/tmp/mycel-lab-procedure-inference-binding.json' <<'JSON'\n"
	bindingStart := strings.Index(driver.execCommands[0], bindingMarker)
	if bindingStart < 0 {
		t.Fatalf("fixture command does not contain binding marker: %q", driver.execCommands[0])
	}
	bindingRaw := driver.execCommands[0][bindingStart+len(bindingMarker):]
	bindingEnd := strings.Index(bindingRaw, "\nJSON\n")
	if bindingEnd < 0 {
		t.Fatalf("fixture command does not terminate binding JSON")
	}
	var binding struct {
		Runtime struct {
			ActorPrincipalID      string `json:"actor_principal_id"`
			OwnerPrincipalID      string `json:"owner_principal_id"`
			OnBehalfOfPrincipalID string `json:"on_behalf_of_principal_id"`
		} `json:"runtime"`
	}
	if err := json.Unmarshal([]byte(bindingRaw[:bindingEnd]), &binding); err != nil {
		t.Fatalf("decode staged binding: %v", err)
	}
	if binding.Runtime.ActorPrincipalID != "automation" || binding.Runtime.OwnerPrincipalID != "writer-principal" || binding.Runtime.OnBehalfOfPrincipalID != "writer-principal" {
		t.Fatalf("binding runtime=%+v", binding.Runtime)
	}
}

func TestProcedureInferenceSetupReportsSafeFailedStage(t *testing.T) {
	resources := &provision.ScenarioResources{
		ProcedureInference: &provision.ProcedureInferenceResources{SpaceID: "space", DomainID: "domain", ProcedureID: "procedure", BindingID: "binding", TargetActorID: "writer", AuditActorID: "auditor", IntelligenceProfileID: "profile"},
		Assignments: []provision.ActorAssignment{
			{ActorID: "writer", PrincipalID: "writer-principal"},
			{ActorID: "auditor", PrincipalID: "auditor-principal", Username: "auditor-user", Password: "auditor-password"},
		},
	}
	driver := &recordingDriver{execResult: func(command string) (env.ExecResult, error) {
		if strings.Contains(command, "automation procedure put") {
			return env.ExecResult{Stderr: "unsafe provider body"}, errors.New("failed")
		}
		return env.ExecResult{}, nil
	}}
	err := setupProcedureInference(context.Background(), driver, env.Environment{}, resources, map[string]any{"timeout": "1s"})
	if err == nil || !strings.Contains(err.Error(), "procedure_put_failed") || strings.Contains(err.Error(), "unsafe provider body") {
		t.Fatalf("error=%v, want sanitized procedure_put_failed", err)
	}
}

func TestProcedureInferenceGraphActorDoesNotRequireSemanticResources(t *testing.T) {
	resources := &provision.ScenarioResources{
		ProcedureInference: &provision.ProcedureInferenceResources{TargetActorID: "writer"},
		Assignments:        []provision.ActorAssignment{{ActorID: "writer", GroupName: "writers"}},
	}
	_, assignment, err := dialGraphActorGroup(context.Background(), resources, "writers", "")
	if err == nil || assignment.ActorID != "writer" || !strings.Contains(err.Error(), "graph actor access token is unavailable") {
		t.Fatalf("assignment=%+v error=%v", assignment, err)
	}
	if _, _, err := dialSemanticActorGroup(context.Background(), resources, "writers"); err == nil || !strings.Contains(err.Error(), "semantic resources are not provisioned") {
		t.Fatalf("semantic dial error=%v", err)
	}
}

func TestGraphActorDefaultSelectsSemanticTarget(t *testing.T) {
	resources := &provision.ScenarioResources{
		Semantic:    &provision.SemanticResources{TargetActorID: "semantic-writer"},
		Assignments: []provision.ActorAssignment{{ActorID: "other"}, {ActorID: "semantic-writer"}},
	}
	_, err := executeSemanticGraphMutation(context.Background(), resources, "semantic-node-create", map[string]any{"nodeId": "node-1"})
	if err == nil || !strings.Contains(err.Error(), "graph actor access token is unavailable") {
		t.Fatalf("error=%v", err)
	}
}

func TestProcedureInferenceAwaitTimeoutReportsBoundedConvergenceCategory(t *testing.T) {
	driver := &recordingDriver{execResult: func(string) (env.ExecResult, error) {
		return env.ExecResult{Stdout: `{"attempts":[],"next_page_token":"","snapshot_high_water_sequence":0,"earliest_retained_state_sequence":0,"snapshot_cursor":""}`}, nil
	}}
	resources := &provision.ScenarioResources{
		ProcedureInference: &provision.ProcedureInferenceResources{
			ProcedureID: "procedure-1", BindingID: "binding-1", AuditActorID: "auditor-1",
		},
		Assignments: []provision.ActorAssignment{{ActorID: "auditor-1", Username: "auditor", Password: "password"}},
	}
	result, err := awaitProcedureInference(context.Background(), driver, env.Environment{}, resources, map[string]procedureInferenceCheckpoint{}, map[string]any{"timeout": "5ms", "interval": "1ms"})
	if err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("error=%v, want bounded convergence timeout", err)
	}
	if result.ConvergenceCategory != "snapshot_invalid" || result.AttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	requiredDiagnostics := []string{`"attemptCount":0`, `"terminalCount":0`, `"succeededCount":0`, `"failedCount":0`, `"deniedCount":0`, `"canceledCount":0`, `"ambiguousCount":0`, `"succeededOutcomeCount":0`, `"authorizationDeniedOutcomeCount":0`, `"canceledBeforeProviderOutcomeCount":0`, `"failedBeforeProviderOutcomeCount":0`, `"providerOutcomeUnknownCount":0`, `"missingOutcomeCount":0`, `"unknownOutcomeCount":0`, `"changeCount":0`, `"convergenceCategory":"snapshot_invalid"`}
	for _, required := range requiredDiagnostics {
		if !bytes.Contains(encoded, []byte(required)) {
			t.Fatalf("serialized result %s does not contain %s", encoded, required)
		}
	}

	recorder := &recordingEventRecorder{}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Driver: "test"}, resources)
	phase := spec.PhaseSpec{Name: "diagnostic", Events: []spec.EventSpec{{Type: "procedure-inference-await-terminal", Target: map[string]any{"timeout": "5ms", "interval": "1ms"}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err == nil {
		t.Fatal("ExecutePhaseEvents() error=nil, want convergence timeout")
	}
	if len(recorder.records) != 2 || recorder.records[1].Type != "event-failed" {
		t.Fatalf("records=%+v", recorder.records)
	}
	failedEvent, marshalErr := json.Marshal(recorder.records[1])
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, required := range requiredDiagnostics {
		if !bytes.Contains(failedEvent, []byte(required)) {
			t.Fatalf("failed event %s does not contain %s", failedEvent, required)
		}
	}
}

func TestProcedureInferenceFailedEventPreservesOnlySafeObservedPublicationDisposition(t *testing.T) {
	cases := []struct {
		name        string
		disposition string
		expected    string
		prohibited  string
	}{
		{name: "allowlisted", disposition: "publication_failed", expected: "publication_failed"},
		{name: "unknown", disposition: "raw-provider-secret", expected: "invalid", prohibited: "raw-provider-secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := procedureInferenceList{
				Attempts:                  []procedureInferenceAttempt{{ID: "attempt-1", State: procedureInferenceSucceeded, PublicationDisposition: tc.disposition}},
				SnapshotHighWaterSequence: 1, EarliestRetainedStateSequence: 1, SnapshotCursor: "opaque",
			}
			encoded, err := json.Marshal(list)
			if err != nil {
				t.Fatal(err)
			}
			driver := &recordingDriver{execResult: func(string) (env.ExecResult, error) {
				return env.ExecResult{Stdout: string(encoded)}, nil
			}}
			resources := &provision.ScenarioResources{
				ProcedureInference: &provision.ProcedureInferenceResources{ProcedureID: "procedure-1", BindingID: "binding-1", AuditActorID: "auditor-1"},
				Assignments:        []provision.ActorAssignment{{ActorID: "auditor-1", Username: "auditor", Password: "password"}},
			}
			recorder := &recordingEventRecorder{}
			runtime := NewDriverRuntimeWithResources(driver, env.Environment{Driver: "test"}, resources)
			phase := spec.PhaseSpec{Name: "diagnostic", Events: []spec.EventSpec{{Type: "procedure-inference-await-terminal", Target: map[string]any{"timeout": "5ms", "interval": "1ms"}}}}
			if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err == nil {
				t.Fatal("ExecutePhaseEvents() error=nil, want convergence timeout")
			}
			if len(recorder.records) != 2 || recorder.records[1].Type != "event-failed" {
				t.Fatalf("records=%+v", recorder.records)
			}
			failedEvent, err := json.Marshal(recorder.records[1])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(failedEvent, []byte(`"observedPublicationDisposition":"`+tc.expected+`"`)) {
				t.Fatalf("failed event does not contain safe observed disposition: %s", failedEvent)
			}
			if tc.prohibited != "" && bytes.Contains(failedEvent, []byte(tc.prohibited)) {
				t.Fatalf("failed event leaked unknown disposition: %s", failedEvent)
			}
		})
	}
}

func TestProcedureInferenceAwaitInvalidAttemptStillReportsFullStateAggregates(t *testing.T) {
	list := procedureInferenceList{
		Attempts: []procedureInferenceAttempt{
			{ID: "invalid-requested", State: procedureInferenceRequested},
			{ID: "invalid-succeeded", State: procedureInferenceSucceeded, OutcomeCategory: "succeeded"},
			{ID: "invalid-failed", State: procedureInferenceFailed, OutcomeCategory: "failed_before_provider"},
			{ID: "invalid-denied", State: procedureInferenceDenied, OutcomeCategory: "authorization_denied"},
			{ID: "invalid-canceled", State: procedureInferenceCanceled, OutcomeCategory: "canceled_before_provider"},
		},
		SnapshotHighWaterSequence: 2, EarliestRetainedStateSequence: 1, SnapshotCursor: "opaque",
	}
	encoded, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	driver := &recordingDriver{execResult: func(string) (env.ExecResult, error) {
		return env.ExecResult{Stdout: string(encoded)}, nil
	}}
	resources := &provision.ScenarioResources{
		ProcedureInference: &provision.ProcedureInferenceResources{
			ProcedureID: "procedure-1", BindingID: "binding-1", AuditActorID: "auditor-1",
		},
		Assignments: []provision.ActorAssignment{{ActorID: "auditor-1", Username: "auditor", Password: "password"}},
	}
	result, err := awaitProcedureInference(context.Background(), driver, env.Environment{}, resources, map[string]procedureInferenceCheckpoint{}, map[string]any{"timeout": "5ms", "interval": "1ms", "maximumAttempts": 5})
	if err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("error=%v, want bounded convergence timeout", err)
	}
	if result.ConvergenceCategory != "attempt_invalid" || result.AttemptValidationCategory != "identity_version_mismatch" || result.ObservedPublicationDisposition != "unset" || result.AttemptCount != 5 || result.TerminalCount != 4 || result.SucceededCount != 1 || result.FailedCount != 1 || result.DeniedCount != 1 || result.CanceledCount != 1 || result.AmbiguousCount != 0 || result.SucceededOutcomeCount != 1 || result.AuthorizationDeniedCount != 1 || result.CanceledBeforeProviderCount != 1 || result.FailedBeforeProviderCount != 1 || result.ProviderUnknownCount != 0 || result.MissingOutcomeCount != 1 || result.UnknownOutcomeCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestProcedureInferenceAwaitPreservesLastDecodedDiagnosticAcrossLaterFailures(t *testing.T) {
	list := procedureInferenceList{
		Attempts:                  []procedureInferenceAttempt{{ID: "invalid-denied", State: procedureInferenceDenied}},
		SnapshotHighWaterSequence: 1, EarliestRetainedStateSequence: 1, SnapshotCursor: "opaque",
	}
	encoded, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func() (env.ExecResult, error){
		"command": func() (env.ExecResult, error) {
			return env.ExecResult{Stderr: "credential-bearing raw failure"}, errors.New("command failed")
		},
		"decode": func() (env.ExecResult, error) {
			return env.ExecResult{Stdout: `{"malformed":`}, nil
		},
	}
	for name, laterResponse := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			driver := &recordingDriver{execWithContext: func(context.Context, string) (env.ExecResult, error) {
				calls++
				if calls == 1 {
					return env.ExecResult{Stdout: string(encoded)}, nil
				}
				return laterResponse()
			}}
			resources := &provision.ScenarioResources{
				ProcedureInference: &provision.ProcedureInferenceResources{ProcedureID: "procedure-1", BindingID: "binding-1", AuditActorID: "auditor-1"},
				Assignments:        []provision.ActorAssignment{{ActorID: "auditor-1", Username: "auditor", Password: "password"}},
			}
			result, err := awaitProcedureInference(context.Background(), driver, env.Environment{}, resources, map[string]procedureInferenceCheckpoint{}, map[string]any{"timeout": "20ms", "interval": "1ms"})
			if err == nil || calls < 2 || result.ConvergenceCategory != "attempt_invalid" || result.AttemptValidationCategory != "identity_version_mismatch" || result.DeniedCount != 1 {
				t.Fatalf("calls=%d result=%+v error=%v", calls, result, err)
			}
			serialized, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if bytes.Contains(serialized, []byte("credential-bearing")) || bytes.Contains(serialized, []byte("raw failure")) || bytes.Contains(serialized, []byte("malformed")) {
				t.Fatalf("serialized diagnostic leaked later failure: %s", serialized)
			}
		})
	}
}

func TestProcedureInferenceObservedPublicationDispositionIsClosed(t *testing.T) {
	cases := map[string]string{
		"":                                    "unset",
		" publication_pending ":               "publication_pending",
		"publication_failed":                  "publication_failed",
		"publication_precondition_failed":     "publication_precondition_failed",
		"suppressed_procedure_control":        "suppressed_procedure_control",
		"suppressed_authorization_or_control": "suppressed_authorization_or_control",
		"raw provider body":                   "invalid",
	}
	for value, expected := range cases {
		if actual := procedureInferenceObservedPublicationDisposition(value); actual != expected {
			t.Fatalf("observed disposition for %q=%q, want %q", value, actual, expected)
		}
	}
}

func TestProcedureInferenceAttemptValidationCategoriesAreClosed(t *testing.T) {
	cases := map[string]string{
		"procedure inference attempt identity/version mismatch":                 "identity_version_mismatch",
		"procedure inference attempt durable references are incomplete":         "durable_references_incomplete",
		"procedure inference attempt source transaction correlation is partial": "source_correlation_partial",
		"procedure inference attempt actor attribution mismatch":                "actor_attribution_mismatch",
		"procedure inference attempt owner attribution mismatch":                "owner_attribution_mismatch",
		"procedure inference attempt on-behalf-of attribution mismatch":         "on_behalf_of_attribution_mismatch",
		"procedure inference attempt event-origin attribution is missing":       "event_origin_attribution_missing",
		"procedure inference attempt event-origin attribution mismatch":         "event_origin_attribution_mismatch",
		"procedure inference attempt authorization evidence mismatch":           "authorization_evidence_mismatch",
		"procedure inference attempt state is invalid":                          "state_invalid",
		"procedure inference attempt token authority is not allowlisted":        "token_authority_invalid",
		"procedure inference attempt outcome category is not allowlisted":       "outcome_category_invalid",
		"procedure inference publication disposition is not allowlisted":        "publication_disposition_invalid",
		"terminal procedure inference attempt is missing an outcome category":   "terminal_outcome_missing",
		"successful procedure inference attempt was not published":              "successful_publication_pending_or_invalid",
		"unexpected raw provider failure":                                       "validation_failed",
	}
	for message, expected := range cases {
		if actual := procedureInferenceAttemptValidationCategory(errors.New(message)); actual != expected {
			t.Fatalf("category for %q=%q, want %q", message, actual, expected)
		}
	}
}

func TestProcedureInferenceGraphContentIsRedactedFromEventEvidence(t *testing.T) {
	target := evidenceSafeEventTarget(spec.EventSpec{Type: "procedure-inference-node-create", Target: map[string]any{"nodeId": "private-node", "text": "private graph content", "timeout": "1s"}})
	if _, exists := target["nodeId"]; exists {
		t.Fatal("node ID retained in event evidence")
	}
	if _, exists := target["text"]; exists {
		t.Fatal("graph text retained in event evidence")
	}
	if target["graphContentRedacted"] != true || target["timeout"] != "1s" {
		t.Fatalf("redacted target=%+v", target)
	}
}

func TestProcedureInferenceEvidenceRejectsForbiddenFields(t *testing.T) {
	for _, raw := range []string{
		`{"attempts":[{"prompt":"secret"}]}`,
		`{"run":{"provider_request_id":"provider-secret"}}`,
		`{"changes":[{"attempt":{"metadata":{"arbitrary":"value"}}}]}`,
	} {
		if !hasForbiddenProcedureInferenceEvidence([]byte(raw)) {
			t.Fatalf("forbidden evidence accepted: %s", raw)
		}
	}
	if hasForbiddenProcedureInferenceEvidence([]byte(`{"attempts":[{"credential_grant_id":"bounded-id","publication_disposition":"published"}]}`)) {
		t.Fatal("allowlisted correlation evidence was rejected")
	}
}

func TestValidateProcedureInferenceChangesRequiresStableOrderedIDs(t *testing.T) {
	configured := &provision.ProcedureInferenceResources{SpaceID: "space-a", DomainID: "domain", ProcedureID: "procedure", BindingID: "binding", TargetActorID: "writer", IntelligenceProfileID: "profile", EndpointID: "endpoint", ModelID: "model", CapabilityID: "capability", CredentialGrantID: "grant"}
	resources := &provision.ScenarioResources{ProcedureInference: configured, Assignments: []provision.ActorAssignment{{ActorID: "writer", PrincipalID: "writer-principal"}}}
	auditor := provision.ActorAssignment{ActorID: "auditor", PrincipalID: "auditor-principal"}
	requested := procedureInferenceAttempt{ID: "attempt-a", RecordVersion: 1, SpaceID: "space-a", DomainID: "domain", ProcedureID: "procedure", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "invocation", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1, SourceEventID: "source", SourceTransactionID: "transaction", SourceGraphRevision: 1, EventOriginPrincipalID: "writer-principal", ActorPrincipalID: "automation", OwnerPrincipalID: "writer-principal", OnBehalfOfPrincipalID: "writer-principal", State: procedureInferenceRequested}
	terminal := requested
	terminal.RecordVersion = 2
	terminal.State = procedureInferenceSucceeded
	terminal.Operation = 2
	terminal.ProfileID, terminal.EndpointID, terminal.ModelID, terminal.CapabilityID = "profile", "endpoint", "model", "capability"
	terminal.PolicyDecisionID, terminal.CredentialGrantID = "policy", "grant"
	terminal.AuthorizationRevision, terminal.ConfigurationRevision = 1, 1
	terminal.OutcomeCategory, terminal.TokenCountAuthority, terminal.PublicationDisposition = "succeeded", "provider_reported", "published"
	valid := procedureInferenceChanges{HighWaterSequence: 2, Changes: []procedureInferenceChange{
		{EventID: "event-1", SchemaVersion: "v1", SpaceID: "space-a", Sequence: 1, AttemptID: "attempt-a", RecordVersion: 1, Attempt: requested},
		{EventID: "event-2", SchemaVersion: "v1", SpaceID: "space-a", Sequence: 2, AttemptID: "attempt-a", RecordVersion: 2, Attempt: terminal},
	}}
	if stable, err := validateProcedureInferenceChanges(valid, configured, auditor, resources, true); err != nil || !stable {
		t.Fatalf("valid changes stable=%t err=%v", stable, err)
	}
	invalid := valid
	invalid.Changes = append([]procedureInferenceChange(nil), valid.Changes...)
	invalid.Changes[1].EventID = "event-1"
	if _, err := validateProcedureInferenceChanges(invalid, configured, auditor, resources, true); err == nil {
		t.Fatal("duplicate event ID accepted")
	}
	invalid = valid
	invalid.HistoryGap = true
	if _, err := validateProcedureInferenceChanges(invalid, configured, auditor, resources, true); err == nil {
		t.Fatal("history gap accepted as ordinary page")
	}
	incomplete := valid
	incomplete.Changes = incomplete.Changes[:1]
	if complete, err := validateProcedureInferenceChanges(incomplete, configured, auditor, resources, true); err != nil || complete {
		t.Fatalf("incomplete lifecycle complete=%t err=%v", complete, err)
	}
	regressed := valid
	regressed.Changes = append([]procedureInferenceChange(nil), valid.Changes...)
	regressed.Changes[1].Attempt.State = 0
	if _, err := validateProcedureInferenceChanges(regressed, configured, auditor, resources, true); err == nil {
		t.Fatal("non-increasing lifecycle state accepted")
	}
}

func TestProcedureInferenceSnapshotRejectsDuplicateAttemptsAndBadWatermarks(t *testing.T) {
	valid := procedureInferenceList{SnapshotCursor: "cursor", SnapshotHighWaterSequence: 2, EarliestRetainedStateSequence: 1, Attempts: []procedureInferenceAttempt{{ID: "attempt-a"}, {ID: "attempt-b"}}}
	if err := validateProcedureInferenceSnapshot(valid); err != nil {
		t.Fatalf("valid snapshot error=%v", err)
	}
	duplicate := valid
	duplicate.Attempts[1].ID = "attempt-a"
	if err := validateProcedureInferenceSnapshot(duplicate); err == nil {
		t.Fatal("duplicate snapshot attempt accepted")
	}
	invalidWatermark := valid
	invalidWatermark.EarliestRetainedStateSequence = 3
	if err := validateProcedureInferenceSnapshot(invalidWatermark); err == nil {
		t.Fatal("invalid snapshot watermark accepted")
	}
}

func TestProcedureInferenceCheckpointPreservesDurableIdentity(t *testing.T) {
	list := procedureInferenceList{SnapshotHighWaterSequence: 2, Attempts: []procedureInferenceAttempt{{ID: "attempt", RunID: "run", RecordVersion: 2, State: procedureInferenceSucceeded, PublicationDisposition: "published"}}}
	changes := procedureInferenceChanges{HighWaterSequence: 2, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1, Changes: []procedureInferenceChange{{Sequence: 1, EventID: "event-1"}, {Sequence: 2, EventID: "event-2"}}}
	checkpoint := buildProcedureInferenceCheckpoint(list, changes)
	if !sameProcedureInferenceCheckpoint(checkpoint, buildProcedureInferenceCheckpoint(list, changes)) {
		t.Fatal("identical checkpoint rejected")
	}
	changes.Changes[1].EventID = "regenerated-event"
	if sameProcedureInferenceCheckpoint(checkpoint, buildProcedureInferenceCheckpoint(list, changes)) {
		t.Fatal("regenerated event identity accepted")
	}
	changes.Changes[1].EventID = "event-2"
	list.Attempts[0].PolicyDecisionID = "mutated-policy"
	if sameProcedureInferenceCheckpoint(checkpoint, buildProcedureInferenceCheckpoint(list, changes)) {
		t.Fatal("mutated attempt evidence accepted")
	}
}

func TestProcedureInferenceChangesPermitMonotonicTerminalPublication(t *testing.T) {
	configured := &provision.ProcedureInferenceResources{SpaceID: "space-a", DomainID: "domain", ProcedureID: "procedure", BindingID: "binding", TargetActorID: "writer", IntelligenceProfileID: "profile", EndpointID: "endpoint", ModelID: "model", CapabilityID: "capability", CredentialGrantID: "grant"}
	resources := &provision.ScenarioResources{ProcedureInference: configured, Assignments: []provision.ActorAssignment{{ActorID: "writer", PrincipalID: "writer-principal"}}}
	auditor := provision.ActorAssignment{PrincipalID: "auditor"}
	base := procedureInferenceAttempt{ID: "attempt", SpaceID: "space-a", DomainID: "domain", ProcedureID: "procedure", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "invocation", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1, SourceEventID: "source", EventOriginPrincipalID: "writer-principal", ActorPrincipalID: "automation", OwnerPrincipalID: "writer-principal", OnBehalfOfPrincipalID: "writer-principal", Operation: 2, ProfileID: "profile", EndpointID: "endpoint", ModelID: "model", CapabilityID: "capability", PolicyDecisionID: "policy", CredentialGrantID: "grant", AuthorizationRevision: 1, ConfigurationRevision: 1, OutcomeCategory: "succeeded", TokenCountAuthority: "provider_reported"}
	changes := procedureInferenceChanges{HighWaterSequence: 4, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1}
	for index, state := range []struct {
		state       int32
		disposition string
	}{{procedureInferenceRequested, ""}, {procedureInferenceSucceeded, ""}, {procedureInferenceSucceeded, "publication_pending"}, {procedureInferenceSucceeded, "published"}} {
		attempt := base
		attempt.RecordVersion = uint64(index + 1)
		attempt.State, attempt.PublicationDisposition = state.state, state.disposition
		changes.Changes = append(changes.Changes, procedureInferenceChange{EventID: fmt.Sprintf("event-%d", index), SchemaVersion: "v1", SpaceID: "space-a", Sequence: uint64(index + 1), AttemptID: "attempt", RecordVersion: attempt.RecordVersion, Attempt: attempt})
	}
	if complete, err := validateProcedureInferenceChanges(changes, configured, auditor, resources, true); err != nil || !complete {
		t.Fatalf("monotonic publication complete=%t err=%v", complete, err)
	}
	changes.Changes[3].Attempt.PublicationDisposition = ""
	if _, err := validateProcedureInferenceChanges(changes, configured, auditor, resources, true); err == nil {
		t.Fatal("publication disposition regression accepted")
	}
	changes.Changes[2].Attempt.PublicationDisposition = "published"
	changes.Changes[3].Attempt.PublicationDisposition = "publication_failed"
	if _, err := validateProcedureInferenceChanges(changes, configured, auditor, resources, true); err == nil {
		t.Fatal("final publication disposition mutation accepted")
	}
	changes.Changes[0].Attempt.PublicationDisposition = "publication_failed"
	changes.Changes[2].Attempt.PublicationDisposition = "publication_pending"
	changes.Changes[3].Attempt.PublicationDisposition = "published"
	if _, err := validateProcedureInferenceChanges(changes, configured, auditor, resources, true); err == nil {
		t.Fatal("pre-terminal final publication disposition accepted")
	}
}

func TestProcedureInferenceSnapshotReconcilesLatestEligibleChange(t *testing.T) {
	materialized := procedureInferenceAttempt{ID: "attempt", RecordVersion: 3, PublicationDisposition: "published"}
	list := procedureInferenceList{SnapshotHighWaterSequence: 3, Attempts: []procedureInferenceAttempt{materialized}}
	changes := procedureInferenceChanges{Changes: []procedureInferenceChange{
		{Sequence: 2, AttemptID: "attempt", Attempt: procedureInferenceAttempt{ID: "attempt", RecordVersion: 2, PublicationDisposition: "publication_pending"}},
		{Sequence: 3, AttemptID: "attempt", Attempt: materialized},
		{Sequence: 4, AttemptID: "attempt", Attempt: procedureInferenceAttempt{ID: "attempt", RecordVersion: 4, PublicationDisposition: "publication_failed"}},
	}}
	if err := reconcileProcedureInferenceSnapshot(list, changes); err != nil {
		t.Fatalf("matching snapshot error=%v", err)
	}
	changes.Changes[1].Attempt.PublicationDisposition = "publication_failed"
	if err := reconcileProcedureInferenceSnapshot(list, changes); err == nil {
		t.Fatal("materialized attempt mismatch accepted")
	}
}

func TestProcedureInferenceChangeDigestsCoverFullImmutablePayload(t *testing.T) {
	changes := []procedureInferenceChange{{EventID: "event", Sequence: 1, Attempt: procedureInferenceAttempt{ID: "attempt", PolicyDecisionID: "policy-a"}}}
	before, err := procedureInferenceChangeDigests(changes)
	if err != nil {
		t.Fatal(err)
	}
	changes[0].Attempt.PolicyDecisionID = "policy-b"
	after, err := procedureInferenceChangeDigests(changes)
	if err != nil {
		t.Fatal(err)
	}
	if before[1] == after[1] {
		t.Fatal("immutable payload mutation did not change digest")
	}
}

func TestProcedureInferencePermissionDeniedRequiresStructuredAuthorizationCode(t *testing.T) {
	failure := errors.New("command failed")
	if !procedureInferencePermissionDenied(env.ExecResult{Stderr: "rpc error: code = PermissionDenied desc = denied"}, failure) {
		t.Fatal("structured PermissionDenied was rejected")
	}
	for _, result := range []env.ExecResult{{Stderr: "connection refused"}, {Stderr: "context deadline exceeded"}, {Stderr: "permission denied"}} {
		if procedureInferencePermissionDenied(result, failure) {
			t.Fatalf("non-structured failure accepted: %q", result.Stderr)
		}
	}
}

func TestProcedureInferenceClosedEvidenceSchemaRejectsUnknownFields(t *testing.T) {
	for _, raw := range []string{
		`{"attempts":[],"prompt_text":"secret"}`,
		`{"attempts":[{"providerResponseBody":"secret"}]}`,
		`{"changes":[{"attempt":{"raw_provider_error":"secret"}}]}`,
	} {
		var target any
		switch {
		case strings.Contains(raw, `"changes"`):
			target = &procedureInferenceChanges{}
		default:
			target = &procedureInferenceList{}
		}
		if err := decodeProcedureInferenceJSON(raw, target); err == nil {
			t.Fatalf("unknown evidence field accepted: %s", raw)
		}
	}
}

func TestProcedureControlBarrierRequiresExactAttribution(t *testing.T) {
	configured := &provision.ProcedureInferenceResources{SpaceID: "space", DomainID: "domain", ProcedureID: "procedure"}
	auditor := provision.ActorAssignment{PrincipalID: "auditor"}
	valid := procedureControlBarrier{SpaceID: "space", DomainID: "domain", ProcedureID: "procedure", ProcedureVersion: 1, TargetStatus: 2, ControlEpoch: 1, ControllingPrincipalID: "auditor", AffectedPartitions: []uint32{2, 1}, AcknowledgedPartitions: []uint32{1, 2}, Completed: true, CompletionRevision: 1}
	if _, _, err := validateProcedureControlBarrier(valid, "disable", configured, auditor); err != nil {
		t.Fatalf("valid barrier error=%v", err)
	}
	for _, mutate := range []func(*procedureControlBarrier){
		func(value *procedureControlBarrier) { value.ProcedureID = "wrong" },
		func(value *procedureControlBarrier) { value.ControllingPrincipalID = "wrong" },
		func(value *procedureControlBarrier) { value.TargetStatus = 1 },
	} {
		invalid := valid
		mutate(&invalid)
		if _, _, err := validateProcedureControlBarrier(invalid, "disable", configured, auditor); err == nil {
			t.Fatalf("invalid barrier accepted: %+v", invalid)
		}
	}
}

func TestProcedureInferenceRunReferenceValidation(t *testing.T) {
	configured := &provision.ProcedureInferenceResources{DomainID: "domain", ProcedureID: "procedure", BindingID: "binding", IntelligenceProfileID: "profile", EndpointID: "endpoint", ModelID: "model", CapabilityID: "capability"}
	attempt := procedureInferenceAttempt{ID: "attempt", RunID: "run", InvocationID: "invocation"}
	var response procedureInferenceRunResponse
	response.Run.ID = "run"
	response.Run.DomainID = "domain"
	response.Run.InvocationID = "invocation"
	response.Run.BindingID = "binding"
	response.Run.BindingVersion = 1
	response.Run.ProcedureID = "procedure"
	response.Run.ProcedureVersion = 1
	response.Run.InferenceProfileID = "profile"
	response.Run.EndpointID = "endpoint"
	response.Run.ModelID = "model"
	response.Run.CapabilityID = "capability"
	response.Run.InferenceAttemptIDs = []string{"attempt"}
	if !procedureInferenceRunReferencesAttempt(response, attempt, configured) {
		t.Fatal("valid attempt reference rejected")
	}
	response.Run.InferenceAttemptIDs = nil
	if procedureInferenceRunReferencesAttempt(response, attempt, configured) {
		t.Fatal("missing attempt reference accepted")
	}
}
