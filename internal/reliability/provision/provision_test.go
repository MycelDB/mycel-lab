package provision

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPlanScenarioCreatesWriterResourcesAndReaderAssignments(t *testing.T) {
	scenario, err := catalog.ResolveScenarioFile(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml"), catalog.ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	resources, err := PlanScenario(scenario, Options{RunID: "raft-3-node-short-outage-20260912-000000", DaemonAddrs: []string{"127.0.0.1:19091", "127.0.0.1:19092", "127.0.0.1:19093"}, DryRun: true})
	if err != nil {
		t.Fatalf("PlanScenario() error = %v", err)
	}
	if len(resources.Principals) != 6 {
		t.Fatalf("principals=%d, want 6", len(resources.Principals))
	}
	if len(resources.Spaces) != 4 || len(resources.Domains) != 4 {
		t.Fatalf("spaces=%d domains=%d, want 4/4", len(resources.Spaces), len(resources.Domains))
	}
	if len(resources.Assignments) != 6 {
		t.Fatalf("assignments=%d, want 6", len(resources.Assignments))
	}
	writers := 0
	readers := 0
	for _, assignment := range resources.Assignments {
		switch assignment.BehaviorType {
		case "graph-transaction":
			writers++
			if assignment.TargetActorID != "" {
				t.Fatalf("writer has target actor: %+v", assignment)
			}
		case "gql-read":
			readers++
			if assignment.TargetActorID == "" || assignment.TargetGroup != "journal-users" {
				t.Fatalf("reader missing writer target: %+v", assignment)
			}
		default:
			t.Fatalf("unexpected behavior type %q", assignment.BehaviorType)
		}
		if assignment.DaemonAddr == "" || assignment.Username == "" {
			t.Fatalf("assignment missing addr/username: %+v", assignment)
		}
	}
	if writers != 4 || readers != 2 {
		t.Fatalf("writers=%d readers=%d, want 4/2", writers, readers)
	}
}

func TestPlanScenarioQualifiesWriterResourcesByActorGroup(t *testing.T) {
	profile := spec.ActorProfile{Behavior: map[string]any{
		"type": "semantic-control",
		"dataScope": map[string]any{
			"space":  map[string]any{"prefix": "semantic-contract-space"},
			"domain": map[string]any{"prefix": "semantic-contract-domain"},
		},
	}}
	scenario := spec.ResolvedScenario{ActorGroups: []spec.ResolvedActorGroup{
		{ActorGroupSpec: spec.ActorGroupSpec{Name: "semantic-controller", Count: 1}, Profile: profile},
		{ActorGroupSpec: spec.ActorGroupSpec{Name: "semantic-outsider", Count: 1}, Profile: profile},
	}}
	resources, err := PlanScenario(scenario, Options{RunID: "semantic-contract-20261021-103741", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resources.Spaces) != 2 || len(resources.Domains) != 2 {
		t.Fatalf("spaces=%d domains=%d, want 2/2", len(resources.Spaces), len(resources.Domains))
	}
	if resources.Spaces[0].Name == resources.Spaces[1].Name {
		t.Fatalf("actor groups share space name %q", resources.Spaces[0].Name)
	}
	if resources.Domains[0].Key == resources.Domains[1].Key {
		t.Fatalf("actor groups share domain key %q", resources.Domains[0].Key)
	}
	for i, group := range []string{"semantic-controller", "semantic-outsider"} {
		if !strings.Contains(resources.Spaces[i].Name, group) || !strings.Contains(resources.Domains[i].Key, group) {
			t.Fatalf("resource scope does not include actor group %q: space=%q domain=%q", group, resources.Spaces[i].Name, resources.Domains[i].Key)
		}
	}
}

func TestComposeSemanticContractTimingAndInitialBackfillProvenance(t *testing.T) {
	scenarioPath := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-contract", "compose-semantic-graph-contract.yaml")
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	scenario, err := catalog.ResolveScenarioFile(scenarioPath, catalog.ResolveOptions{
		ProviderProfileOverride: profilePath,
		ProviderDryRun:          true,
		ProviderManifest:        provider.BuiltInHarnessManifest(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if scenario.Semantic == nil || scenario.ProviderProfile == nil {
		t.Fatal("semantic scenario or provider profile was not resolved")
	}
	cooldown := scenario.Semantic.Rule.DirtyCooldown.Duration
	maintenanceTimeout := scenario.Semantic.Maintenance.Timeout.Duration
	requestTimeout := scenario.ProviderProfile.Profile.Budgets.RequestTimeout.Duration
	if cooldown <= 0 || cooldown >= maintenanceTimeout {
		t.Fatalf("dirty cooldown=%s must be positive and below maintenance timeout=%s", cooldown, maintenanceTimeout)
	}
	if requestTimeout <= maintenanceTimeout {
		t.Fatalf("request timeout=%s must exceed maintenance timeout=%s", requestTimeout, maintenanceTimeout)
	}
	const initialBackfillTarget = "initial-backfill-target-revision"
	var seedEvents []spec.EventSpec
	initialBackfillForced := false
	initialAssertions := 0
	for _, phase := range scenario.Phases {
		switch phase.Name {
		case "seed-before-rule":
			seedEvents = phase.Events
		case "create-rule-backfill-and-converge":
			for _, event := range phase.Events {
				if event.Type == "semantic-backfill" {
					initialBackfillForced, _ = event.Target["force"].(bool)
				}
			}
		case "prove-direct-indexed-retrieval":
			for _, event := range phase.Events {
				if event.Type != "semantic-search-assert" {
					continue
				}
				initialAssertions++
				if got := event.Target["expectSourceRevisionFrom"]; got != initialBackfillTarget {
					t.Fatalf("initial retrieval source-revision capture=%v, want %q", got, initialBackfillTarget)
				}
			}
		}
	}
	if len(seedEvents) != 3 {
		t.Fatalf("initial seed event count=%d, want 3", len(seedEvents))
	}
	if !initialBackfillForced {
		t.Fatal("initial pinned semantic backfill must force publication over racing historical maintenance")
	}
	for index, event := range seedEvents {
		capture, _ := event.Target["captureRevisionAs"].(string)
		if index == len(seedEvents)-1 {
			if capture != initialBackfillTarget {
				t.Fatalf("final pre-rule revision capture=%q, want %q", capture, initialBackfillTarget)
			}
		} else if capture != "" {
			t.Fatalf("pre-rule event %d unexpectedly captures revision %q", index, capture)
		}
	}
	if initialAssertions != 3 {
		t.Fatalf("initial semantic retrieval assertions=%d, want 3", initialAssertions)
	}
}

func TestCleanupStatusJSONExcludesRawErrors(t *testing.T) {
	encoded, err := json.Marshal(CleanupStatus{Attempted: true, Errors: []string{"secret raw daemon error"}, ErrorCategories: []string{"resource_cleanup_failed"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret raw daemon error") || !strings.Contains(string(encoded), "resource_cleanup_failed") {
		t.Fatalf("unsafe cleanup evidence: %s", encoded)
	}
}

func TestCleanupAcrossSpaceBarrierRetriesAuditReferencedResources(t *testing.T) {
	tests := []struct {
		name            string
		barrierFailures []string
		retryFailures   []string
		wantFailures    []string
	}{
		{name: "retry succeeds"},
		{name: "barrier failure remains final", barrierFailures: []string{"space delete failed"}, wantFailures: []string{"space delete failed"}},
		{name: "retry failure remains final", retryFailures: []string{"semantic retry failed"}, wantFailures: []string{"semantic retry failed"}},
		{name: "barrier and retry failures remain final", barrierFailures: []string{"space delete failed"}, retryFailures: []string{"semantic retry failed"}, wantFailures: []string{"space delete failed", "semantic retry failed"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls []string
			semanticAttempts := 0
			failures := cleanupAcrossSpaceBarrier(
				func() []string {
					semanticAttempts++
					calls = append(calls, "semantic")
					if semanticAttempts == 1 {
						return []string{"transient immutable audit reference"}
					}
					return append([]string(nil), test.retryFailures...)
				},
				func() []string {
					calls = append(calls, "spaces")
					return append([]string(nil), test.barrierFailures...)
				},
			)
			if !reflect.DeepEqual(failures, test.wantFailures) {
				t.Fatalf("cleanup failures=%v, want %v", failures, test.wantFailures)
			}
			if want := []string{"semantic", "spaces", "semantic"}; !reflect.DeepEqual(calls, want) {
				t.Fatalf("cleanup order=%v, want %v", calls, want)
			}
		})
	}
}

func TestSemanticCleanupRetainedByAuditFailsClosed(t *testing.T) {
	const (
		usageOne    = "usage_event:11111111-1111-4111-8111-111111111111"
		usageTwo    = "usage_event:22222222-2222-4222-8222-222222222222"
		decisionOne = "policy_decision:33333333-3333-4333-8333-333333333333"
	)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "usage events", err: status.Error(codes.FailedPrecondition, "credential is referenced by "+usageOne+", "+usageTwo), want: true},
		{name: "policy decisions with truncated tail", err: status.Error(codes.FailedPrecondition, "access policy is referenced by "+decisionOne+", ...6 more")},
		{name: "mixed audit types", err: status.Error(codes.FailedPrecondition, "resource is referenced by "+usageOne+", "+decisionOne), want: true},
		{name: "mixed reference types", err: status.Error(codes.FailedPrecondition, "credential is referenced by "+usageOne+", credential_grant:44444444-4444-4444-8444-444444444444")},
		{name: "audit token with trailing reference", err: status.Error(codes.FailedPrecondition, "credential is referenced by "+usageOne+" credential_grant:44444444-4444-4444-8444-444444444444")},
		{name: "empty audit id", err: status.Error(codes.FailedPrecondition, "credential is referenced by usage_event:")},
		{name: "malformed truncation", err: status.Error(codes.FailedPrecondition, "credential is referenced by "+usageOne+", ...credential_grant:grant-1 more")},
		{name: "zero truncation", err: status.Error(codes.FailedPrecondition, "credential is referenced by "+usageOne+", ...0 more")},
		{name: "truncation only", err: status.Error(codes.FailedPrecondition, "credential is referenced by ...6 more")},
		{name: "truncation is not final", err: status.Error(codes.FailedPrecondition, "credential is referenced by ...6 more, "+usageOne)},
		{name: "unrecognized precondition", err: status.Error(codes.FailedPrecondition, "credential cannot be deleted")},
		{name: "wrong status", err: status.Error(codes.Internal, "credential is referenced by "+usageOne)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := semanticCleanupRetainedByAudit(test.err); got != test.want {
				t.Fatalf("semanticCleanupRetainedByAudit()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestSemanticCleanupFailureEvidenceExcludesRawDaemonReferences(t *testing.T) {
	const rawID = "11111111-1111-4111-8111-111111111111"
	err := status.Error(codes.FailedPrecondition, "capability is referenced by usage_event:"+rawID+", ...9 more raw-secret")
	resources := &SemanticResources{Cleanup: []SemanticOperation{{Resource: "capability", Status: "planned"}}}
	message := sanitizedSemanticCleanupFailure("capability", err)
	setSemanticCleanupStatus(resources, "capability", "failed", message)
	encoded, marshalErr := json.Marshal(resources.Cleanup)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if !strings.Contains(string(encoded), `"error":"delete capability: rpc_failedprecondition"`) {
		t.Fatalf("semantic cleanup evidence lacks closed failure category: %s", encoded)
	}
	for _, prohibited := range []string{rawID, "raw-secret", "usage_event", "...9 more"} {
		if strings.Contains(string(encoded), prohibited) {
			t.Fatalf("semantic cleanup evidence leaked %q: %s", prohibited, encoded)
		}
	}
}

func TestProcedureInferenceCleanupRetainedByAuditRequiresExactRootedClosure(t *testing.T) {
	const (
		policyID     = "33333333-3333-4333-8333-333333333333"
		grantID      = "44444444-4444-4444-8444-444444444444"
		credentialID = "55555555-5555-4555-8555-555555555555"
		profileID    = "66666666-6666-4666-8666-666666666666"
		capabilityID = "77777777-7777-4777-8777-777777777777"
		modelID      = "88888888-8888-4888-8888-888888888888"
		endpointID   = "99999999-9999-4999-8999-999999999999"
		usageID      = "11111111-1111-4111-8111-111111111111"
		decisionID   = "22222222-2222-4222-8222-222222222222"
	)
	resources := &ProcedureInferenceResources{
		AccessPolicyID: policyID, CredentialGrantID: grantID, CredentialID: credentialID, IntelligenceProfileID: profileID,
		CapabilityID: capabilityID, ModelID: modelID, EndpointID: endpointID,
	}
	for index, resource := range []string{"access-policy", "credential-grant", "credential", "intelligence-profile", "capability", "model", "endpoint", "inference-package"} {
		resources.Cleanup = append(resources.Cleanup, SemanticOperation{Sequence: index + 1, Resource: resource, Status: "planned"})
	}
	retainedThisPass := make(map[string]struct{})
	retain := func(resource, message string) {
		t.Helper()
		details, ok := procedureInferenceCleanupRetainedByAudit(resources, resource, retainedThisPass, status.Error(codes.FailedPrecondition, message))
		if !ok {
			t.Fatalf("resource %s was not recognized as an immutable audit closure: %s", resource, message)
		}
		setProcedureInferenceCleanupStatus(resources, resource, "retained-by-audit", "immutable audit closure", details)
		retainedThisPass[resource] = struct{}{}
		encoded, err := json.Marshal(details)
		if err != nil {
			t.Fatal(err)
		}
		for _, rawID := range []string{policyID, grantID, credentialID, profileID, capabilityID, modelID, endpointID, usageID, decisionID} {
			if strings.Contains(string(encoded), rawID) {
				t.Fatalf("cleanup details leaked raw reference ID %s: %s", rawID, encoded)
			}
		}
		if !strings.Contains(string(encoded), `"retentionCategory":"immutable-audit-closure"`) || !strings.Contains(string(encoded), `"idHash":"`) {
			t.Fatalf("cleanup details are not typed and hashed: %s", encoded)
		}
	}

	retain("access-policy", "access policy is referenced by policy_decision:"+decisionID)
	retain("credential-grant", "credential grant is referenced by policy_decision:"+decisionID)
	retain("credential", "credential is referenced by credential_grant:"+grantID+", usage_event:"+usageID)
	retain("intelligence-profile", "intelligence profile is referenced by inference_policy:"+policyID)
	retain("capability", "model endpoint capability is referenced by credential_grant:"+grantID+", intelligence_profile:"+profileID)
	retain("model", "model is referenced by capability:"+capabilityID+", credential_grant:"+grantID)
	retain("endpoint", "model endpoint is referenced by capability:"+capabilityID+", credential:"+credentialID)

	fresh := *resources
	fresh.Cleanup = append([]SemanticOperation(nil), resources.Cleanup...)
	for index := range fresh.Cleanup {
		// Persisted first-pass statuses must not authenticate a second-pass closure.
		fresh.Cleanup[index].Status = "retained-by-audit"
	}
	negative := []struct {
		target         string
		category       string
		hashedEvidence bool
		err            error
	}{
		{"credential", "unrooted-run-owned-reference", true, status.Error(codes.FailedPrecondition, "resource is referenced by credential_grant:"+grantID)},
		{"credential", "foreign-run-owned-reference", true, status.Error(codes.FailedPrecondition, "resource is referenced by credential_grant:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")},
		{"endpoint", "unsupported-reference-type", true, status.Error(codes.FailedPrecondition, "resource is referenced by semantic_index:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")},
		{"credential-grant", "truncated-reference-list", true, status.Error(codes.FailedPrecondition, "resource is referenced by usage_event:"+usageID+", ...2 more")},
		{"credential-grant", "malformed-reference-list", true, status.Error(codes.FailedPrecondition, "resource is referenced by usage_event:"+usageID+", malformed")},
		{"credential-grant", "unsupported-reference-type", false, status.Error(codes.FailedPrecondition, "resource is referenced by future_reference:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")},
		{"capability", "invalid-reference-edge", true, status.Error(codes.FailedPrecondition, "resource is referenced by capability:"+capabilityID)},
		{"credential-grant", "invalid-reference-edge", true, status.Error(codes.FailedPrecondition, "resource is referenced by credential:"+credentialID)},
	}
	for _, test := range negative {
		details, ok := procedureInferenceCleanupRetainedByAudit(&fresh, test.target, map[string]struct{}{}, test.err)
		if ok || details["rejectionCategory"] != test.category {
			t.Fatalf("rejected reference category=%v accepted=%t, want %s", details, ok, test.category)
		}
		encoded, err := json.Marshal(details)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), `"idHash":"`) != test.hashedEvidence {
			t.Fatalf("rejection evidence hash presence=%t, want %t: %s", strings.Contains(string(encoded), `"idHash":"`), test.hashedEvidence, encoded)
		}
		for _, rawID := range []string{grantID, usageID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"} {
			if strings.Contains(string(encoded), rawID) {
				t.Fatalf("rejection evidence leaked raw reference ID %s: %s", rawID, encoded)
			}
		}
	}
}

func TestProcedureInferencePackageCleanupIsExplicitlyRetainedByDesign(t *testing.T) {
	resources := &ProcedureInferenceResources{PackageID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Cleanup: []SemanticOperation{{Action: "retain", Resource: "inference-package", Status: "planned"}}}
	setProcedureInferenceCleanupStatus(resources, "inference-package", "retained-by-design", "", map[string]any{"retentionCategory": "catalog-package-no-delete-api"})
	cleanup := resources.Cleanup[0]
	if cleanup.Status != "retained-by-design" || cleanup.Action != "retain" || cleanup.Details["retentionCategory"] != "catalog-package-no-delete-api" || cleanup.Error != "" {
		t.Fatalf("package cleanup=%+v", cleanup)
	}
}

func TestPlanProcedureInferenceCleanupIncludesRetainedPackage(t *testing.T) {
	scenarioPath := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-procedure-inference-control-emulator", "compose-procedure-inference-control-emulator.yaml")
	scenario, err := catalog.ResolveScenarioFile(scenarioPath, catalog.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "openai-compatible-procedure-emulator.yaml")
	profileSpec, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolvedProfile, err := provider.Resolve(profileSpec, source, provider.ResolveOptions{DryRun: true, RequiredCapabilities: []string{"generation"}, Manifest: provider.BuiltInHarnessManifest()})
	if err != nil {
		t.Fatal(err)
	}
	scenario.ProviderProfile = &resolvedProfile
	resources, err := PlanScenario(scenario, Options{RunID: "compose-procedure-inference-control-emulator-20261024-000000", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if resources.ProcedureInference == nil || len(resources.ProcedureInference.Cleanup) != 8 {
		t.Fatalf("procedure inference cleanup=%+v", resources.ProcedureInference)
	}
	packageCleanup := resources.ProcedureInference.Cleanup[7]
	if packageCleanup.Sequence != 8 || packageCleanup.Action != "retain" || packageCleanup.Resource != "inference-package" || packageCleanup.Status != "planned" {
		t.Fatalf("package cleanup plan=%+v", packageCleanup)
	}
}

func TestProcedureInferenceCredentialGrantRequestRestrictsDelegationToWriter(t *testing.T) {
	resources := &ProcedureInferenceResources{SpaceID: "space", DomainID: "domain", CredentialID: "credential", EndpointID: "endpoint", ModelID: "model"}
	request := procedureInferenceCredentialGrantRequest(resources, " writer-principal ")
	if request == nil || !reflect.DeepEqual(request.GetAllowOnBehalfOfPrincipalIds(), []string{"writer-principal"}) {
		t.Fatalf("allow_on_behalf_of_principal_ids=%v", request.GetAllowOnBehalfOfPrincipalIds())
	}
	if request.GetSpaceId() != "space" || request.GetCredentialId() != "credential" || request.GetScope().GetDomainId() != "domain" || !request.GetAllowBackgroundUse() {
		t.Fatalf("grant request=%+v", request)
	}
}

func TestProcedureInferenceTargetPrincipalID(t *testing.T) {
	resources := &ScenarioResources{
		ProcedureInference: &ProcedureInferenceResources{TargetActorID: "writer"},
		Assignments: []ActorAssignment{
			{ActorID: "other", PrincipalID: "principal-other"},
			{ActorID: "writer", PrincipalID: " principal-writer "},
		},
	}
	if got := procedureInferenceTargetPrincipalID(resources); got != "principal-writer" {
		t.Fatalf("procedureInferenceTargetPrincipalID()=%q", got)
	}
	resources.Assignments[1].PrincipalID = ""
	if got := procedureInferenceTargetPrincipalID(resources); got != "" {
		t.Fatalf("procedureInferenceTargetPrincipalID()=%q, want empty", got)
	}
}

func TestPlanScenarioCreatesDeterministicSafeSemanticPlan(t *testing.T) {
	scenario, err := catalog.ResolveScenarioFile(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml"), catalog.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := provider.LoadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := provider.Resolve(profile, source, provider.ResolveOptions{DryRun: true, Manifest: provider.BuiltInHarnessManifest()})
	if err != nil {
		t.Fatal(err)
	}
	scenario.ProviderProfileRef = "deterministic-token-basis-v1"
	scenario.ProviderProfile = &resolved
	scenario.Semantic = &spec.SemanticScenarioSpec{
		TargetActorGroup: "journal-users",
		Rule:             spec.SemanticRuleSpec{Key: "fixture-search", DisplayName: "Fixture search", TargetLabels: []string{"Document"}, BindingKey: "search", Purpose: "semantic_search", SourceMode: "self", PhysicalIndex: "exact", MaxBatchSize: 16, WorkerConcurrency: 1},
		Maintenance:      spec.SemanticMaintenanceSpec{Timeout: spec.Duration{Duration: time.Minute}, PollInterval: spec.Duration{Duration: time.Second}, AnalyzeLimit: 64, ProcessLimit: 64},
	}
	first, err := PlanScenario(scenario, Options{RunID: "semantic-contract-fixed", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanScenario(scenario, Options{RunID: "semantic-contract-fixed", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if first.Semantic == nil || !reflect.DeepEqual(first.Semantic, second.Semantic) {
		t.Fatalf("semantic plans are not deterministic: first=%#v second=%#v", first.Semantic, second.Semantic)
	}
	wantOperations := []string{"inference-package", "intelligence-profile", "credential", "credential-grant", "access-policy", "semantic-rule"}
	for index, want := range wantOperations {
		if got := first.Semantic.Operations[index]; got.Sequence != index+1 || got.Resource != want || got.Status != "planned" {
			t.Fatalf("operation[%d]=%#v want resource %q", index, got, want)
		}
	}
	wantCleanup := []string{"semantic-rule", "access-policy", "credential-grant", "credential", "intelligence-profile", "capability", "vector-store", "model", "endpoint"}
	for index, want := range wantCleanup {
		if got := first.Semantic.Cleanup[index]; got.Sequence != index+1 || got.Resource != want {
			t.Fatalf("cleanup[%d]=%#v want resource %q", index, got, want)
		}
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "Password") || strings.Contains(string(raw), "secretValue") {
		t.Fatalf("semantic plan contains secret-bearing fields: %s", raw)
	}
}
