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

func TestComposeSemanticContractTimingCanReachMaintenanceDeadline(t *testing.T) {
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
