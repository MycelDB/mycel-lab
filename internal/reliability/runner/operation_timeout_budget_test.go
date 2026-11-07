package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func TestCanonicalScenariosEventTimeoutsFitActualOperationBudget(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range paths {
		scenario, err := catalog.ResolveScenarioFile(path, catalog.ResolveOptions{
			ProviderDryRun: true, DeferLiveProviderValidation: true,
			ProviderLookupEnv: func(string) (string, bool) {
				t.Fatal("event budget preflight must not look up provider credentials")
				return "", false
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if scenario.ProviderProfile == nil {
			continue
		}
		checked++
		t.Run(scenario.Metadata.Name, func(t *testing.T) {
			if err := validateOperationTimeoutBudget(scenario); err != nil {
				t.Fatal(err)
			}
		})
	}
	if checked < 18 {
		t.Fatalf("only checked %d provider-backed scenarios", checked)
	}
}

func TestOperationTimeoutBudgetRejectsClippedWaitWithoutWeakeningIt(t *testing.T) {
	scenario := canonicalComposeScenario(t, "compose-semantic-pending-restart")
	profile := *scenario.ProviderProfile
	profile.Profile.Budgets.RequestTimeout = provider.Duration{Duration: 45 * time.Second}
	scenario.ProviderProfile = &profile
	if err := validateOperationTimeoutBudget(scenario); err == nil || !strings.Contains(err.Error(), "cluster-recovery-wait: timeout 1m0s must be below") {
		t.Fatalf("clipped 60s recovery wait accepted: %v", err)
	}
	profile.Profile.Budgets.RequestTimeout = provider.Duration{Duration: time.Minute}
	if err := validateOperationTimeoutBudget(scenario); err == nil || !strings.Contains(err.Error(), "timeout 1m0s must be below") {
		t.Fatalf("equal timeout accepted without scheduling margin: %v", err)
	}
	profile.Profile.Budgets.RequestTimeout = provider.Duration{Duration: 75 * time.Second}
	if err := validateOperationTimeoutBudget(scenario); err != nil {
		t.Fatalf("declared 60s wait should be available under 75s budget: %v", err)
	}
}

func TestCanonicalReusableRecoveryWaitsKeepSingleLoginAndStablePollBudgets(t *testing.T) {
	count := 0
	for _, name := range []string{"compose-semantic-pending-restart", "compose-semantic-domain-isolation", "compose-semantic-model-migration", "compose-procedure-inference-control-emulator"} {
		scenario := canonicalComposeScenario(t, name)
		for _, phase := range scenario.Phases {
			for _, event := range phase.Events {
				if event.Type != "cluster-recovery-wait" || event.Target["reuseEndpointSessions"] != true {
					continue
				}
				count++
				if event.Target["timeout"] != "60s" || event.Target["commandTimeout"] != "3s" || event.Target["sessionInitTimeout"] != "20s" || event.Target["minimumStablePolls"] != 3 {
					t.Fatalf("%s %s changed recovery/session assertions: %v", name, phase.Name, event.Target)
				}
			}
		}
		if err := validateOperationTimeoutBudget(scenario); err != nil {
			t.Fatalf("%s session policy exceeded operation budget: %v", name, err)
		}
	}
	if count != 5 {
		t.Fatalf("covered %d reusable recovery waits, want five across four scenarios", count)
	}
}

func TestRecoverySessionInitializationBudgetRejectsImpossibleAndUnauthenticatedPlans(t *testing.T) {
	scenario := canonicalComposeScenario(t, "compose-semantic-pending-restart")
	for phase := range scenario.Phases {
		for event := range scenario.Phases[phase].Events {
			probe := &scenario.Phases[phase].Events[event]
			if probe.Type != "cluster-recovery-wait" {
				continue
			}
			original := probe.Target["sessionInitTimeout"]
			probe.Target["sessionInitTimeout"] = "58s"
			if err := validateOperationTimeoutBudget(scenario); err == nil || !strings.Contains(err.Error(), "leave time for concurrent endpoint logins") {
				t.Fatalf("58s session initialization accepted without recovery polling budget: %v", err)
			}
			probe.Target["sessionInitTimeout"] = "nonsense"
			if err := validateOperationTimeoutBudget(scenario); err == nil || !strings.Contains(err.Error(), "sessionInitTimeout") {
				t.Fatalf("invalid session initialization limit accepted: %v", err)
			}
			probe.Target["sessionInitTimeout"] = original
			probe.Target["reuseEndpointSessions"] = false
			if err := validateOperationTimeoutBudget(scenario); err == nil || !strings.Contains(err.Error(), "requires reusable endpoint sessions") {
				t.Fatalf("session creation budget without reusable sessions accepted: %v", err)
			}
			probe.Target["reuseEndpointSessions"] = true
		}
	}
	if err := validateOperationTimeoutBudget(scenario); err != nil {
		t.Fatalf("restored 20s session initialization and 60s recovery budget rejected: %v", err)
	}
}

func TestSuiteRejectsLaterImpossibleSessionInitializationBeforeProvisioning(t *testing.T) {
	original := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-pending-restart", "compose-semantic-pending-restart.yaml")
	raw, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	modified := strings.Replace(string(raw), "sessionInitTimeout: 20s", "sessionInitTimeout: 58s", 1)
	if modified == string(raw) {
		t.Fatal("pending restart lost its explicit session initialization budget")
	}
	second := filepath.Join(t.TempDir(), "later.yaml")
	if err := os.WriteFile(second, []byte(modified), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := filepath.Abs(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-recovery", "compose-semantic-graph-recovery.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	suite := filepath.Join(t.TempDir(), "budget-suite.yaml")
	body := fmt.Sprintf("apiVersion: myceldb.io/reliability/v1\nkind: Suite\nmetadata:\n  name: session-budget-preflight\nscenarios:\n  - path: %q\n  - path: %q\nexecution:\n  mode: sequential\n  stopOnFailure: true\n", first, second)
	if err := os.WriteFile(suite, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := t.TempDir()
	result, err := RunSuiteFile(context.Background(), suite, Options{DryRun: true, ArtifactRoot: artifacts, ProfileDirs: []string{filepath.Join("..", "..", "..", "tests", "reliability")}})
	if err == nil || !strings.Contains(err.Error(), "leave time for concurrent endpoint logins") || len(result.Results) != 0 {
		t.Fatalf("later impossible session plan must stop before provisioning: results=%+v error=%v", result, err)
	}
	entries, err := os.ReadDir(artifacts)
	if err != nil || len(entries) != 0 {
		t.Fatalf("preflight wrote artifacts before rejection: entries=%v error=%v", entries, err)
	}
}

func TestSuiteRejectsLaterClippedWaitBeforeFirstScenario(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-pending-restart", "compose-semantic-pending-restart.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	modified := strings.Replace(string(raw), "timeout: 60s", "timeout: 90s", 1)
	if modified == string(raw) {
		t.Fatal("pending-restart recovery wait changed")
	}
	second := filepath.Join(t.TempDir(), "later.yaml")
	if err := os.WriteFile(second, []byte(modified), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := filepath.Abs(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-recovery", "compose-semantic-graph-recovery.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	suite := filepath.Join(t.TempDir(), "budget-suite.yaml")
	body := fmt.Sprintf("apiVersion: myceldb.io/reliability/v1\nkind: Suite\nmetadata:\n  name: operation-budget-preflight\nscenarios:\n  - path: %q\n  - path: %q\nexecution:\n  mode: sequential\n  parallelism: 1\n  stopOnFailure: true\n", first, second)
	if err := os.WriteFile(suite, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := t.TempDir()
	out, err := RunSuiteFile(context.Background(), suite, Options{DryRun: true, ArtifactRoot: artifacts, ProfileDirs: []string{filepath.Join("..", "..", "..", "tests", "reliability")}})
	if err == nil || !strings.Contains(err.Error(), "operation requestTimeout") || len(out.Results) != 0 {
		t.Fatalf("later clipped wait must stop suite before its first run: result=%+v err=%v", out, err)
	}
	entries, err := os.ReadDir(artifacts)
	if err != nil || len(entries) != 0 {
		t.Fatalf("preflight wrote run artifacts before rejecting wait budget: entries=%v err=%v", entries, err)
	}
}
