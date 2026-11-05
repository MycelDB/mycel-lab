package runner

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

// Develop's new public suite names must retain main's fail-closed candidate
// bundle requirement. These tests use recording drivers, never Docker or RPCs.
func TestSemanticGateNamesPreserveMainHermeticBundleRequirement(t *testing.T) {
	for _, name := range []string{"semantic-live-provider", "semantic-graph-index-live", "semantic-graph-index-release-gate"} {
		t.Run(name, func(t *testing.T) {
			driver := &recordingDriver{omitEndpoints: true}
			provisioned := false
			result, err := RunSuiteFile(context.Background(), filepath.Join("..", "..", "..", "tests", "reliability", "suites", name+".yaml"), Options{ArtifactRoot: t.TempDir(), ConfirmDestructive: true, EnvironmentDriver: driver, scenarioProvision: func(context.Context, *provision.ScenarioResources, provision.Options) error {
				provisioned = true
				return nil
			}})
			if err == nil || !strings.Contains(err.Error(), "requires a hermetic release execution bundle") {
				t.Fatalf("suite %s did not fail at the candidate bundle guard: %v", name, err)
			}
			if provisioned || len(driver.order) != 0 {
				t.Fatalf("suite %s reached provisioning before bundle validation", name)
			}
			if len(result.Results) != 1 || result.Results[0].Status != RunFailed {
				t.Fatalf("suite %s did not retain a failed first-member result", name)
			}
		})
	}
}

func TestSemanticGateNamesStillAllowExplicitPlanningWithoutRuntime(t *testing.T) {
	for _, test := range []struct {
		name              string
		scenarios, phases int
	}{{"semantic-graph-index-live", 1, 4}, {"semantic-graph-index-release-gate", 16, 65}} {
		t.Run(test.name, func(t *testing.T) {
			result, err := RunSuiteFile(context.Background(), filepath.Join("..", "..", "..", "tests", "reliability", "suites", test.name+".yaml"), Options{DryRun: true, ArtifactRoot: t.TempDir()})
			if err != nil || !result.DryRun || len(result.Results) != test.scenarios {
				t.Fatalf("planning failed: %v", err)
			}
			phases := 0
			for _, run := range result.Results {
				if !run.DryRun || run.Status != RunPassed || run.ProviderBudget == nil || run.ProviderBudget.ProviderRequests != 0 {
					t.Fatal("ordinary planning claimed runtime or provider execution")
				}
				phases += len(run.Phases)
			}
			if phases != test.phases {
				t.Fatalf("planning phases=%d, want %d", phases, test.phases)
			}
		})
	}
}
