package runner

import (
	"context"
	"path/filepath"
	"testing"
)

func semanticFaultScenarioNames() []string {
	out := []string{}
	for _, name := range []string{"rate-limit", "server-error", "transport", "malformed", "truncated", "dimensions", "refusal", "deadline", "cancel"} {
		out = append(out, "compose-semantic-fault-"+name)
	}
	return out
}
func TestSemanticFailureSuiteHasNineCredentialFreeCases(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "suites", "semantic-provider-failure-matrix.yaml")
	result, err := RunSuiteFile(context.Background(), path, Options{DryRun: true, ArtifactRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	want := semanticFaultScenarioNames()
	if len(result.Results) != len(want) {
		t.Fatal("failure matrix cases missing")
	}
	for i, name := range want {
		if result.Results[i].ScenarioName != name || result.Results[i].Status != RunPassed {
			t.Fatalf("matrix result[%d]=%+v", i, result.Results[i])
		}
	}
}
