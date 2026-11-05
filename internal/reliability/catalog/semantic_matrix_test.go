package catalog

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"gopkg.in/yaml.v3"
)

func TestSemanticFaultMatrixDefinitionsResolveWithoutCredentials(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	command := exec.Command("python3", filepath.Join(root, "scripts", "generateSemanticFaultMatrix.py"), "--check")
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("matrix drift: %s %v", raw, err)
	}
	names := []string{"rate-limit", "server-error", "transport", "malformed", "truncated", "dimensions", "refusal", "deadline", "cancel", "out-of-order"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			scenario := "compose-semantic-fault-" + name
			if name == "out-of-order" {
				scenario = "compose-semantic-out-of-order"
			}
			path := filepath.Join(root, "tests", "reliability", "scenarios", scenario, scenario+".yaml")
			resolved, err := ResolveScenarioFile(path, ResolveOptions{ProviderDryRun: true, ProviderLookupEnv: func(string) (string, bool) { t.Fatal("credential lookup in local matrix"); return "", false }})
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Cluster.Image != semanticV0214Image || resolved.Environment.Options["imageOverride"] != semanticV0214Image || len(resolved.ActorGroups) != 1 || resolved.ActorGroups[0].Count != 3 {
				t.Fatal("identity/authentication fixture drift")
			}
			if len(resolved.Phases) != 3 || resolved.ProviderProfile.Profile.Mode != provider.ModeEmulator {
				t.Fatal("matrix is not credential-free bounded emulator coverage")
			}
		})
	}
}
func TestSemanticCompositionBlocksUnsupportedNativeStageBeforeCredentialLookup(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "reliability")
	path := filepath.Join(root, "scenarios", "compose-semantic-graph-contract", "compose-semantic-graph-contract.yaml")
	loaded, err := spec.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	scenario := loaded.(spec.Scenario)
	scenario.Semantic.ProviderStages = []provider.StageSpec{{Name: "enrich", Capability: "generation", ProfileRef: "openai-compatible-procedure-emulator", InputFrom: "graph"}, {Name: "embed", Capability: "embedding", ProfileRef: scenario.ProviderProfileRef, InputFrom: "enrich"}}
	// Use a genuine live generation profile shape so an accidental stage
	// resolution would attempt credential lookup and fail this regression.
	live, _, err := provider.LoadFile(filepath.Join(root, "provider-profiles", "openai-compatible-live.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	capability := *live.Capabilities.Embedding
	capability.Dimensions, capability.VectorSpaceKey = 0, ""
	live.Metadata.Name = "composition-live-generation"
	live.Capabilities = provider.Capabilities{Generation: &capability}
	raw, err := yaml.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}
	livePath := filepath.Join(t.TempDir(), "generation.yaml")
	if err := os.WriteFile(livePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	scenario.Semantic.ProviderStages[0].ProfileRef = livePath
	for _, dry := range []bool{true, false} {
		_, err := ResolveScenario(scenario, filepath.Dir(path), ResolveOptions{ProviderDryRun: dry, ProviderLookupEnv: func(string) (string, bool) { t.Fatal("unsupported composition read a credential"); return "", false }})
		var missing *provider.CompositionCapabilityError
		if !errors.As(err, &missing) {
			t.Fatalf("missing typed fail-closed capability gate: %v", err)
		}
	}
	scenario.Semantic.ProviderStages = scenario.Semantic.ProviderStages[1:]
	scenario.Semantic.ProviderStages[0].InputFrom = "graph"
	resolved, err := ResolveScenario(scenario, filepath.Dir(path), ResolveOptions{ProviderDryRun: true})
	if err != nil || resolved.ProviderComposition == nil || resolved.ProviderComposition.RequireExecutable() != nil {
		t.Fatalf("direct-stage compatibility broke: %v", err)
	}
}
