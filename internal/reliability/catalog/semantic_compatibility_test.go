package catalog

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func TestSemanticCompatibilityCasesResolveAndRetainStrictBoundaries(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	command := exec.Command("python3", filepath.Join(root, "scripts", "generateSemanticCompatibilityCases.py"), "--check")
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compatibility fixtures drift: %s %v", raw, err)
	}
	for _, name := range []string{"compose-semantic-domain-isolation", "compose-semantic-model-migration"} {
		t.Run(name, func(t *testing.T) {
			scenario, err := ResolveScenarioFile(filepath.Join(root, "tests", "reliability", "scenarios", name, name+".yaml"), ResolveOptions{ProviderDryRun: true, ProviderLookupEnv: func(string) (string, bool) { t.Fatal("deterministic fixture read credentials"); return "", false }})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.Cluster.Image != semanticV0214Image || scenario.Environment.Options["imageOverride"] != semanticV0214Image || scenario.ProviderProfile.Profile.Mode != provider.ModeDeterministic || len(scenario.Phases) != 5 || scenario.ActorGroups[0].Count != 3 {
				t.Fatal("fixture identity, topology, or phase contract drift")
			}
			if name == "compose-semantic-model-migration" {
				rebuilds, dimensions, spaces := 0, 0, 0
				for phaseIndex, phase := range scenario.Phases {
					fenced, rejected := false, false
					for _, event := range phase.Events {
						if event.Type == "semantic-backfill" && phaseIndex > 0 {
							t.Fatal("migration uses repair backfill rather than explicit fenced rebuild")
						}
						if event.Type == "semantic-reliability-assert" && event.Target["expectRebuildRequired"] == true && event.Target["requireGenerationAfter"] != nil {
							fenced = true
						}
						if event.Type == "semantic-search-assert" && event.Target["expectRebuildFence"] == true {
							rejected = true
						}
						if event.Type == "semantic-rebuild" {
							if !fenced || !rejected {
								t.Fatal("rebuild precedes incompatibility and rejection evidence")
							}
							rebuilds++
						}
						if event.Type == "semantic-model-config-update" {
							_, dim := event.Target["dimensions"]
							_, space := event.Target["vectorSpaceKey"]
							if dim == space {
								t.Fatal("dimension and space changes are not independent")
							}
							if dim {
								dimensions++
							} else {
								spaces++
							}
						}
					}
				}
				if rebuilds != 3 || dimensions != 1 || spaces != 1 {
					t.Fatal("model, dimension, or vector-space transition missing")
				}
			} else {
				proofs := 0
				for _, phase := range scenario.Phases {
					for _, event := range phase.Events {
						if event.Type == "semantic-domain-isolation-assert" {
							proofs++
							if event.Target["primaryQuery"] == event.Target["peerQuery"] {
								t.Fatal("indistinguishable domain corpus")
							}
						}
					}
				}
				if proofs != 3 {
					t.Fatal("initial, post-peer-update, or retained-restart isolation proof missing")
				}
			}
		})
	}
}
