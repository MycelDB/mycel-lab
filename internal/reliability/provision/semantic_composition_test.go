package provision

import (
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestSemanticCompositionCannotBypassCatalogViaResolvedScenario(t *testing.T) {
	stage := provider.StageSpec{Name: "enrich", Capability: "generation", ProfileRef: "generation-only", InputFrom: "graph"}
	scenario := spec.ResolvedScenario{Semantic: &spec.SemanticScenarioSpec{ProviderStages: []provider.StageSpec{stage}}}
	if validateSemanticCompositionRuntime(scenario) == nil {
		t.Fatal("missing plan accepted")
	}
	scenario.ProviderComposition = &provider.CompositionPlan{SchemaVersion: 1, Status: "planned", Stages: []provider.CompositionStage{{StageSpec: stage, RequiredAdapter: "semantic-stage/embedding/openai-compatible"}}}
	if validateSemanticCompositionRuntime(scenario) == nil {
		t.Fatal("forged ready plan bypassed native adapter gate")
	}
	stage = provider.StageSpec{Name: "embed", Capability: "embedding", ProfileRef: "embeddings", InputFrom: "graph"}
	scenario.Semantic.ProviderStages = []provider.StageSpec{stage}
	scenario.ProviderComposition.Stages = []provider.CompositionStage{{StageSpec: stage, RequiredAdapter: "semantic-stage/embedding/openai-compatible"}}
	if err := validateSemanticCompositionRuntime(scenario); err != nil {
		t.Fatal(err)
	}
}
