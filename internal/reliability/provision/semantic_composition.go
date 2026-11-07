package provision

import (
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

// Defend the provisioning boundary too: direct callers of PlanScenario must not
// bypass catalog capability checks by supplying a forged resolved composition.
func validateSemanticCompositionRuntime(scenario spec.ResolvedScenario) error {
	if scenario.Semantic == nil || len(scenario.Semantic.ProviderStages) == 0 {
		return nil
	}
	stages := scenario.Semantic.ProviderStages
	plan := scenario.ProviderComposition
	if plan == nil || len(plan.Stages) != len(stages) {
		return errors.New("semantic composition requires its validated resolved plan")
	}
	if err := plan.RequireExecutable(); err != nil {
		return err
	}
	adapters := provider.SemanticStageAdapters()
	for i, stage := range plan.Stages {
		if stage.StageSpec != stages[i] || stage.Capability != "embedding" || !adapters[stage.RequiredAdapter] {
			return &provider.CompositionCapabilityError{Missing: []string{stage.RequiredAdapter}}
		}
	}
	if len(stages) != 1 {
		return errors.New("native semantic composition has no implemented pipeline adapter")
	}
	return nil
}
