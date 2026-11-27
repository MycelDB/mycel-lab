package runner

import (
	"errors"
	"slices"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/semanticbench"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func validateSemanticScalingScenario(scenario spec.ResolvedScenario, image string, bound bool) error {
	for _, phase := range scenario.Phases {
		for _, event := range phase.Events {
			if event.Type != spec.SemanticScalingEvent {
				continue
			}
			if bound || scenario.Environment.Driver != "compose" || scenario.Cluster.Nodes != 3 || !env.IsDigestPinnedImageReference(image) {
				return errors.New("semantic scaling requires a digest-pinned three-node Compose scenario outside release bundles")
			}
			if scenario.Semantic == nil {
				return errors.New("semantic scaling requires a semantic rule")
			}
			rule := scenario.Semantic.Rule
			if !rule.DeferProvisioning || rule.PhysicalIndex != "exact" || rule.SourceMode != "self" || !slices.Equal(rule.TargetLabels, []string{"Document"}) || !slices.Equal(rule.IncludeProperties, []string{"text"}) || len(rule.ExcludeProperties) > 0 {
				return errors.New("semantic scaling requires the deferred exact self-projection fixture")
			}
			if scenario.ProviderProfile == nil || scenario.ProviderProfile.Profile.Mode != provider.ModeDeterministic {
				return errors.New("semantic scaling requires a deterministic provider fixture")
			}
			embedding := scenario.ProviderProfile.Profile.Capabilities.Embedding
			if embedding == nil || embedding.ConnectorType != "fake" || embedding.FixtureStrategy != "token-basis-v1" || embedding.Dimensions != 8 {
				return errors.New("semantic scaling requires the local eight-dimensional token-basis-v1 fixture")
			}
		}
	}
	return nil
}

func (r *runRecorder) writeSemanticScalingReport(report semanticbench.Report) error {
	report.Environment.LabRevision = r.harnessIdentity.VCS["vcs.revision"]
	report.Environment.LabModified = r.harnessIdentity.VCS["vcs.modified"] == "true"
	report.Environment.HarnessSHA256 = r.harnessIdentity.ExecutableSHA256
	if _, err := r.sink.WriteJSON("benchmark/semantic-search-scaling.json", report); err != nil {
		return err
	}
	_, err := r.sink.WriteText("benchmark/semantic-search-scaling.md", report.Markdown())
	return err
}
