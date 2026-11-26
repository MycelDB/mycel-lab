package releasebundle

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestProtectedClusterGateJobReservesFinalizationTime(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	draft, err := os.ReadFile(filepath.Join(root, "tests", "reliability", "release-bundles", "cluster-release-gate.draft.yaml.tmpl"))
	if err != nil {
		t.Fatal(err)
	}
	var bundle Bundle
	dec := yaml.NewDecoder(bytes.NewReader(draft))
	dec.KnownFields(true)
	if err := dec.Decode(&bundle); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release-gate.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			TimeoutMinutes int `yaml:"timeout-minutes"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	const workflowOverhead = 20 * time.Minute // setup, bundle build, evidence upload and verdict
	// RunSuiteFile uses an additional suite preflight bounded by the longest
	// scenario; individual outer deadlines do not bound finalization work.
	need := workflowOverhead + bundle.MaximumOuterTimeout()
	for _, scenario := range bundle.Scenarios {
		f := scenario.Finalization
		need += scenario.OuterTimeout + f.SensitiveHostCleanup + f.ActorStop + f.ProviderObservation +
			f.ClusterAssertions + f.FinalOracle + f.PortForwardStop + f.ScenarioCleanup + f.EnvironmentDelete + f.DeletionVerification
	}
	budget := time.Duration(workflow.Jobs["cluster-release-gate"].TimeoutMinutes) * time.Minute
	if budget > 6*time.Hour || need > budget {
		t.Fatalf("cluster gate needs %s including setup and cleanup, job budget is %s (GitHub-hosted maximum is 6h)", need, budget)
	}
}
