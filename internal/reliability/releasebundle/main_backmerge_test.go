package releasebundle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCanonicalSemanticLiveWorkflowAndDraftUseSameBoundSuite(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "tests", "reliability", "release-bundles", "semantic-live-provider.draft.yaml.tmpl"))
	if err != nil {
		t.Fatal(err)
	}
	var draft Bundle
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&draft); err != nil {
		t.Fatal(err)
	}
	const suite = "tests/reliability/suites/semantic-graph-index-live.yaml"
	if len(draft.Scenarios) != 1 || draft.Scenarios[0].Name != "compose-semantic-live-provider" || draft.Scenarios[0].SuitePath != suite {
		t.Fatal("live bundle draft does not bind the canonical live suite")
	}
	found := false
	for _, file := range draft.Files {
		if file.Role == "suite" && file.Path == suite && file.Source == "lab" {
			found = true
		}
	}
	if !found {
		t.Fatal("canonical live suite is not hashed as a bound input")
	}
	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "semantic-live.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"$RUNNER_TEMP/bin/mycel-lab" run suite semantic-graph-index-live`, `--release-bundle ../release-bundle.yaml`, `--confirm-destructive`} {
		if !strings.Contains(string(workflow), required) {
			t.Fatalf("protected live workflow lost %s", required)
		}
	}
}
