package releasebundle

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"gopkg.in/yaml.v3"
)

// Review draft completeness against the actual catalog, without Docker, hosted
// providers, provisioning, a candidate capture, or destructive execution.
func TestSemanticReleaseGateDraftBindsAllSuiteInputsInOrder(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	draftPath := filepath.Join(root, "tests", "reliability", "release-bundles", "semantic-graph-index-release-gate.draft.yaml.tmpl")
	raw, err := os.ReadFile(draftPath)
	if err != nil {
		t.Fatal(err)
	}
	var draft Bundle
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&draft); err != nil {
		t.Fatal(err)
	}
	if draft.Images.ProviderEmulator == nil || !draft.Images.ProviderEmulator.Local || draft.Images.ProviderEmulator.AnonymousPull || draft.Images.ProviderEmulator.Reference != "local/mycel-provider-emulator:issue46-reviewed" {
		t.Fatal("draft must bind a non-default locally built emulator by exact image identity at capture")
	}
	if draft.Images.Daemon.Local || draft.Images.Daemon.Reference != "" || !draft.Images.Daemon.AnonymousPull {
		t.Fatal("daemon must be supplied as an immutable registry image at capture")
	}
	for _, helper := range draft.Images.Helpers {
		if helper.Local || !helper.AnonymousPull || helper.RuntimeEnvironmentVariable == "" {
			t.Fatal("helpers must be registry-pinned and environment-bound")
		}
	}
	suitePath := "tests/reliability/suites/semantic-graph-index-release-gate.yaml"
	suiteRaw, err := os.ReadFile(filepath.Join(root, suitePath))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := spec.Load(suiteRaw)
	if err != nil {
		t.Fatal(err)
	}
	suite := parsed.(spec.Suite)
	if len(suite.Scenarios) != 16 || len(draft.Scenarios) != 16 {
		t.Fatalf("suite/draft scenarios=%d/%d, want 16", len(suite.Scenarios), len(draft.Scenarios))
	}
	bound := map[string]string{}
	for _, file := range draft.Files {
		key := file.Source + ":" + file.Path
		if old, exists := bound[key]; exists {
			t.Fatalf("duplicate bound file %s, roles %s/%s", key, old, file.Role)
		}
		bound[key] = file.Role
		path := filepath.Join(root, file.Path)
		if file.Source == "daemon" {
			path = filepath.Join(root, "..", "mycel", file.Path)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing bound file %s: %v", key, err)
		}
	}
	require := func(source, role, path string) {
		t.Helper()
		if bound[source+":"+path] != role {
			t.Fatalf("missing %s %s:%s", role, source, path)
		}
	}
	require("lab", "source", "tests/reliability/release-bundles/semantic-graph-index-release-gate.draft.yaml.tmpl")
	require("lab", "suite", suitePath)
	require("lab", "provenance", "Dockerfile.emulator")
	require("daemon", "provenance", "Dockerfile")
	require("lab", "runtime-config", "tests/compose/semantic-pending-restart.yml")
	seen := map[string]bool{}
	phases := 0
	for index, ref := range suite.Scenarios {
		scenarioPath := filepath.ToSlash(filepath.Clean(filepath.Join("tests/reliability/suites", ref.Path)))
		item := draft.Scenarios[index]
		if item.ScenarioPath != scenarioPath || item.SuitePath != suitePath || item.Source != "lab" || seen[item.Name] {
			t.Fatalf("draft case %d misbound: %+v", index, item)
		}
		seen[item.Name] = true
		require("lab", "scenario", scenarioPath)
		resolved, err := catalog.ResolveScenarioFile(filepath.Join(root, scenarioPath), catalog.ResolveOptions{ProviderDryRun: true})
		if err != nil {
			t.Fatalf("resolve %s: %v", scenarioPath, err)
		}
		if item.Name != resolved.Metadata.Name || resolved.ProviderProfile == nil || resolved.ProviderProfile.Profile.Mode == "live" {
			t.Fatalf("case %d has changed name/profile", index)
		}
		require("lab", "provider-profile", "tests/reliability/provider-profiles/"+resolved.ProviderProfile.Profile.Metadata.Name+".yaml")
		phases += len(resolved.Phases)
		if item.Environment.Driver != "compose" || item.Environment.KeepOnFailure || len(item.Environment.Options) != 3 || item.Environment.Options["projectNamePrefix"] != "draft" || item.Environment.Namespace != "draft" || item.OuterTimeout <= 0 || item.OuterTimeout > 6*time.Hour {
			t.Fatalf("unsafe case %s environment/bounds: %+v", item.Name, item)
		}
		var wantFiles []string
		for _, value := range resolved.Environment.Options["composeFiles"].([]any) {
			path := value.(string)
			if strings.HasPrefix(path, "../mycel/") {
				wantFiles = append(wantFiles, "daemon:"+strings.TrimPrefix(path, "../mycel/"))
			} else {
				wantFiles = append(wantFiles, "lab:"+path)
			}
		}
		var gotFiles []string
		for _, file := range item.Environment.RuntimeFiles {
			require(file.Source, "runtime-config", file.Path)
			if file.Option != "composeFiles" {
				t.Fatalf("unbound option in %s: %s", item.Name, file.Option)
			}
			gotFiles = append(gotFiles, file.Source+":"+file.Path)
		}
		if !reflect.DeepEqual(gotFiles, wantFiles) {
			t.Fatalf("case %s compose inputs %v != %v", item.Name, gotFiles, wantFiles)
		}
		// The protocol case intentionally uses distinct ports; never collide it
		// with the other 15 cases' 19091–19093 host bindings.
		wantPorts := []any{19091, 19092, 19093}
		if item.Name == "compose-semantic-openai-emulator" {
			wantPorts = []any{19791, 19792, 19793}
		}
		if !reflect.DeepEqual(item.Environment.Options["grpcPorts"], wantPorts) {
			t.Fatalf("case %s ports %v != %v", item.Name, item.Environment.Options["grpcPorts"], wantPorts)
		}
	}
	if phases != 65 {
		t.Fatalf("phase count=%d, want 65", phases)
	}
}
