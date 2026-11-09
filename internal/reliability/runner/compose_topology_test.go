package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func canonicalComposeScenario(t *testing.T, name string) spec.ResolvedScenario {
	t.Helper()
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", name, name+".yaml")
	resolved, err := catalog.ResolveScenarioFile(path, catalog.ResolveOptions{ProviderDryRun: true, DeferLiveProviderValidation: true, ProviderLookupEnv: func(string) (string, bool) {
		t.Fatal("Raft fixture audit must not look up provider credentials")
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestAllCanonicalComposeRaftInputsMatchResolvedProfiles(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := spec.Load(raw)
		if err != nil {
			t.Fatal(err)
		}
		scenario, ok := loaded.(spec.Scenario)
		if !ok || scenario.Environment.Driver != "compose" {
			continue
		}
		count++
		t.Run(scenario.Metadata.Name, func(t *testing.T) {
			resolved := canonicalComposeScenario(t, scenario.Metadata.Name)
			if err := validateComposeRaftTopology(resolved, path); err != nil {
				t.Fatal(err)
			}
		})
	}
	if count != 27 {
		t.Fatalf("audited %d Compose scenarios, want 27; review newly added/removed scenarios", count)
	}
}

func TestComposeRaftTopologyRejectsMismatchAndOverride(t *testing.T) {
	scenario := canonicalComposeScenario(t, "compose-semantic-graph-recovery")
	if scenario.Cluster.Raft.PartitionCount != 16 {
		t.Fatal("baseline Compose profile is not 16 partitions")
	}
	scenario.Cluster.Raft.PartitionCount = 64
	if err := validateComposeRaftTopology(scenario, ""); err == nil || !strings.Contains(err.Error(), "PARTITION_COUNT") {
		t.Fatalf("inherited 64/16 mismatch accepted: %v", err)
	}
	scenario.Cluster.Raft.PartitionCount = 16
	overlay := filepath.Join(t.TempDir(), "override.yml")
	if err := os.WriteFile(overlay, []byte("services:\n  myceld-c:\n    environment:\n      MYCELD_CLUSTER_RAFT_PARTITION_COUNT: '8'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err := topologyStringSlice(scenario.Environment.Options["composeFiles"])
	if err != nil {
		t.Fatal(err)
	}
	scenario.Environment.Options["composeFiles"] = append(files, overlay)
	if err := validateComposeRaftTopology(scenario, ""); err == nil || !strings.Contains(err.Error(), "myceld-c") {
		t.Fatalf("per-node overlay mismatch accepted: %v", err)
	}
	scenario.Environment.Options["composeFiles"] = files
	t.Setenv("MYCELD_CLUSTER_RAFT_PARTITION_COUNT", "8")
	if err := validateComposeRaftTopology(scenario, ""); err == nil || !strings.Contains(err.Error(), "PARTITION_COUNT") {
		t.Fatalf("host Compose interpolation mismatch accepted: %v", err)
	}
	t.Setenv("MYCELD_CLUSTER_RAFT_PARTITION_COUNT", "not-a-number")
	if err := validateComposeRaftTopology(scenario, ""); err == nil || !strings.Contains(err.Error(), "positive integer") {
		t.Fatalf("unknown Compose interpolation accepted: %v", err)
	}
}

func TestSuiteRejectsLaterTopologyMismatchBeforeFirstScenario(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-pending-restart", "compose-semantic-pending-restart.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	modified := strings.Replace(string(raw), "partitionCount: 16", "partitionCount: 64", 1)
	if modified == string(raw) {
		t.Fatal("pending-restart topology declaration changed")
	}
	second := filepath.Join(t.TempDir(), "later.yaml")
	if err := os.WriteFile(second, []byte(modified), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := filepath.Abs(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-recovery", "compose-semantic-graph-recovery.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	suite := filepath.Join(t.TempDir(), "topology-suite.yaml")
	body := fmt.Sprintf("apiVersion: myceldb.io/reliability/v1\nkind: Suite\nmetadata:\n  name: topology-preflight\nscenarios:\n  - path: %q\n  - path: %q\nexecution:\n  mode: sequential\n  parallelism: 1\n  stopOnFailure: true\n", first, second)
	if err := os.WriteFile(suite, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := t.TempDir()
	out, err := RunSuiteFile(context.Background(), suite, Options{DryRun: true, ArtifactRoot: artifacts, ProfileDirs: []string{filepath.Join("..", "..", "..", "tests", "reliability")}})
	if err == nil || !strings.Contains(err.Error(), "topology mismatch") || len(out.Results) != 0 {
		t.Fatalf("later mismatch must stop entire suite before run: result=%+v err=%v", out, err)
	}
	entries, err := os.ReadDir(artifacts)
	if err != nil || len(entries) != 0 {
		t.Fatalf("preflight wrote run artifacts before rejecting topology: entries=%v err=%v", entries, err)
	}
}
