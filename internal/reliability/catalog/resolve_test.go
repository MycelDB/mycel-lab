package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestResolveScenarioFileByName(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml")
	resolved, err := ResolveScenarioFile(path, ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	if resolved.Metadata.Name != "raft-3-node-short-outage" {
		t.Fatalf("name = %q", resolved.Metadata.Name)
	}
	if resolved.Cluster.Nodes != 3 {
		t.Fatalf("cluster nodes = %d, want 3", resolved.Cluster.Nodes)
	}
	if resolved.Cluster.Raft.PartitionCount != 32 {
		t.Fatalf("partition count = %d, want override 32", resolved.Cluster.Raft.PartitionCount)
	}
	if len(resolved.ActorGroups) != 2 {
		t.Fatalf("actor groups = %d, want 2", len(resolved.ActorGroups))
	}
	if resolved.ActorGroups[0].Profile.Metadata.Name != "graph-committer" {
		t.Fatalf("first profile = %q", resolved.ActorGroups[0].Profile.Metadata.Name)
	}
}

func TestResolveScenarioFileByPathAndActorOverride(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "example.yaml")
	resolved, err := ResolveScenarioFile(path, ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	if resolved.Cluster.Raft.PartitionCount != 64 {
		t.Fatalf("partition count = %d, want base profile value 64", resolved.Cluster.Raft.PartitionCount)
	}
	behavior := resolved.ActorGroups[0].Profile.Behavior
	transaction, ok := behavior["transaction"].(map[string]any)
	if !ok {
		t.Fatalf("transaction override missing or wrong type: %#v", behavior["transaction"])
	}
	ops, ok := transaction["operationsPerTransaction"].(map[string]any)
	if !ok {
		t.Fatalf("operationsPerTransaction missing or wrong type: %#v", transaction["operationsPerTransaction"])
	}
	if got := ops["max"]; got != 3 {
		t.Fatalf("operationsPerTransaction.max = %#v, want 3", got)
	}
}

func TestResolvedScenarioOutputIsStable(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml")
	first, err := ResolveScenarioFile(path, ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile(first) error = %v", err)
	}
	second, err := ResolveScenarioFile(path, ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile(second) error = %v", err)
	}
	firstYAML, err := spec.MarshalYAML(first)
	if err != nil {
		t.Fatalf("MarshalYAML(first) error = %v", err)
	}
	secondYAML, err := spec.MarshalYAML(second)
	if err != nil {
		t.Fatalf("MarshalYAML(second) error = %v", err)
	}
	if string(firstYAML) != string(secondYAML) {
		t.Fatalf("resolved output is not stable\nfirst:\n%s\nsecond:\n%s", firstYAML, secondYAML)
	}
}

func TestResolveScenarioFileMissingProfile(t *testing.T) {
	tmp := t.TempDir()
	scenario := filepath.Join(tmp, "scenario.yaml")
	writeFile(t, scenario, `
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: missing
seed: 1
clusterRef: no-such-cluster
actorGroups:
  - name: writers
    profileRef: no-such-actor
    count: 1
    rate:
      commitsPerSecond: 1
phases:
  - name: warmup
    duration: 1m
`)
	if _, err := ResolveScenarioFile(scenario, ResolveOptions{}); err == nil {
		t.Fatalf("ResolveScenarioFile() error = nil, want missing profile")
	}
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
