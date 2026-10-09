package spec

import (
	"strings"
	"testing"
)

func TestLoadClusterProfile(t *testing.T) {
	loaded, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: ClusterProfile
metadata:
  name: raft-3-node
cluster:
  nodes: 3
  image: myceldb/mycel:latest
  raft:
    nodeCount: 3
    partitionCount: 64
    replicaFactor: 3
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	profile, ok := loaded.(ClusterProfile)
	if !ok {
		t.Fatalf("Load() = %T, want ClusterProfile", loaded)
	}
	if profile.Metadata.Name != "raft-3-node" || profile.Cluster.Raft.PartitionCount != 64 {
		t.Fatalf("unexpected profile: %+v", profile)
	}
}

func TestLoadRejectsUnsupportedKind(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Unknown
metadata:
  name: nope
`))
	if err == nil || !strings.Contains(err.Error(), "unsupported kind") {
		t.Fatalf("Load() error = %v, want unsupported kind", err)
	}
}

func TestScenarioDefaultsAndValidation(t *testing.T) {
	loaded, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: example
seed: 99
clusterRef: raft-3-node
actorGroups:
  - name: writers
    profileRef: graph-committer
    count: 2
    rate:
      commitsPerSecond: 5
phases:
  - name: warmup
    duration: 1m
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	scenario := loaded.(Scenario)
	if scenario.Environment.Driver != "k3d" {
		t.Fatalf("default driver = %q, want k3d", scenario.Environment.Driver)
	}
	if scenario.ActorGroups[0].Rate.Mode != "group-total" {
		t.Fatalf("default rate mode = %q, want group-total", scenario.ActorGroups[0].Rate.Mode)
	}
}

func TestLoadSuite(t *testing.T) {
	loaded, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Suite
metadata:
  name: baseline
scenarios:
  - path: ../scenarios/example/example.yaml
execution:
  stopOnFailure: true
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	suite := loaded.(Suite)
	if suite.Execution.Mode != "sequential" {
		t.Fatalf("default execution mode = %q, want sequential", suite.Execution.Mode)
	}
}

func TestScenarioRejectsUnsupportedEvent(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: bad-event
seed: 1
clusterRef: raft-3-node
actorGroups:
  - name: writers
    profileRef: graph-committer
    count: 1
    rate:
      commitsPerSecond: 1
phases:
  - name: boom
    duration: 1m
    events:
      - type: cosmic-ray
        target:
          pod: myceld-0
`))
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Load() error = %v, want unsupported event", err)
	}
}

func TestActorProfileRequiresBehaviorType(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: ActorProfile
metadata:
  name: broken
behavior:
  retry:
    maxAttempts: 2
`))
	if err == nil || !strings.Contains(err.Error(), "behavior.type") {
		t.Fatalf("Load() error = %v, want behavior.type", err)
	}
}

func TestScenarioEnvironmentOptionsAndCapabilities(t *testing.T) {
	loaded, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: compose-example
seed: 99
environment:
  driver: compose
  namespace: lab
  options:
    composeFile: ../mycel/tests/compose/cluster/compose.yml
    serviceNames:
      - myceld-a
      - myceld-b
  capabilities:
    required:
      - per-node-endpoints
      - logs
clusterRef: raft-3-node
actorGroups:
  - name: writers
    profileRef: graph-committer
    count: 2
    rate:
      commitsPerSecond: 5
phases:
  - name: warmup
    duration: 1m
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	scenario := loaded.(Scenario)
	if scenario.Environment.Driver != "compose" || scenario.Environment.Namespace != "lab" {
		t.Fatalf("environment=%+v", scenario.Environment)
	}
	if got := scenario.Environment.Options["composeFile"]; got != "../mycel/tests/compose/cluster/compose.yml" {
		t.Fatalf("composeFile option=%v", got)
	}
	if len(scenario.Environment.Capabilities.Required) != 2 || scenario.Environment.Capabilities.Required[1] != "logs" {
		t.Fatalf("capabilities=%+v", scenario.Environment.Capabilities.Required)
	}
}

func TestScenarioRejectsGenericKubernetesDriver(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: bad-driver
seed: 1
environment:
  driver: kubernetes
clusterRef: raft-3-node
actorGroups:
  - name: writers
    profileRef: graph-committer
    count: 1
    rate:
      commitsPerSecond: 1
phases:
  - name: warmup
    duration: 1m
`))
	if err == nil || !strings.Contains(err.Error(), "environment.driver") || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Load() error=%v, want unsupported environment.driver", err)
	}
}
