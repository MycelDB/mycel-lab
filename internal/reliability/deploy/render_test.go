package deploy

import (
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
)

func TestRenderKubernetesManifestsReflectClusterProfile(t *testing.T) {
	scenario, err := catalog.ResolveScenarioFile("../../../tests/reliability/scenarios/raft-3-node-short-outage.yaml", catalog.ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	manifests, err := RenderKubernetesManifests(scenario)
	if err != nil {
		t.Fatalf("RenderKubernetesManifests() error = %v", err)
	}
	checks := []string{
		"replicas: 3",
		`MYCELD_CLUSTER_RAFT_NODE_COUNT: "3"`,
		`MYCELD_CLUSTER_RAFT_PARTITION_COUNT: "32"`,
		`MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: "3"`,
		"myceld-0.myceld.mycel-lab.svc.cluster.local:7000",
		"cpu: \"500m\"",
		"memory: \"512Mi\"",
		"storageClassName: local-path",
		"storage: 10Gi",
	}
	for _, check := range checks {
		if !strings.Contains(manifests.YAML, check) {
			t.Fatalf("manifest missing %q\n%s", check, manifests.YAML)
		}
	}
}
