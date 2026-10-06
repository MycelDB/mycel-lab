package deploy

import (
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
)

func TestRenderKubernetesManifestsIncludesClusterEnvOverrides(t *testing.T) {
	scenario, err := catalog.ResolveScenarioFile("../../../tests/reliability/scenarios/k3d-raft-snapshot-pvc-rejoin.yaml", catalog.ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	manifests, err := RenderKubernetesManifests(scenario)
	if err != nil {
		t.Fatalf("RenderKubernetesManifests() error = %v", err)
	}
	check := `MYCELD_CLUSTER_RAFT_EMPTY_STORAGE_REJOIN_RECOVERY: "true"`
	if !strings.Contains(manifests.YAML, check) {
		t.Fatalf("manifest missing %q\n%s", check, manifests.YAML)
	}
}

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
		`MYCELD_MODE: "mesh"`,
		`MYCELD_GRPC_ADDR: "0.0.0.0:9091"`,
		`MYCELD_BOOTSTRAP_ADMIN_USERNAME: "admin"`,
		`MYCELD_BOOTSTRAP_ADMIN_PASSWORD: "admin-password"`,
		`MYCELD_CLUSTER_BACKEND_AUTH_TOKEN: "mycel-lab-local-backend-token"`,
		`MYCELD_CLUSTER_RAFT_NODE_COUNT: "3"`,
		`MYCELD_CLUSTER_RAFT_PARTITION_COUNT: "32"`,
		`MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: "3"`,
		`MYCELD_CLUSTER_RAFT_NODE_ADDRS: "myceld-0.myceld.mycel-lab.svc.cluster.local:9091,myceld-1.myceld.mycel-lab.svc.cluster.local:9091,myceld-2.myceld.mycel-lab.svc.cluster.local:9091"`,
		"imagePullPolicy: IfNotPresent",
		"mountPath: /data/mycel",
		`export MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID="$((ordinal + 1))"`,
		`export MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$HOSTNAME.myceld.mycel-lab.svc.cluster.local:9091"`,
		"name: myceld-0-client",
		"statefulset.kubernetes.io/pod-name: myceld-0",
		"name: myceld-1-client",
		"statefulset.kubernetes.io/pod-name: myceld-1",
		"name: myceld-2-client",
		"statefulset.kubernetes.io/pod-name: myceld-2",
		"port: 9091",
		"targetPort: 9091",
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
	if strings.Contains(manifests.YAML, "mountPath: /var/lib/myceld") {
		t.Fatalf("manifest mounts PVC at legacy path instead of daemon data dir:\n%s", manifests.YAML)
	}
}
