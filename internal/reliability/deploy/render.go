package deploy

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type ManifestSet struct {
	Namespace   string `json:"namespace"`
	StatefulSet string `json:"statefulSet"`
	YAML        string `json:"yaml"`
}

func RenderKubernetesManifests(scenario spec.ResolvedScenario) (ManifestSet, error) {
	cluster := scenario.Cluster
	if err := cluster.Validate(); err != nil {
		return ManifestSet{}, err
	}
	namespace := scenario.Environment.Namespace
	if namespace == "" {
		namespace = "mycel-lab"
	}
	var raftNodeAddrs []string
	for i := 0; i < cluster.Nodes; i++ {
		raftNodeAddrs = append(raftNodeAddrs, fmt.Sprintf("myceld-%d.myceld.%s.svc.cluster.local:9091", i, namespace))
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n---\n", namespace)
	fmt.Fprintf(&b, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: myceld-config\n  namespace: %s\ndata:\n", namespace)
	fmt.Fprintf(&b, "  MYCELD_MODE: %q\n", "mesh")
	fmt.Fprintf(&b, "  MYCELD_GRPC_ADDR: %q\n", "0.0.0.0:9091")
	fmt.Fprintf(&b, "  MYCELD_BOOTSTRAP_ADMIN_USERNAME: %q\n", "admin")
	fmt.Fprintf(&b, "  MYCELD_BOOTSTRAP_ADMIN_PASSWORD: %q\n", "admin-password")
	fmt.Fprintf(&b, "  MYCELD_CLUSTER_BACKEND_AUTH_TOKEN: %q\n", "mycel-lab-local-backend-token")
	fmt.Fprintf(&b, "  MYCELD_CLUSTER_RAFT_NODE_COUNT: %q\n", fmt.Sprint(cluster.Raft.NodeCount))
	fmt.Fprintf(&b, "  MYCELD_CLUSTER_RAFT_PARTITION_COUNT: %q\n", fmt.Sprint(cluster.Raft.PartitionCount))
	fmt.Fprintf(&b, "  MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: %q\n", fmt.Sprint(cluster.Raft.ReplicaFactor))
	fmt.Fprintf(&b, "  MYCELD_CLUSTER_RAFT_NODE_ADDRS: %q\n", strings.Join(raftNodeAddrs, ","))
	fmt.Fprintf(&b, "---\napiVersion: v1\nkind: Service\nmetadata:\n  name: myceld\n  namespace: %s\nspec:\n  clusterIP: None\n  selector:\n    app: myceld\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n    - name: raft\n      port: 7000\n      targetPort: 7000\n---\n", namespace)
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Service\nmetadata:\n  name: myceld-client\n  namespace: %s\nspec:\n  selector:\n    app: myceld\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n---\n", namespace)
	for i := 0; i < cluster.Nodes; i++ {
		podName := fmt.Sprintf("myceld-%d", i)
		fmt.Fprintf(&b, "apiVersion: v1\nkind: Service\nmetadata:\n  name: %s-client\n  namespace: %s\nspec:\n  selector:\n    statefulset.kubernetes.io/pod-name: %s\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n---\n", podName, namespace, podName)
	}
	fmt.Fprintf(&b, "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: myceld\n  namespace: %s\nspec:\n  serviceName: myceld\n  replicas: %d\n  selector:\n    matchLabels:\n      app: myceld\n  template:\n    metadata:\n      labels:\n        app: myceld\n    spec:\n      containers:\n        - name: myceld\n          image: %s\n          imagePullPolicy: IfNotPresent\n          command:\n            - /bin/sh\n            - -ec\n            - |\n              ordinal=\"${HOSTNAME##*-}\"\n              export MYCELD_NODE_NAME=\"$HOSTNAME\"\n              export MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID=\"$((ordinal + 1))\"\n              export MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR=\"$HOSTNAME.myceld.%s.svc.cluster.local:9091\"\n              exec myceld\n          envFrom:\n            - configMapRef:\n                name: myceld-config\n", namespace, cluster.Nodes, cluster.Image, namespace)
	if len(cluster.Resources.Requests) > 0 || len(cluster.Resources.Limits) > 0 {
		b.WriteString("          resources:\n")
		if len(cluster.Resources.Requests) > 0 {
			b.WriteString("            requests:\n")
			for _, key := range sortedResourceKeys(cluster.Resources.Requests) {
				fmt.Fprintf(&b, "              %s: %q\n", key, cluster.Resources.Requests[key])
			}
		}
		if len(cluster.Resources.Limits) > 0 {
			b.WriteString("            limits:\n")
			for _, key := range sortedResourceKeys(cluster.Resources.Limits) {
				fmt.Fprintf(&b, "              %s: %q\n", key, cluster.Resources.Limits[key])
			}
		}
	}
	fmt.Fprintf(&b, "          volumeMounts:\n            - name: data\n              mountPath: /data/mycel\n  volumeClaimTemplates:\n    - metadata:\n        name: data\n      spec:\n        accessModes: [\"ReadWriteOnce\"]\n")
	if cluster.Storage.ClassName != "" {
		fmt.Fprintf(&b, "        storageClassName: %s\n", cluster.Storage.ClassName)
	}
	storageSize := cluster.Storage.Size
	if storageSize == "" {
		storageSize = "10Gi"
	}
	fmt.Fprintf(&b, "        resources:\n          requests:\n            storage: %s\n", storageSize)
	return ManifestSet{Namespace: namespace, StatefulSet: "myceld", YAML: b.String()}, nil
}

func sortedResourceKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	if len(keys) == 2 && keys[0] > keys[1] {
		keys[0], keys[1] = keys[1], keys[0]
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}
