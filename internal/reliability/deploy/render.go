package deploy

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	if _, configured := cluster.Env["MYCELD_LOG_FORMAT"]; !configured {
		fmt.Fprintf(&b, "  MYCELD_LOG_FORMAT: %q\n", "json")
	}
	for _, key := range sortedResourceKeys(cluster.Env) {
		fmt.Fprintf(&b, "  %s: %q\n", key, cluster.Env[key])
	}
	fmt.Fprintf(&b, "---\napiVersion: v1\nkind: Service\nmetadata:\n  name: myceld\n  namespace: %s\nspec:\n  clusterIP: None\n  selector:\n    app: myceld\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n    - name: raft\n      port: 7000\n      targetPort: 7000\n---\n", namespace)
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Service\nmetadata:\n  name: myceld-client\n  namespace: %s\nspec:\n  selector:\n    app: myceld\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n---\n", namespace)
	for i := 0; i < cluster.Nodes; i++ {
		podName := fmt.Sprintf("myceld-%d", i)
		fmt.Fprintf(&b, "apiVersion: v1\nkind: Service\nmetadata:\n  name: %s-client\n  namespace: %s\nspec:\n  selector:\n    statefulset.kubernetes.io/pod-name: %s\n  ports:\n    - name: grpc\n      port: 9091\n      targetPort: 9091\n---\n", podName, namespace, podName)
	}
	if err := renderProviderEmulator(&b, scenario, namespace); err != nil {
		return ManifestSet{}, err
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

func renderProviderEmulator(b *bytes.Buffer, scenario spec.ResolvedScenario, namespace string) error {
	if scenario.ProviderProfile == nil || string(scenario.ProviderProfile.Profile.Mode) != "emulator" {
		return nil
	}
	capability, _, err := scenario.ProviderProfile.Profile.ProtocolEmulatorCapability()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(capability.EmulatorScript)
	if err != nil {
		return fmt.Errorf("encode provider emulator script: %w", err)
	}
	script := base64.StdEncoding.EncodeToString(raw)
	hash := sha256.Sum256(raw)
	image := "local/mycel-provider-emulator:dev"
	if configured, ok := scenario.Environment.Options["providerEmulatorImage"].(string); ok && strings.TrimSpace(configured) != "" {
		image = strings.TrimSpace(configured)
	}
	fmt.Fprintf(b, "apiVersion: v1\nkind: Service\nmetadata:\n  name: mycel-provider-emulator\n  namespace: %s\nspec:\n  selector:\n    app: mycel-provider-emulator\n  ports:\n    - name: http\n      port: 18080\n      targetPort: 18080\n---\n", namespace)
	fmt.Fprintf(b, "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: mycel-provider-emulator\n  namespace: %s\n  annotations:\n    reliability.mycel.io/script-sha256: %q\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: mycel-provider-emulator\n  template:\n    metadata:\n      labels:\n        app: mycel-provider-emulator\n    spec:\n      securityContext:\n        fsGroup: 65532\n      containers:\n        - name: emulator\n          image: %s\n          imagePullPolicy: IfNotPresent\n          args: [\"serve\"]\n          env:\n            - name: MYCEL_EMULATOR_DIMENSIONS\n              value: %q\n            - name: MYCEL_EMULATOR_SCRIPT_B64\n              value: %q\n            - name: MYCEL_EMULATOR_OBSERVATIONS\n              value: /data/observations.jsonl\n          ports:\n            - name: http\n              containerPort: 18080\n          readinessProbe:\n            exec:\n              command: [\"/mycel-provider-emulator\", \"health\"]\n            initialDelaySeconds: 1\n            periodSeconds: 1\n            timeoutSeconds: 2\n          volumeMounts:\n            - name: observations\n              mountPath: /data\n      volumes:\n        - name: observations\n          emptyDir: {}\n---\n", namespace, hex.EncodeToString(hash[:]), image, fmt.Sprint(emulatorDimensions(capability.Dimensions)), script)
	return nil
}

func emulatorDimensions(configured int) int {
	if configured > 0 {
		return configured
	}
	return 8
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
