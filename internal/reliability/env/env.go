package env

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type Capability string

const (
	CapabilityPlanning           Capability = "planning"
	CapabilityArtifacts          Capability = "artifacts"
	CapabilityPerNodeEndpoints   Capability = "per-node-endpoints"
	CapabilityNodeExec           Capability = "node-exec"
	CapabilityNodeRestart        Capability = "node-restart"
	CapabilityRollingRestart     Capability = "rolling-restart"
	CapabilityLogs               Capability = "logs"
	CapabilityObjectStoreFixture Capability = "object-store-fixture"
	CapabilityVolumeReplacement  Capability = "volume-replacement"
	CapabilityImageUpgrade       Capability = "image-upgrade"
	CapabilityProviderEmulator   Capability = "provider-emulator"
)

type CapabilitySet map[Capability]struct{}

func NewCapabilitySet(capabilities ...Capability) CapabilitySet {
	out := CapabilitySet{}
	for _, capability := range capabilities {
		out[capability] = struct{}{}
	}
	return out
}

func (s CapabilitySet) Has(capability Capability) bool {
	_, ok := s[capability]
	return ok
}

func (s CapabilitySet) Missing(required []string) []string {
	missing := []string{}
	for _, raw := range required {
		capability := Capability(strings.TrimSpace(raw))
		if capability == "" {
			continue
		}
		if !s.Has(capability) {
			missing = append(missing, string(capability))
		}
	}
	sort.Strings(missing)
	return missing
}

func (s CapabilitySet) Strings() []string {
	out := make([]string, 0, len(s))
	for capability := range s {
		out = append(out, string(capability))
	}
	sort.Strings(out)
	return out
}

type Environment struct {
	Name      string            `json:"name"`
	Driver    string            `json:"driver"`
	Namespace string            `json:"namespace,omitempty"`
	Context   string            `json:"context,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Node struct {
	Name     string            `json:"name"`
	Ordinal  int               `json:"ordinal"`
	Role     string            `json:"role,omitempty"`
	Resource string            `json:"resource"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Endpoint struct {
	NodeName     string `json:"nodeName"`
	ServiceName  string `json:"serviceName,omitempty"`
	LocalAddress string `json:"localAddress,omitempty"`
	LocalPort    int    `json:"localPort,omitempty"`
	RemotePort   int    `json:"remotePort,omitempty"`
	DaemonAddr   string `json:"daemonAddr"`
	Scheme       string `json:"scheme,omitempty"`
}

type NodeRef struct {
	Name    string
	Ordinal int
}

type ExecRequest struct {
	Command []string `json:"command"`
}

type ExecResult = executil.Result

type EnvironmentResetter interface {
	Reset(context.Context, Environment) error
}

type ProviderEmulatorInspector interface {
	ProviderEmulatorObservations(context.Context, Environment) (ExecResult, error)
}

func PersistProviderEmulatorObservations(sink *artifacts.Sink, raw string, requireNonempty bool) error {
	if sink == nil {
		return errors.New("provider emulator observation sink is required")
	}
	persisted := raw
	if sanitizer := sink.Sanitizer(); sanitizer != nil {
		persisted = sanitizer.SanitizeText(raw)
	}
	observations, err := emulator.DecodeObservationEvidence(persisted)
	if err != nil {
		return errors.New("provider emulator observations failed validation")
	}
	if requireNonempty && len(observations) == 0 {
		return errors.New("provider emulator observations are empty")
	}
	if _, err := sink.WriteText("provider/emulator-observations.jsonl", raw); err != nil {
		return errors.New("write provider emulator observations failed")
	}
	return nil
}

type NodeFileStager interface {
	CopyFromNode(context.Context, Environment, NodeRef, string, string) (ExecResult, error)
	CopyToNode(context.Context, Environment, NodeRef, string, string) (ExecResult, error)
}

type RestoreArtifact struct {
	Ordinal       int
	PodName       string
	ArchiveName   string
	ManifestName  string
	LocalArchive  string
	LocalManifest string
}

type RestoreBackupSetRequest struct {
	PVCName       string
	BackupSetPath string
	Artifacts     []RestoreArtifact
	Ordinal       int
	Image         string
}

type VolumeReplacementDriver interface {
	PVCUIDs(context.Context, Environment, string, int) (map[string]string, error)
	WaitPVCs(context.Context, Environment, string, int) error
	ResetNamespace(context.Context, Environment) error
	ApplyYAML(context.Context, Environment, string) (ExecResult, error)
	ScaleStatefulSet(context.Context, Environment, string, int) error
	DeletePVC(context.Context, Environment, string) error
	RestoreArchiveToPVC(context.Context, Environment, string, string) error
}

type StatefulSetImageUpdateRequest struct {
	StatefulSet string
	Container   string
	Image       string
	Timeout     time.Duration
}

type StatefulSetImageUpdater interface {
	UpdateStatefulSetImage(context.Context, Environment, StatefulSetImageUpdateRequest) (ExecResult, error)
}

type ClusterRestoreDriver interface {
	RestoreBackupSetOrdinalToPVC(context.Context, Environment, RestoreBackupSetRequest) (ExecResult, error)
}

type DeletionVerification struct {
	Supported  bool `json:"supported"`
	Completed  bool `json:"completed"`
	Clusters   int  `json:"clusters"`
	Containers int  `json:"containers"`
	Networks   int  `json:"networks"`
	Volumes    int  `json:"volumes"`
}

type EnvironmentDeletionVerifier interface {
	VerifyDeleted(context.Context, Environment) (DeletionVerification, error)
}

type EnvironmentDriver interface {
	Name() string
	Capabilities() CapabilitySet
	Validate(spec.EnvironmentSpec) error
	Preflight(context.Context, spec.EnvironmentSpec) error
	Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (Environment, error)
	WaitReady(context.Context, Environment) error
	Nodes(context.Context, Environment) ([]Node, error)
	Endpoints(context.Context, Environment) ([]Endpoint, error)
	Exec(context.Context, Environment, NodeRef, ExecRequest) (ExecResult, error)
	RestartNode(context.Context, Environment, NodeRef) error
	RollingRestart(context.Context, Environment) error
	CaptureState(context.Context, Environment, *artifacts.Sink) error
	Delete(context.Context, Environment) error
}

type Options struct {
	ConfirmDestructive bool
	DryRun             bool
}

type DriverSelectionOptions struct {
	DryRun             bool
	ConfirmDestructive bool
	Runner             executil.Runner
	WaitTimeout        time.Duration
}

var ErrUnsupportedCapability = errors.New("unsupported environment capability")

func RequireDestructiveConfirmation(opts Options) error {
	if opts.DryRun || opts.ConfirmDestructive {
		return nil
	}
	return errors.New("destructive environment operations require --confirm-destructive")
}

func SelectDriver(environment spec.EnvironmentSpec, opts DriverSelectionOptions) (EnvironmentDriver, spec.EnvironmentSpec, error) {
	effective := environment
	if opts.DryRun {
		effective.Driver = "dry-run"
	}
	if effective.Driver == "" {
		effective.Driver = "k3d"
	}
	var driver EnvironmentDriver
	switch effective.Driver {
	case "dry-run":
		driver = DryRunDriver{}
	case "k3d":
		driver = K3DDriver{Confirmed: opts.ConfirmDestructive, Runner: opts.Runner, WaitTimeout: opts.WaitTimeout}
	case "compose":
		driver = ComposeDriver{Confirmed: opts.ConfirmDestructive, Runner: opts.Runner, WaitTimeout: opts.WaitTimeout}
	default:
		return nil, effective, fmt.Errorf("unsupported environment driver %q", effective.Driver)
	}
	if err := driver.Validate(effective); err != nil {
		return nil, effective, err
	}
	if !opts.DryRun {
		if missing := driver.Capabilities().Missing(effective.Capabilities.Required); len(missing) > 0 {
			return nil, effective, fmt.Errorf("scenario requires capability %q, but driver %q supports: %s", strings.Join(missing, ","), driver.Name(), strings.Join(driver.Capabilities().Strings(), ","))
		}
	}
	return driver, effective, nil
}

func WriteEnvironmentArtifacts(sink *artifacts.Sink, environment Environment, driver EnvironmentDriver, nodes []Node, endpoints []Endpoint) error {
	if _, err := sink.WriteJSON("environment/state.json", environment); err != nil {
		return err
	}
	if _, err := sink.WriteJSON("environment/capabilities.json", driver.Capabilities().Strings()); err != nil {
		return err
	}
	if _, err := sink.WriteJSON("environment/nodes.json", nodes); err != nil {
		return err
	}
	_, err := sink.WriteJSON("environment/endpoints.json", endpoints)
	return err
}

type DryRunDriver struct{}

func (DryRunDriver) Name() string { return "dry-run" }

func (DryRunDriver) Capabilities() CapabilitySet {
	return NewCapabilitySet(CapabilityPlanning, CapabilityArtifacts, CapabilityPerNodeEndpoints)
}

func (DryRunDriver) Validate(spec.EnvironmentSpec) error { return nil }

func (DryRunDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }

func (DryRunDriver) Create(_ context.Context, scenario spec.ResolvedScenario, environmentSpec spec.EnvironmentSpec) (Environment, error) {
	name := scenario.Metadata.Name
	if name == "" {
		name = "mycel-lab"
	}
	namespace := environmentSpec.Namespace
	if namespace == "" {
		namespace = "mycel-lab"
	}
	return Environment{Name: name, Driver: "dry-run", Namespace: namespace, Context: "dry-run", Metadata: map[string]string{"nodeCount": strconv.Itoa(scenario.Cluster.Nodes)}}, nil
}

func (DryRunDriver) WaitReady(context.Context, Environment) error { return nil }

func (DryRunDriver) Nodes(_ context.Context, environment Environment) ([]Node, error) {
	count := intFromMetadata(environment.Metadata, "nodeCount", 1)
	if count <= 0 {
		count = 1
	}
	nodes := make([]Node, 0, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("myceld-%d", i)
		nodes = append(nodes, Node{Name: name, Ordinal: i, Role: "daemon", Resource: environment.Name + "/" + name})
	}
	return nodes, nil
}

func (DryRunDriver) Endpoints(_ context.Context, environment Environment) ([]Endpoint, error) {
	count := intFromMetadata(environment.Metadata, "nodeCount", 1)
	if count <= 0 {
		count = 1
	}
	endpoints := make([]Endpoint, 0, count)
	for _, endpoint := range ConsoleEndpointsForNodeCount(count, DefaultConsolePortBase) {
		endpoints = append(endpoints, Endpoint{NodeName: endpoint.NodeName, ServiceName: endpoint.ServiceName, LocalAddress: endpoint.LocalAddress, LocalPort: endpoint.LocalPort, RemotePort: endpoint.RemotePort, DaemonAddr: endpoint.DaemonAddr, Scheme: "grpc"})
	}
	return endpoints, nil
}

func (DryRunDriver) Exec(context.Context, Environment, NodeRef, ExecRequest) (ExecResult, error) {
	return ExecResult{}, fmt.Errorf("%w: node-exec", ErrUnsupportedCapability)
}

func (DryRunDriver) RestartNode(context.Context, Environment, NodeRef) error {
	return fmt.Errorf("%w: node-restart", ErrUnsupportedCapability)
}

func (DryRunDriver) RollingRestart(context.Context, Environment) error {
	return fmt.Errorf("%w: rolling-restart", ErrUnsupportedCapability)
}

func (DryRunDriver) CaptureState(_ context.Context, environment Environment, sink *artifacts.Sink) error {
	_, err := sink.WriteJSON("environment/state.json", environment)
	return err
}

func (DryRunDriver) Delete(context.Context, Environment) error { return nil }

func (DryRunDriver) VerifyDeleted(context.Context, Environment) (DeletionVerification, error) {
	return DeletionVerification{Supported: true, Completed: true}, nil
}

type K3DDriver struct {
	Confirmed       bool
	Runner          executil.Runner
	WaitTimeout     time.Duration
	CreateArguments []string
	nodeCount       int
	portBase        int
}

func (d K3DDriver) Name() string { return "k3d" }

func (d K3DDriver) Capabilities() CapabilitySet {
	return NewCapabilitySet(CapabilityArtifacts, CapabilityPerNodeEndpoints, CapabilityNodeExec, CapabilityNodeRestart, CapabilityRollingRestart, CapabilityLogs, CapabilityVolumeReplacement, CapabilityImageUpgrade, CapabilityProviderEmulator)
}

func (d K3DDriver) Validate(environment spec.EnvironmentSpec) error {
	return rejectUnknownOptions(environment.Options, "clusterNamePrefix", "createArguments", "consolePortBase", "providerEmulatorImage")
}

func (d K3DDriver) Preflight(ctx context.Context, environment spec.EnvironmentSpec) error {
	if !d.Confirmed {
		return RequireDestructiveConfirmation(Options{})
	}
	runner := d.runner()
	if _, err := runner.Run(ctx, "k3d", "version"); err != nil {
		return fmt.Errorf("k3d preflight failed: %w", err)
	}
	if _, err := runner.Run(ctx, "kubectl", "version", "--client=true"); err != nil {
		return fmt.Errorf("kubectl preflight failed: %w", err)
	}
	if image := strings.TrimSpace(stringOption(environment.Options, "providerEmulatorImage", "")); image != "" {
		if _, err := runner.Run(ctx, "docker", "image", "inspect", image); err != nil {
			return fmt.Errorf("provider emulator image %s is unavailable: %w", image, err)
		}
	}
	return nil
}

func (d K3DDriver) Create(ctx context.Context, scenario spec.ResolvedScenario, environmentSpec spec.EnvironmentSpec) (Environment, error) {
	if !d.Confirmed {
		return Environment{}, RequireDestructiveConfirmation(Options{})
	}
	runner := d.runner()
	clusterName := k3dClusterName(scenario.Metadata.Name, stringOption(environmentSpec.Options, "clusterNamePrefix", "mlab"))
	namespace := environmentSpec.Namespace
	if namespace == "" {
		namespace = "mycel-lab"
	}
	args := []string{"cluster", "create", clusterName, "--wait"}
	args = append(args, stringSliceOption(environmentSpec.Options, "createArguments")...)
	args = append(args, d.CreateArguments...)
	if _, err := runner.Run(ctx, "k3d", args...); err != nil {
		return Environment{}, err
	}
	environment := Environment{Name: clusterName, Driver: "k3d", Namespace: namespace, Context: "k3d-" + clusterName, Metadata: map[string]string{"nodeCount": strconv.Itoa(scenario.Cluster.Nodes), "consolePortBase": strconv.Itoa(intOption(environmentSpec.Options, "consolePortBase", DefaultConsolePortBase)), "imageOverride": strings.TrimSpace(scenario.Cluster.Image)}}
	if scenario.ProviderProfile != nil && string(scenario.ProviderProfile.Profile.Mode) == "emulator" {
		scriptHash, err := providerEmulatorScriptSHA256(scenario)
		if err != nil {
			_ = d.Delete(context.Background(), environment)
			return Environment{}, err
		}
		environment.Metadata["providerEmulator"] = "true"
		environment.Metadata["providerEmulatorImage"] = stringOption(environmentSpec.Options, "providerEmulatorImage", "local/mycel-provider-emulator:dev")
		environment.Metadata["providerEmulatorScriptSHA256"] = scriptHash
	}
	if err := d.importLocalImageIfPresent(ctx, environment, scenario.Cluster.Image); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	if emulatorImage := environment.Metadata["providerEmulatorImage"]; emulatorImage != "" {
		if err := d.importLocalImageIfPresent(ctx, environment, emulatorImage); err != nil {
			_ = d.Delete(context.Background(), environment)
			return Environment{}, err
		}
	}
	manifests, err := deploy.RenderKubernetesManifests(scenario)
	if err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	manifestPath, err := writeTempManifest(clusterName, manifests.YAML)
	if err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	defer os.Remove(manifestPath)
	if _, err := runner.Run(ctx, "kubectl", "--context", environment.Context, "apply", "-f", manifestPath); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	if err := d.WaitReady(ctx, environment); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	return environment, nil
}

func (d K3DDriver) importLocalImageIfPresent(ctx context.Context, environment Environment, image string) error {
	image = strings.TrimSpace(image)
	if image == "" {
		return nil
	}
	runner := d.runner()
	if _, err := runner.Run(ctx, "docker", "image", "inspect", image); err != nil {
		return nil
	}
	if _, err := runner.Run(ctx, "k3d", "image", "import", image, "-c", environment.Name); err != nil {
		return fmt.Errorf("import local image %s into k3d cluster %s: %w", image, environment.Name, err)
	}
	return nil
}

func (d K3DDriver) Delete(ctx context.Context, environment Environment) error {
	if environment.Name == "" {
		return nil
	}
	_, err := d.runner().Run(ctx, "k3d", "cluster", "delete", environment.Name)
	return err
}

func (d K3DDriver) VerifyDeleted(ctx context.Context, environment Environment) (DeletionVerification, error) {
	verification := DeletionVerification{Supported: true}
	if strings.TrimSpace(environment.Name) == "" {
		verification.Completed = true
		return verification, nil
	}
	result, err := d.runner().Run(ctx, "k3d", "cluster", "list", "-o", "json")
	if err != nil {
		return verification, errors.New("k3d cluster absence check failed")
	}
	var clusters []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(result.Stdout), &clusters); err != nil {
		return verification, errors.New("k3d cluster absence output is invalid")
	}
	for _, cluster := range clusters {
		if strings.TrimSpace(cluster.Name) == strings.TrimSpace(environment.Name) {
			verification.Clusters++
		}
	}
	verification.Completed = verification.Clusters == 0
	if !verification.Completed {
		return verification, errors.New("k3d cluster remains after deletion")
	}
	return verification, nil
}

func (d K3DDriver) WaitReady(ctx context.Context, environment Environment) error {
	timeout := d.WaitTimeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	timeoutArg := fmt.Sprintf("--timeout=%s", timeout.Round(time.Second))
	runner := d.runner()
	if _, err := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "rollout", "status", "statefulset/myceld", timeoutArg); err != nil {
		return err
	}
	if _, err := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "pods", "-l", "app=myceld", "--for=condition=Ready", timeoutArg); err != nil {
		return err
	}
	if environment.Metadata["providerEmulator"] == "true" {
		if _, err := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "rollout", "status", "deployment/mycel-provider-emulator", timeoutArg); err != nil {
			return err
		}
	}
	return nil
}

func (d K3DDriver) Nodes(ctx context.Context, environment Environment) ([]Node, error) {
	result, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "get", "pods", "-l", "app=myceld", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil, err
	}
	names := strings.Fields(result.Stdout)
	if len(names) == 0 {
		for i := 0; i < intFromMetadata(environment.Metadata, "nodeCount", d.nodeCount); i++ {
			names = append(names, fmt.Sprintf("myceld-%d", i))
		}
	}
	sort.Strings(names)
	nodes := make([]Node, 0, len(names))
	for i, name := range names {
		nodes = append(nodes, Node{Name: name, Ordinal: ordinalFromName(name, i), Role: "daemon", Resource: "pod/" + name})
	}
	return nodes, nil
}

func (d K3DDriver) Endpoints(_ context.Context, environment Environment) ([]Endpoint, error) {
	nodeCount := intFromMetadata(environment.Metadata, "nodeCount", d.nodeCount)
	if nodeCount <= 0 {
		nodeCount = 1
	}
	portBase := intFromMetadata(environment.Metadata, "consolePortBase", d.portBase)
	if portBase <= 0 {
		portBase = DefaultConsolePortBase
	}
	endpoints := make([]Endpoint, 0, nodeCount)
	for _, endpoint := range ConsoleEndpointsForNodeCount(nodeCount, portBase) {
		endpoints = append(endpoints, Endpoint{NodeName: endpoint.NodeName, ServiceName: endpoint.ServiceName, LocalAddress: endpoint.LocalAddress, LocalPort: endpoint.LocalPort, RemotePort: endpoint.RemotePort, DaemonAddr: endpoint.DaemonAddr, Scheme: "grpc"})
	}
	return endpoints, nil
}

func (d K3DDriver) Exec(ctx context.Context, environment Environment, node NodeRef, req ExecRequest) (ExecResult, error) {
	if len(req.Command) == 0 {
		return ExecResult{}, errors.New("exec command is required")
	}
	pod := node.Name
	if pod == "" && node.Ordinal >= 0 {
		pod = fmt.Sprintf("myceld-%d", node.Ordinal)
	}
	args := []string{"--context", environment.Context, "-n", environment.Namespace, "exec", pod, "--"}
	args = append(args, req.Command...)
	return d.runner().Run(ctx, "kubectl", args...)
}

func (d K3DDriver) RestartNode(ctx context.Context, environment Environment, node NodeRef) error {
	pod := node.Name
	if pod == "" && node.Ordinal >= 0 {
		pod = fmt.Sprintf("myceld-%d", node.Ordinal)
	}
	if pod == "" {
		return errors.New("node name or ordinal is required")
	}
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "delete", "pod", pod, "--ignore-not-found=true"); err != nil {
		return err
	}
	return d.WaitReady(ctx, environment)
}

func (d K3DDriver) RollingRestart(ctx context.Context, environment Environment) error {
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "rollout", "restart", "statefulset/myceld"); err != nil {
		return err
	}
	return d.WaitReady(ctx, environment)
}

func (d K3DDriver) UpdateStatefulSetImage(ctx context.Context, environment Environment, req StatefulSetImageUpdateRequest) (ExecResult, error) {
	statefulSet := strings.TrimSpace(req.StatefulSet)
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	container := strings.TrimSpace(req.Container)
	if container == "" {
		container = "myceld"
	}
	image := strings.TrimSpace(req.Image)
	if image == "" {
		return ExecResult{}, errors.New("image is required")
	}
	if err := d.importLocalImageIfPresent(ctx, environment, image); err != nil {
		return ExecResult{}, err
	}
	result, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "set", "image", "statefulset/"+statefulSet, container+"="+image)
	if err != nil {
		return result, err
	}
	timeout := req.Timeout
	if timeout == 0 {
		timeout = d.WaitTimeout
	}
	if timeout == 0 {
		timeout = 10 * time.Minute
	}
	timeoutArg := fmt.Sprintf("--timeout=%s", timeout.Round(time.Second))
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "rollout", "status", "statefulset/"+statefulSet, timeoutArg); err != nil {
		return result, err
	}
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "pods", "-l", "app=myceld", "--for=condition=Ready", timeoutArg); err != nil {
		return result, err
	}
	return result, nil
}

func (d K3DDriver) CopyFromNode(ctx context.Context, environment Environment, node NodeRef, containerPath, hostPath string) (ExecResult, error) {
	if containerPath == "" || hostPath == "" {
		return ExecResult{}, errors.New("container and host paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return ExecResult{}, err
	}
	pod := k3dPodName(node)
	return d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", pod+":"+containerPath, hostPath)
}

func (d K3DDriver) CopyToNode(ctx context.Context, environment Environment, node NodeRef, hostPath, containerPath string) (ExecResult, error) {
	if hostPath == "" || containerPath == "" {
		return ExecResult{}, errors.New("host and container paths are required")
	}
	pod := k3dPodName(node)
	return d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", hostPath, pod+":"+containerPath)
}

func (d K3DDriver) PVCUIDs(ctx context.Context, environment Environment, statefulSet string, count int) (map[string]string, error) {
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	if count <= 0 {
		count = intFromMetadata(environment.Metadata, "nodeCount", d.nodeCount)
	}
	out := map[string]string{}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("data-%s-%d", statefulSet, i)
		result, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "get", "pvc", name, "-o", "jsonpath={.metadata.uid}")
		if err != nil {
			return nil, err
		}
		out[name] = strings.TrimSpace(result.Stdout)
	}
	return out, nil
}

func (d K3DDriver) WaitPVCs(ctx context.Context, environment Environment, statefulSet string, count int) error {
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	if count <= 0 {
		count = intFromMetadata(environment.Metadata, "nodeCount", d.nodeCount)
	}
	if count <= 0 {
		return fmt.Errorf("PVC count is required")
	}
	waitTimeout := d.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 5 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		missing := []string{}
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("data-%s-%d", statefulSet, i)
			result, err := d.runner().Run(waitCtx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "get", "pvc", name, "-o", "jsonpath={.metadata.uid}")
			if err != nil || strings.TrimSpace(result.Stdout) == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for replacement PVCs %s: %w", strings.Join(missing, ", "), waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (d K3DDriver) ResetNamespace(ctx context.Context, environment Environment) error {
	_, _ = d.runner().Run(ctx, "kubectl", "--context", environment.Context, "delete", "namespace", environment.Namespace, "--wait=true", "--timeout=5m")
	_, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "create", "namespace", environment.Namespace)
	return err
}

func (d K3DDriver) ApplyYAML(ctx context.Context, environment Environment, manifest string) (ExecResult, error) {
	path, err := writeTempManifest(environment.Name+"-restore", manifest)
	if err != nil {
		return ExecResult{}, err
	}
	defer os.Remove(path)
	return d.runner().Run(ctx, "kubectl", "--context", environment.Context, "apply", "-f", path)
}

func (d K3DDriver) ScaleStatefulSet(ctx context.Context, environment Environment, statefulSet string, replicas int) error {
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "scale", "statefulset/"+statefulSet, "--replicas", strconv.Itoa(replicas)); err != nil {
		return err
	}
	if replicas == 0 {
		_, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "pods", "-l", "app=myceld", "--for=delete", "--timeout=5m")
		return err
	}
	timeout := d.WaitTimeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	timeoutArg := fmt.Sprintf("--timeout=%s", timeout.Round(time.Second))
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "rollout", "status", "statefulset/"+statefulSet, timeoutArg); err != nil {
		return err
	}
	for ordinal := 0; ordinal < replicas; ordinal++ {
		pod := fmt.Sprintf("%s-%d", statefulSet, ordinal)
		if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "pod/"+pod, "--for=condition=Ready", timeoutArg); err != nil {
			return err
		}
	}
	return nil
}

func (d K3DDriver) DeletePVC(ctx context.Context, environment Environment, pvcName string) error {
	if strings.TrimSpace(pvcName) == "" {
		return errors.New("pvc name is required")
	}
	_, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "delete", "pvc", pvcName, "--wait=true", "--timeout=5m")
	return err
}

func (d K3DDriver) RestoreArchiveToPVC(ctx context.Context, environment Environment, pvcName, archivePath string) error {
	if pvcName == "" || archivePath == "" {
		return errors.New("pvc name and archive path are required")
	}
	restorePod := "restore-" + strings.ReplaceAll(pvcName, "_", "-")
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: restore
      image: alpine:3.21
      command: ["/bin/sh", "-ec", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data/mycel
        - name: restore
          mountPath: /restore
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
    - name: restore
      emptyDir: {}
`, restorePod, environment.Namespace, pvcName)
	if _, err := d.ApplyYAML(ctx, environment, manifest); err != nil {
		return err
	}
	defer func() {
		_, _ = d.runner().Run(context.Background(), "kubectl", "--context", environment.Context, "-n", environment.Namespace, "delete", "pod", restorePod, "--ignore-not-found=true", "--wait=true", "--timeout=3m")
	}()
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "--for=condition=Ready", "pod/"+restorePod, "--timeout=5m"); err != nil {
		return err
	}
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", archivePath, restorePod+":/restore/backup.tar"); err != nil {
		return err
	}
	_, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "exec", restorePod, "--", "sh", "-ec", "find /data/mycel -mindepth 1 -maxdepth 1 -exec rm -rf {} + && tar -xf /restore/backup.tar -C /data/mycel")
	return err
}

func (d K3DDriver) RestoreBackupSetOrdinalToPVC(ctx context.Context, environment Environment, req RestoreBackupSetRequest) (ExecResult, error) {
	if strings.TrimSpace(req.PVCName) == "" || strings.TrimSpace(req.BackupSetPath) == "" {
		return ExecResult{}, errors.New("pvc name and backup-set path are required")
	}
	if req.Ordinal < 0 {
		return ExecResult{}, errors.New("restore ordinal must be non-negative")
	}
	image := strings.TrimSpace(req.Image)
	if image == "" {
		image = "myceldb/mycel:latest"
	}
	restorePod := "restore-" + strings.ReplaceAll(req.PVCName, "_", "-")
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: restore
      image: %s
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-ec", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data/mycel
        - name: restore
          mountPath: /restore
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
    - name: restore
      emptyDir: {}
`, restorePod, environment.Namespace, image, req.PVCName)
	if _, err := d.ApplyYAML(ctx, environment, manifest); err != nil {
		return ExecResult{}, err
	}
	defer func() {
		_, _ = d.runner().Run(context.Background(), "kubectl", "--context", environment.Context, "-n", environment.Namespace, "delete", "pod", restorePod, "--ignore-not-found=true", "--wait=true", "--timeout=3m")
	}()
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "--for=condition=Ready", "pod/"+restorePod, "--timeout=5m"); err != nil {
		return ExecResult{}, err
	}
	backupDir := "/tmp/mycel-system-backups"
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "exec", restorePod, "--", "sh", "-ec", "rm -rf "+shellQuote(backupDir)+" && mkdir -p "+shellQuote(backupDir)); err != nil {
		return ExecResult{}, err
	}
	if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", req.BackupSetPath, restorePod+":"+backupDir+"/backup-set.json"); err != nil {
		return ExecResult{}, err
	}
	for _, artifact := range req.Artifacts {
		if strings.TrimSpace(artifact.LocalArchive) == "" || strings.TrimSpace(artifact.ArchiveName) == "" {
			return ExecResult{}, fmt.Errorf("restore artifact ordinal %d missing archive evidence", artifact.Ordinal)
		}
		if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", artifact.LocalArchive, restorePod+":"+backupDir+"/"+artifact.ArchiveName); err != nil {
			return ExecResult{}, err
		}
		if strings.TrimSpace(artifact.ManifestName) != "" {
			if strings.TrimSpace(artifact.LocalManifest) == "" {
				return ExecResult{}, fmt.Errorf("restore artifact ordinal %d missing local manifest evidence", artifact.Ordinal)
			}
			if _, err := d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "cp", artifact.LocalManifest, restorePod+":"+backupDir+"/"+artifact.ManifestName); err != nil {
				return ExecResult{}, err
			}
		}
	}
	cmd := fmt.Sprintf("find /data/mycel -mindepth 1 -maxdepth 1 -exec rm -rf {} + && test -z \"$(find /data/mycel -mindepth 1 -maxdepth 1 -print -quit)\" && mycel --output json admin backup cluster restore-local --backup-set %s --ordinal %d --data-dir /data/mycel", shellQuote(backupDir), req.Ordinal)
	return d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "exec", restorePod, "--", "sh", "-ec", cmd)
}

func (d K3DDriver) ProviderEmulatorObservations(ctx context.Context, environment Environment) (ExecResult, error) {
	if environment.Metadata["providerEmulator"] != "true" {
		return ExecResult{}, errors.New("provider emulator is not configured")
	}
	return d.runner().Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "exec", "deployment/mycel-provider-emulator", "--", "/mycel-provider-emulator", "dump", "--file", "/data/observations.jsonl")
}

func (d K3DDriver) CaptureState(ctx context.Context, environment Environment, sink *artifacts.Sink) error {
	if _, err := sink.WriteJSON("environment/state.json", environment); err != nil {
		return err
	}
	runner := d.runner()
	daemonImage := localImageSourceEvidence(ctx, runner, environment.Metadata["imageOverride"])
	if environment.Metadata["requireImageProvenance"] == "true" && (daemonImage.ID == "" || !validImageProvenance(daemonImage.Provenance, "https://github.com/MycelDB/mycel")) {
		return errors.New("daemon image provenance is incomplete")
	}
	if _, err := sink.WriteJSON("environment/daemon-image.json", daemonImage); err != nil {
		return err
	}
	captures := []struct {
		name string
		args []string
	}{
		{name: "environment/kubernetes-resources.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "get", "all,pvc", "-o", "wide"}},
		{name: "environment/pods-describe.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "describe", "pods"}},
		{name: "environment/pod-logs.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "logs", "statefulset/myceld", "--all-containers=true", "--tail=200"}},
		{name: "environment/pod-logs-all.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "logs", "-l", "app=myceld", "--all-containers=true", "--prefix=true", "--tail=5000"}},
		{name: "environment/pod-logs-previous.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "logs", "-l", "app=myceld", "--all-containers=true", "--prefix=true", "--previous", "--tail=5000"}},
	}
	for _, capture := range captures {
		result, err := runner.Run(ctx, "kubectl", capture.args...)
		content := formatCommandResult(result, err)
		if _, writeErr := sink.WriteText(capture.name, content); writeErr != nil {
			return writeErr
		}
	}
	if environment.Metadata["providerEmulator"] == "true" {
		imageID, imageErr := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "get", "pods", "-l", "app=mycel-provider-emulator", "-o", "jsonpath={range .items[*].status.containerStatuses[*]}{.imageID}{\"\\n\"}{end}")
		if imageErr != nil {
			return fmt.Errorf("capture provider emulator image ID: %w", imageErr)
		}
		if strings.TrimSpace(imageID.Stdout) == "" {
			return errors.New("capture provider emulator image ID: empty image ID")
		}
		emulatorImage := localImageSourceEvidence(ctx, runner, environment.Metadata["providerEmulatorImage"])
		emulatorImage.Service = "mycel-provider-emulator"
		emulatorImage.ScriptSHA256 = environment.Metadata["providerEmulatorScriptSHA256"]
		emulatorImage.ContainerName = strings.TrimSpace(imageID.Stdout)
		if environment.Metadata["requireImageProvenance"] == "true" && (emulatorImage.ID == "" || !validImageProvenance(emulatorImage.Provenance, "https://github.com/MycelDB/mycel-lab")) {
			return errors.New("provider emulator image provenance is incomplete")
		}
		if _, err := sink.WriteJSON("provider/emulator-image.json", emulatorImage); err != nil {
			return err
		}
		observations, err := d.ProviderEmulatorObservations(ctx, environment)
		if err != nil {
			return fmt.Errorf("capture provider emulator observations: %w", err)
		}
		if err := PersistProviderEmulatorObservations(sink, observations.Stdout, false); err != nil {
			return err
		}
		logs, logErr := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "logs", "deployment/mycel-provider-emulator", "--tail=1000")
		if _, err := sink.WriteText("provider/emulator.log", formatCommandResult(logs, logErr)); err != nil {
			return err
		}
	}
	return nil
}

func (d K3DDriver) runner() executil.Runner {
	if d.Runner != nil {
		return d.Runner
	}
	return executil.LocalRunner{}
}

type ComposeDriver struct {
	Confirmed   bool
	Runner      executil.Runner
	WaitTimeout time.Duration
}

func (d ComposeDriver) Name() string { return "compose" }

func (d ComposeDriver) Capabilities() CapabilitySet {
	return NewCapabilitySet(CapabilityArtifacts, CapabilityPerNodeEndpoints, CapabilityNodeExec, CapabilityNodeRestart, CapabilityRollingRestart, CapabilityLogs, CapabilityObjectStoreFixture, CapabilityProviderEmulator)
}

func (d ComposeDriver) Validate(environment spec.EnvironmentSpec) error {
	return rejectUnknownOptions(environment.Options, "composeFile", "composeFiles", "projectName", "projectNamePrefix", "serviceNames", "grpcPorts", "objectStoreFixture", "imageOverride", "providerEmulatorImage")
}

func (d ComposeDriver) Preflight(ctx context.Context, environment spec.EnvironmentSpec) error {
	if !d.Confirmed {
		return RequireDestructiveConfirmation(Options{})
	}
	if _, err := d.runner().Run(ctx, "docker", "compose", "version"); err != nil {
		return fmt.Errorf("docker compose preflight failed: %w", err)
	}
	composeFiles := composeFileOptions(environment)
	if len(composeFiles) == 0 {
		return errors.New("compose environment requires options.composeFile or options.composeFiles")
	}
	for _, composeFile := range composeFiles {
		if !filepath.IsAbs(composeFile) {
			abs, err := filepath.Abs(composeFile)
			if err != nil {
				return err
			}
			composeFile = abs
		}
		if _, err := os.Stat(composeFile); err != nil {
			return fmt.Errorf("compose file %s is not readable: %w", composeFile, err)
		}
	}
	if ports := intSliceOption(environment.Options, "grpcPorts"); len(ports) > 0 {
		if err := ensureHostPortsAvailable(ports); err != nil {
			return err
		}
	}
	if image := strings.TrimSpace(stringOption(environment.Options, "providerEmulatorImage", "")); image != "" {
		if _, err := d.runner().Run(ctx, "docker", "image", "inspect", image); err != nil {
			return fmt.Errorf("provider emulator image %s is unavailable: %w", image, err)
		}
	}
	return nil
}

func ensureHostPortsAvailable(ports []int) error {
	for _, port := range ports {
		if port <= 0 {
			continue
		}
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return fmt.Errorf("compose preflight failed: host port %d is not available: %w", port, err)
		}
		_ = listener.Close()
	}
	return nil
}

func (d ComposeDriver) Create(ctx context.Context, scenario spec.ResolvedScenario, environmentSpec spec.EnvironmentSpec) (Environment, error) {
	if !d.Confirmed {
		return Environment{}, RequireDestructiveConfirmation(Options{})
	}
	composeFiles := composeFileOptions(environmentSpec)
	if len(composeFiles) == 0 {
		return Environment{}, errors.New("compose environment requires options.composeFile or options.composeFiles")
	}
	for i, composeFile := range composeFiles {
		if !filepath.IsAbs(composeFile) {
			abs, err := filepath.Abs(composeFile)
			if err != nil {
				return Environment{}, err
			}
			composeFiles[i] = abs
		}
	}
	generatedEmulatorFile := ""
	if scenario.ProviderProfile != nil && string(scenario.ProviderProfile.Profile.Mode) == "emulator" {
		var err error
		generatedEmulatorFile, err = writeComposeProviderEmulatorOverride(scenario, stringOption(environmentSpec.Options, "providerEmulatorImage", "local/mycel-provider-emulator:dev"))
		if err != nil {
			return Environment{}, err
		}
		composeFiles = append(composeFiles, generatedEmulatorFile)
	}
	removeGeneratedEmulatorFile := generatedEmulatorFile != ""
	defer func() {
		if removeGeneratedEmulatorFile {
			_ = os.RemoveAll(filepath.Dir(generatedEmulatorFile))
		}
	}()
	projectName := stringOption(environmentSpec.Options, "projectName", "")
	if projectName == "" {
		projectName = composeProjectName(scenario.Metadata.Name, stringOption(environmentSpec.Options, "projectNamePrefix", "mycel-lab"))
	}
	metadata := map[string]string{"composeFile": composeFiles[0], "composeFiles": strings.Join(composeFiles, string(os.PathListSeparator)), "projectName": projectName, "nodeCount": strconv.Itoa(scenario.Cluster.Nodes)}
	if generatedEmulatorFile != "" {
		scriptHash, err := providerEmulatorScriptSHA256(scenario)
		if err != nil {
			_ = os.RemoveAll(filepath.Dir(generatedEmulatorFile))
			return Environment{}, err
		}
		metadata["providerEmulator"] = "true"
		metadata["providerEmulatorImage"] = stringOption(environmentSpec.Options, "providerEmulatorImage", "local/mycel-provider-emulator:dev")
		metadata["providerEmulatorScriptSHA256"] = scriptHash
		metadata["generatedProviderEmulatorFile"] = generatedEmulatorFile
	}
	if image := strings.TrimSpace(stringOption(environmentSpec.Options, "imageOverride", "")); image != "" {
		metadata["imageOverride"] = image
	}
	if serviceNames := stringSliceOption(environmentSpec.Options, "serviceNames"); len(serviceNames) > 0 {
		metadata["serviceNames"] = strings.Join(serviceNames, ",")
	}
	if grpcPorts := intSliceOption(environmentSpec.Options, "grpcPorts"); len(grpcPorts) > 0 {
		parts := make([]string, 0, len(grpcPorts))
		for _, port := range grpcPorts {
			parts = append(parts, strconv.Itoa(port))
		}
		metadata["grpcPorts"] = strings.Join(parts, ",")
	}
	if objectStore := stringOption(environmentSpec.Options, "objectStoreFixture", ""); objectStore != "" {
		metadata["objectStoreFixture"] = objectStore
	}
	environment := Environment{Name: projectName, Driver: "compose", Namespace: environmentSpec.Namespace, Metadata: metadata}
	if _, err := d.compose(ctx, environment, "down", "--volumes", "--remove-orphans"); err != nil {
		return Environment{}, err
	}
	if _, err := d.compose(ctx, environment, "up", "-d", "--wait"); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	if err := d.WaitReady(ctx, environment); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
	}
	removeGeneratedEmulatorFile = false
	return environment, nil
}

func (d ComposeDriver) WaitReady(ctx context.Context, environment Environment) error {
	_, err := d.compose(ctx, environment, "ps")
	return err
}

func (d ComposeDriver) Nodes(_ context.Context, environment Environment) ([]Node, error) {
	services := composeServiceNames(environment)
	nodes := make([]Node, 0, len(services))
	for i, service := range services {
		nodes = append(nodes, Node{Name: fmt.Sprintf("myceld-%d", i), Ordinal: i, Role: "daemon", Resource: service, Metadata: map[string]string{"service": service}})
	}
	return nodes, nil
}

func (d ComposeDriver) Endpoints(ctx context.Context, environment Environment) ([]Endpoint, error) {
	services := composeServiceNames(environment)
	ports := composeGRPCPorts(environment)
	endpoints := make([]Endpoint, 0, len(services))
	for i, service := range services {
		port := 0
		if i < len(ports) {
			port = ports[i]
		} else {
			result, err := d.compose(ctx, environment, "port", service, strconv.Itoa(DefaultDaemonGRPCPort))
			if err != nil {
				return nil, fmt.Errorf("discover compose port for %s: %w", service, err)
			}
			port = parsePublishedPort(result.Stdout)
		}
		if port <= 0 {
			return nil, fmt.Errorf("compose service %s does not expose daemon port %d", service, DefaultDaemonGRPCPort)
		}
		endpoints = append(endpoints, Endpoint{NodeName: fmt.Sprintf("myceld-%d", i), ServiceName: service, LocalAddress: "127.0.0.1", LocalPort: port, RemotePort: DefaultDaemonGRPCPort, DaemonAddr: fmt.Sprintf("127.0.0.1:%d", port), Scheme: "grpc"})
	}
	return endpoints, nil
}

func (d ComposeDriver) Exec(ctx context.Context, environment Environment, node NodeRef, req ExecRequest) (ExecResult, error) {
	if len(req.Command) == 0 {
		return ExecResult{}, errors.New("exec command is required")
	}
	service := composeNodeService(environment, node)
	args := []string{"exec", "-T", service}
	args = append(args, req.Command...)
	return d.compose(ctx, environment, args...)
}

func (d ComposeDriver) RestartNode(ctx context.Context, environment Environment, node NodeRef) error {
	service := composeNodeService(environment, node)
	if _, err := d.compose(ctx, environment, "restart", service); err != nil {
		return err
	}
	return d.WaitReady(ctx, environment)
}

func (d ComposeDriver) RollingRestart(ctx context.Context, environment Environment) error {
	for i, service := range composeServiceNames(environment) {
		if err := d.RestartNode(ctx, environment, NodeRef{Name: service, Ordinal: i}); err != nil {
			return err
		}
	}
	return nil
}

func (d ComposeDriver) Reset(ctx context.Context, environment Environment) error {
	if _, err := d.compose(ctx, environment, "down", "--volumes", "--remove-orphans"); err != nil {
		return err
	}
	if _, err := d.compose(ctx, environment, "up", "-d", "--wait"); err != nil {
		return err
	}
	return d.WaitReady(ctx, environment)
}

func (d ComposeDriver) CopyFromNode(ctx context.Context, environment Environment, node NodeRef, containerPath, hostPath string) (ExecResult, error) {
	if containerPath == "" || hostPath == "" {
		return ExecResult{}, errors.New("container and host paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return ExecResult{}, err
	}
	service := composeNodeService(environment, node)
	return d.compose(ctx, environment, "cp", service+":"+containerPath, hostPath)
}

func (d ComposeDriver) CopyToNode(ctx context.Context, environment Environment, node NodeRef, hostPath, containerPath string) (ExecResult, error) {
	if hostPath == "" || containerPath == "" {
		return ExecResult{}, errors.New("host and container paths are required")
	}
	service := composeNodeService(environment, node)
	return d.compose(ctx, environment, "cp", hostPath, service+":"+containerPath)
}

type composeImageEvidence struct {
	Service       string                `json:"service,omitempty"`
	ImageOverride string                `json:"imageOverride,omitempty"`
	ScriptSHA256  string                `json:"scriptSHA256,omitempty"`
	ID            string                `json:"id"`
	ContainerName string                `json:"containerName"`
	Repository    string                `json:"repository,omitempty"`
	Tag           string                `json:"tag,omitempty"`
	Platform      string                `json:"platform,omitempty"`
	Provenance    imageSourceProvenance `json:"provenance"`
}

type imageSourceProvenance struct {
	CaptureStatus string `json:"captureStatus"`
	Revision      string `json:"revision,omitempty"`
	Source        string `json:"source,omitempty"`
	Dirty         string `json:"dirty,omitempty"`
	ContextDigest string `json:"contextDigest,omitempty"`
	BuildCommand  string `json:"buildCommand,omitempty"`
}

func parseComposeImageEvidence(raw string) ([]composeImageEvidence, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("compose image output is empty")
	}
	var images []composeImageEvidence
	if strings.HasPrefix(raw, "[") {
		if err := json.Unmarshal([]byte(raw), &images); err != nil {
			return nil, err
		}
		return images, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	for {
		var image composeImageEvidence
		if err := decoder.Decode(&image); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		images = append(images, image)
	}
	if len(images) == 0 {
		return nil, errors.New("compose image output contains no records")
	}
	return images, nil
}

func (d ComposeDriver) ProviderEmulatorObservations(ctx context.Context, environment Environment) (ExecResult, error) {
	if environment.Metadata["providerEmulator"] != "true" {
		return ExecResult{}, errors.New("provider emulator is not configured")
	}
	return d.compose(ctx, environment, "exec", "-T", "mycel-provider-emulator", "/mycel-provider-emulator", "dump", "--file", "/var/lib/mycel-emulator/observations.jsonl")
}

func (d ComposeDriver) imageProvenance(ctx context.Context, imageID string) imageSourceProvenance {
	return captureImageProvenance(ctx, d.runner(), imageID)
}

func captureImageProvenance(ctx context.Context, runner executil.Runner, imageID string) imageSourceProvenance {
	provenance := imageSourceProvenance{CaptureStatus: "unavailable"}
	if strings.TrimSpace(imageID) == "" {
		return provenance
	}
	result, err := runner.Run(ctx, "docker", "image", "inspect", imageID, "--format", "{{json .Config.Labels}}")
	if err != nil {
		return provenance
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(result.Stdout)), &labels); err != nil {
		provenance.CaptureStatus = "invalid"
		return provenance
	}
	provenance.CaptureStatus = "completed"
	provenance.Revision = safeFullRevision(labels["org.opencontainers.image.revision"])
	provenance.Source = allowlistedDiagnosticValue(labels["org.opencontainers.image.source"], "https://github.com/MycelDB/mycel", "https://github.com/MycelDB/mycel-lab")
	provenance.Dirty = allowlistedDiagnosticValue(labels["io.myceldb.source.dirty"], "true", "false")
	provenance.ContextDigest = safeSHA256(labels["io.myceldb.build.context-digest"])
	provenance.BuildCommand = allowlistedDiagnosticValue(labels["io.myceldb.build.command"], "docker-buildx-local-linux-arm64", "docker-buildx-local-linux-arm64-emulator", "github-actions-buildx-multiarch")
	if provenance.Revision == "" || provenance.Source == "" || provenance.Dirty == "" || provenance.ContextDigest == "" || provenance.BuildCommand == "" {
		provenance.CaptureStatus = "incomplete"
	}
	return provenance
}

func localImageSourceEvidence(ctx context.Context, runner executil.Runner, image string) composeImageEvidence {
	evidence := composeImageEvidence{ImageOverride: strings.TrimSpace(image), Provenance: imageSourceProvenance{CaptureStatus: "unavailable"}}
	if evidence.ImageOverride == "" {
		return evidence
	}
	result, err := runner.Run(ctx, "docker", "image", "inspect", evidence.ImageOverride, "--format", "{{.Id}}")
	if err != nil {
		return evidence
	}
	evidence.ID = strings.TrimSpace(result.Stdout)
	evidence.Provenance = captureImageProvenance(ctx, runner, evidence.ID)
	return evidence
}

func validImageProvenance(provenance imageSourceProvenance, expectedSource string) bool {
	return provenance.CaptureStatus == "completed" && provenance.Source == expectedSource && provenance.Dirty == "false" && provenance.Revision != "" && provenance.ContextDigest != "" && provenance.BuildCommand != ""
}

func safeFullRevision(value string) string {
	if len(value) != 40 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return strings.ToLower(value)
}

func (d ComposeDriver) CaptureState(ctx context.Context, environment Environment, sink *artifacts.Sink) error {
	if _, err := sink.WriteJSON("environment/state.json", environment); err != nil {
		return err
	}
	captures := []struct {
		name string
		args []string
	}{
		{name: "environment/compose-ps.txt", args: []string{"ps"}},
		{name: "environment/compose-config.yaml", args: []string{"config"}},
	}
	for _, capture := range captures {
		result, err := d.compose(ctx, environment, capture.args...)
		if _, writeErr := sink.WriteText(capture.name, formatCommandResult(result, err)); writeErr != nil {
			return writeErr
		}
	}
	services := composeServiceNames(environment)
	imageEvidence := make([]composeImageEvidence, 0, len(services))
	for _, service := range services {
		result, err := d.compose(ctx, environment, "images", "--format", "json", service)
		if _, writeErr := sink.WriteText(filepath.Join("environment", "images", service+".txt"), formatCommandResult(result, err)); writeErr != nil {
			return writeErr
		}
		if err != nil {
			return fmt.Errorf("capture compose image for %s: %w", service, err)
		}
		images, err := parseComposeImageEvidence(result.Stdout)
		if err != nil {
			return fmt.Errorf("parse compose image for %s: %w", service, err)
		}
		if len(images) != 1 || strings.TrimSpace(images[0].ID) == "" || strings.TrimSpace(images[0].ContainerName) == "" {
			return fmt.Errorf("compose image evidence for %s must contain exactly one container with a nonempty image ID", service)
		}
		image := images[0]
		image.Service = service
		image.ImageOverride = environment.Metadata["imageOverride"]
		image.Provenance = d.imageProvenance(ctx, image.ID)
		if environment.Metadata["requireImageProvenance"] == "true" && !validImageProvenance(image.Provenance, "https://github.com/MycelDB/mycel") {
			return errors.New("daemon image provenance is incomplete")
		}
		imageEvidence = append(imageEvidence, image)
	}
	if len(imageEvidence) != len(services) {
		return fmt.Errorf("compose image evidence is incomplete")
	}
	var encodedEvidence strings.Builder
	for _, image := range imageEvidence {
		encoded, err := json.Marshal(image)
		if err != nil {
			return err
		}
		encodedEvidence.Write(encoded)
		encodedEvidence.WriteByte('\n')
	}
	if _, err := sink.WriteText("environment/compose-images.jsonl", encodedEvidence.String()); err != nil {
		return err
	}
	if environment.Metadata["providerEmulator"] == "true" {
		result, err := d.compose(ctx, environment, "images", "--format", "json", "mycel-provider-emulator")
		if err != nil {
			return fmt.Errorf("capture provider emulator image: %w", err)
		}
		images, err := parseComposeImageEvidence(result.Stdout)
		if err != nil || len(images) != 1 || strings.TrimSpace(images[0].ID) == "" {
			return fmt.Errorf("provider emulator image evidence must contain exactly one nonempty image ID")
		}
		images[0].Service = "mycel-provider-emulator"
		images[0].ImageOverride = environment.Metadata["providerEmulatorImage"]
		images[0].ScriptSHA256 = environment.Metadata["providerEmulatorScriptSHA256"]
		images[0].Provenance = d.imageProvenance(ctx, images[0].ID)
		if environment.Metadata["requireImageProvenance"] == "true" && !validImageProvenance(images[0].Provenance, "https://github.com/MycelDB/mycel-lab") {
			return errors.New("provider emulator image provenance is incomplete")
		}
		if _, err := sink.WriteJSON("provider/emulator-image.json", images[0]); err != nil {
			return err
		}
		observations, err := d.ProviderEmulatorObservations(ctx, environment)
		if err != nil {
			return fmt.Errorf("capture provider emulator observations: %w", err)
		}
		if err := PersistProviderEmulatorObservations(sink, observations.Stdout, false); err != nil {
			return err
		}
		logs, logErr := d.compose(ctx, environment, "logs", "--no-color", "--tail=1000", "mycel-provider-emulator")
		if _, err := sink.WriteText("provider/emulator.log", formatCommandResult(logs, logErr)); err != nil {
			return err
		}
	}
	for _, service := range services {
		result, err := d.compose(ctx, environment, "logs", "--no-color", "--tail=5000", service)
		if _, writeErr := sink.WriteText(filepath.Join("environment", "logs", service+".log"), formatCommandResult(result, err)); writeErr != nil {
			return writeErr
		}
		diagnosticResult, diagnosticErr := d.compose(ctx, environment, "exec", "-T", service, "sh", "-c", "if [ -f /data/mycel/log/myceld.log ]; then tail -c 1048576 /data/mycel/log/myceld.log | tail -n 5000; fi")
		diagnostics := parseProcedureInferenceDaemonDiagnostics(service, diagnosticResult.Stdout)
		if diagnosticErr != nil {
			diagnostics.CaptureStatus = "unavailable"
		}
		if _, writeErr := sink.WriteJSON(filepath.Join("environment", "logs", service+"-procedure-inference.json"), diagnostics); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

type procedureInferenceDaemonDiagnostics struct {
	SchemaVersion string                               `json:"schemaVersion"`
	Service       string                               `json:"service"`
	CaptureStatus string                               `json:"captureStatus"`
	Records       []procedureInferenceDaemonDiagnostic `json:"records"`
}

type procedureInferenceDaemonDiagnostic struct {
	Time                        string `json:"time,omitempty"`
	Level                       string `json:"level,omitempty"`
	Message                     string `json:"message"`
	Stage                       string `json:"stage,omitempty"`
	Result                      string `json:"result,omitempty"`
	Category                    string `json:"category,omitempty"`
	RunIDHash                   string `json:"runIdHash,omitempty"`
	AttemptIDHash               string `json:"attemptIdHash,omitempty"`
	ClaimOwnerNodeID            uint64 `json:"claimOwnerNodeId,omitempty"`
	ClaimVersion                uint64 `json:"claimVersion,omitempty"`
	AuthorizationRevision       uint64 `json:"authorizationRevision,omitempty"`
	GlobalAuthorizationRevision uint64 `json:"globalAuthorizationRevision,omitempty"`
	SpaceAuthorizationRevision  uint64 `json:"spaceAuthorizationRevision,omitempty"`
	OutputKeyHash               string `json:"outputKeyHash,omitempty"`
	GraphRevision               int64  `json:"graphRevision,omitempty"`
}

var procedureInferenceDiagnosticStages = map[string]struct{}{
	"runtime_control": {}, "output_parse": {}, "transaction_reopen": {}, "pending_persistence": {},
	"procedure_control_recheck": {}, "authorization_recheck": {}, "conflict_transaction_reopen": {},
	"claim_check": {}, "claim_renewal": {}, "receipt_probe": {}, "action_apply": {},
	"precommit_claim_renewal": {}, "global_authorization_fence": {}, "procedure_control_fence": {},
	"graph_commit": {}, "publication": {}, "session_finalize": {}, "graph_finalize_ack": {},
	"final_disposition": {}, "receipt_acknowledgement": {}, "recovery_output_probe": {},
	"recovery_receipt_acknowledgement": {},
}

var procedureInferenceDiagnosticCategories = map[string]struct{}{
	"none": {}, "committed": {}, "acknowledged": {}, "published": {}, "publication_failed": {},
	"deadline_exceeded": {}, "canceled": {}, "procedure_disabled": {}, "authorization_revoked": {},
	"graph_conflict": {}, "failed_precondition": {}, "unavailable": {}, "conflict": {}, "not_found": {},
	"permission_denied": {}, "resource_exhausted": {}, "operation_failed": {},
}

func parseProcedureInferenceDaemonDiagnostics(service, raw string) procedureInferenceDaemonDiagnostics {
	const maxDiagnosticBytes = 1024 * 1024
	if len(raw) > maxDiagnosticBytes {
		raw = raw[len(raw)-maxDiagnosticBytes:]
	}
	evidence := procedureInferenceDaemonDiagnostics{
		SchemaVersion: "reliability.mycel.io/procedure-inference-daemon-diagnostics/v1alpha1",
		Service:       service,
		CaptureStatus: "completed",
		Records:       []procedureInferenceDaemonDiagnostic{},
	}
	lines := strings.Split(raw, "\n")
	if len(lines) > 5000 {
		lines = lines[len(lines)-5000:]
	}
	for _, line := range lines {
		if len(line) == 0 || len(line) > 64*1024 {
			continue
		}
		var decoded map[string]any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			continue
		}
		message, _ := decoded["msg"].(string)
		if message != "procedure_inference_publication_stage" && message != "automation_worker_process_pending_failed" {
			continue
		}
		record := procedureInferenceDaemonDiagnostic{Message: message}
		record.Time = safeDiagnosticTime(decoded["time"])
		record.Level = allowlistedDiagnosticValue(decoded["level"], "INFO", "WARN", "ERROR")
		record.Category = allowlistedDiagnosticMapValue(decoded["category"], procedureInferenceDiagnosticCategories)
		if message == "procedure_inference_publication_stage" {
			record.Stage = allowlistedDiagnosticMapValue(decoded["stage"], procedureInferenceDiagnosticStages)
			record.Result = allowlistedDiagnosticValue(decoded["result"], "started", "completed", "failed")
			record.RunIDHash = diagnosticCorrelationHash(decoded["run_id"])
			record.AttemptIDHash = diagnosticCorrelationHash(decoded["attempt_id"])
			record.ClaimOwnerNodeID = diagnosticUint64(decoded["claim_owner_node_id"])
			record.ClaimVersion = diagnosticUint64(decoded["claim_version"])
			record.AuthorizationRevision = diagnosticUint64(decoded["authorization_revision"])
			record.GlobalAuthorizationRevision = diagnosticUint64(decoded["global_authorization_revision"])
			record.SpaceAuthorizationRevision = diagnosticUint64(decoded["space_authorization_revision"])
			record.OutputKeyHash = safeSHA256(decoded["output_key_hash"])
			record.GraphRevision = diagnosticInt64(decoded["graph_revision"])
		}
		evidence.Records = append(evidence.Records, record)
	}
	return evidence
}

func safeDiagnosticTime(value any) string {
	raw, _ := value.(string)
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return ""
	}
	return parsed.UTC().Format(time.RFC3339Nano)
}

func allowlistedDiagnosticValue(value any, allowed ...string) string {
	raw, _ := value.(string)
	for _, candidate := range allowed {
		if raw == candidate {
			return raw
		}
	}
	if raw != "" {
		return "invalid"
	}
	return ""
}

func allowlistedDiagnosticMapValue(value any, allowed map[string]struct{}) string {
	raw, _ := value.(string)
	if _, ok := allowed[raw]; ok {
		return raw
	}
	if raw != "" {
		return "invalid"
	}
	return ""
}

func diagnosticCorrelationHash(value any) string {
	raw, _ := value.(string)
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func safeSHA256(value any) string {
	raw, _ := value.(string)
	if len(raw) != sha256.Size*2 {
		return ""
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return ""
	}
	return strings.ToLower(raw)
}

func diagnosticUint64(value any) uint64 {
	raw, ok := value.(json.Number)
	if !ok {
		return 0
	}
	parsed, err := strconv.ParseUint(raw.String(), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func diagnosticInt64(value any) int64 {
	raw, ok := value.(json.Number)
	if !ok {
		return 0
	}
	parsed, err := strconv.ParseInt(raw.String(), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func (d ComposeDriver) Delete(ctx context.Context, environment Environment) error {
	_, err := d.compose(ctx, environment, "down", "--volumes", "--remove-orphans")
	if generated := strings.TrimSpace(environment.Metadata["generatedProviderEmulatorFile"]); generated != "" {
		_ = os.RemoveAll(filepath.Dir(generated))
	}
	return err
}

func (d ComposeDriver) VerifyDeleted(ctx context.Context, environment Environment) (DeletionVerification, error) {
	verification := DeletionVerification{Supported: true}
	project := strings.TrimSpace(environment.Metadata["projectName"])
	if project == "" {
		return verification, errors.New("compose deletion verification requires a project name")
	}
	filter := "label=com.docker.compose.project=" + project
	checks := []struct {
		kind string
		args []string
	}{
		{kind: "containers", args: []string{"ps", "-a", "--filter", filter, "--format", "{{.ID}}"}},
		{kind: "networks", args: []string{"network", "ls", "--filter", filter, "--format", "{{.ID}}"}},
		{kind: "volumes", args: []string{"volume", "ls", "--filter", filter, "--format", "{{.Name}}"}},
	}
	for _, check := range checks {
		result, err := d.runner().Run(ctx, "docker", check.args...)
		if err != nil {
			return verification, fmt.Errorf("compose %s absence check failed", check.kind)
		}
		count := countNonemptyLines(result.Stdout)
		switch check.kind {
		case "containers":
			verification.Containers = count
		case "networks":
			verification.Networks = count
		case "volumes":
			verification.Volumes = count
		}
	}
	verification.Completed = verification.Containers == 0 && verification.Networks == 0 && verification.Volumes == 0
	if !verification.Completed {
		return verification, errors.New("compose resources remain after deletion")
	}
	return verification, nil
}

func countNonemptyLines(value string) int {
	count := 0
	for _, line := range strings.Split(value, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func providerEmulatorScriptSHA256(scenario spec.ResolvedScenario) (string, error) {
	if scenario.ProviderProfile == nil {
		return "", errors.New("provider emulator requires a provider profile")
	}
	capability, _, err := scenario.ProviderProfile.Profile.ProtocolEmulatorCapability()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(capability.EmulatorScript)
	if err != nil {
		return "", fmt.Errorf("encode provider emulator script identity: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func writeComposeProviderEmulatorOverride(scenario spec.ResolvedScenario, image string) (string, error) {
	if scenario.ProviderProfile == nil {
		return "", errors.New("compose provider emulator requires a provider profile")
	}
	capability, _, err := scenario.ProviderProfile.Profile.ProtocolEmulatorCapability()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(capability.EmulatorScript)
	if err != nil {
		return "", fmt.Errorf("encode compose provider emulator script: %w", err)
	}
	directory, err := os.MkdirTemp("", "mycel-lab-provider-emulator-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(directory, "compose.yml")
	content := fmt.Sprintf("services:\n  mycel-provider-emulator:\n    image: %s\n    command: [\"serve\"]\n    environment:\n      MYCEL_EMULATOR_DIMENSIONS: %q\n      MYCEL_EMULATOR_SCRIPT_B64: %q\n      MYCEL_EMULATOR_OBSERVATIONS: /var/lib/mycel-emulator/observations.jsonl\n    volumes:\n      - mycel-provider-emulator-data:/var/lib/mycel-emulator\n    healthcheck:\n      test: [\"CMD\", \"/mycel-provider-emulator\", \"health\"]\n      interval: 1s\n      timeout: 2s\n      retries: 30\nvolumes:\n  mycel-provider-emulator-data:\n", strings.TrimSpace(image), strconv.Itoa(providerEmulatorDimensions(capability.Dimensions)), base64.StdEncoding.EncodeToString(raw))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		_ = os.RemoveAll(directory)
		return "", err
	}
	return path, nil
}

func providerEmulatorDimensions(configured int) int {
	if configured > 0 {
		return configured
	}
	return 8
}

func (d ComposeDriver) compose(ctx context.Context, environment Environment, args ...string) (executil.Result, error) {
	base := []string{}
	files := filepath.SplitList(environment.Metadata["composeFiles"])
	if len(files) == 0 && environment.Metadata["composeFile"] != "" {
		files = []string{environment.Metadata["composeFile"]}
	}
	for _, file := range files {
		if file != "" {
			base = append(base, "-f", file)
		}
	}
	if project := environment.Metadata["projectName"]; project != "" {
		base = append(base, "-p", project)
	}
	base = append(base, args...)
	dockerArgs := append([]string{"compose"}, base...)
	if image := strings.TrimSpace(environment.Metadata["imageOverride"]); image != "" {
		return d.runner().Run(ctx, "env", append([]string{"MYCEL_IMAGE=" + image, "docker"}, dockerArgs...)...)
	}
	return d.runner().Run(ctx, "docker", dockerArgs...)
}

func (d ComposeDriver) runner() executil.Runner {
	if d.Runner != nil {
		return d.Runner
	}
	return executil.LocalRunner{}
}

func rejectUnknownOptions(options map[string]any, allowed ...string) error {
	if len(options) == 0 {
		return nil
	}
	allowedSet := map[string]struct{}{}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range options {
		if _, ok := allowedSet[key]; !ok {
			return fmt.Errorf("unsupported environment option %q", key)
		}
	}
	return nil
}

func stringOption(options map[string]any, key, fallback string) string {
	if options == nil {
		return fallback
	}
	value, ok := options[key]
	if !ok || value == nil {
		return fallback
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func intOption(options map[string]any, key string, fallback int) int {
	if options == nil {
		return fallback
	}
	value, ok := options[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func stringSliceOption(options map[string]any, key string) []string {
	if options == nil {
		return nil
	}
	value, ok := options[key]
	if !ok || value == nil {
		return nil
	}
	switch v := value.(type) {
	case []string:
		return append([]string(nil), v...)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, strings.TrimSpace(fmt.Sprint(item)))
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			out = append(out, strings.TrimSpace(part))
		}
		return out
	default:
		return []string{strings.TrimSpace(fmt.Sprint(v))}
	}
}

func intSliceOption(options map[string]any, key string) []int {
	values := stringSliceOption(options, key)
	out := make([]int, 0, len(values))
	for _, value := range values {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			out = append(out, parsed)
		}
	}
	return out
}

func composeFileOptions(environment spec.EnvironmentSpec) []string {
	files := stringSliceOption(environment.Options, "composeFiles")
	if len(files) == 0 {
		if file := stringOption(environment.Options, "composeFile", ""); file != "" {
			files = []string{file}
		}
	}
	return files
}

func k3dPodName(node NodeRef) string {
	if node.Name != "" {
		return node.Name
	}
	if node.Ordinal >= 0 {
		return fmt.Sprintf("myceld-%d", node.Ordinal)
	}
	return "myceld-0"
}

func composeServiceNames(environment Environment) []string {
	if raw := environment.Metadata["serviceNames"]; raw != "" {
		return splitCSV(raw)
	}
	count := intFromMetadata(environment.Metadata, "nodeCount", 1)
	if count <= 0 {
		count = 1
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, fmt.Sprintf("myceld-%d", i))
	}
	return out
}

func composeGRPCPorts(environment Environment) []int {
	parts := splitCSV(environment.Metadata["grpcPorts"])
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		port, err := strconv.Atoi(part)
		if err == nil {
			out = append(out, port)
		}
	}
	return out
}

func composeNodeService(environment Environment, node NodeRef) string {
	services := composeServiceNames(environment)
	if node.Ordinal >= 0 && node.Ordinal < len(services) {
		return services[node.Ordinal]
	}
	if node.Name != "" {
		return node.Name
	}
	if len(services) > 0 {
		return services[0]
	}
	return "myceld-0"
}

func parsePublishedPort(stdout string) int {
	stdout = strings.TrimSpace(stdout)
	if stdout == "" {
		return 0
	}
	fields := strings.Fields(stdout)
	last := fields[len(fields)-1]
	idx := strings.LastIndex(last, ":")
	if idx >= 0 {
		last = last[idx+1:]
	}
	port, err := strconv.Atoi(strings.TrimSpace(last))
	if err != nil {
		return 0
	}
	return port
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func intFromMetadata(metadata map[string]string, key string, fallback int) int {
	if metadata == nil {
		return fallback
	}
	value := strings.TrimSpace(metadata[key])
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

const k3dClusterNameMaxLength = 32

func k3dClusterName(name, prefix string) string {
	base := sanitizeName(name)
	if base == "" {
		base = "scenario"
	}
	prefix = sanitizeName(prefix)
	if prefix == "" {
		prefix = "mlab"
	}
	base = trimNamePart(base, 14)
	stamp := time.Now().UTC().Format("060102150405")
	nameBudget := k3dClusterNameMaxLength - len(stamp) - 2
	if nameBudget < 2 {
		return trimNamePart(prefix+"-"+base+"-"+stamp, k3dClusterNameMaxLength)
	}
	if len(prefix)+len(base) > nameBudget {
		baseBudget := nameBudget - len(prefix)
		if baseBudget < 3 {
			baseBudget = 3
			prefix = trimNamePart(prefix, nameBudget-baseBudget)
		}
		base = trimNamePart(base, baseBudget)
	}
	return prefix + "-" + base + "-" + stamp
}

func trimNamePart(value string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}
	if len(value) <= maxLength {
		return strings.Trim(value, "-")
	}
	return strings.Trim(value[:maxLength], "-")
}

func composeProjectName(name, prefix string) string {
	base := sanitizeName(name)
	if base == "" {
		base = "scenario"
	}
	if prefix == "" {
		prefix = "mycel-lab"
	}
	return sanitizeName(prefix) + "-" + base + "-" + time.Now().UTC().Format("060102150405")
}

func sanitizeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func ordinalFromName(name string, fallback int) int {
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return fallback
	}
	parsed, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return fallback
	}
	return parsed
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func writeTempManifest(clusterName, yaml string) (string, error) {
	dir, err := os.MkdirTemp("", clusterName+"-manifest-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "myceld.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func formatCommandResult(result executil.Result, err error) string {
	var b strings.Builder
	if result.Command != "" {
		b.WriteString("$ ")
		b.WriteString(result.Command)
		b.WriteByte('\n')
	}
	if result.Stdout != "" {
		b.WriteString("\nstdout:\n")
		b.WriteString(result.Stdout)
	}
	if result.Stderr != "" {
		b.WriteString("\nstderr:\n")
		b.WriteString(result.Stderr)
	}
	if err != nil {
		b.WriteString("\nerror:\n")
		b.WriteString(err.Error())
		b.WriteByte('\n')
	}
	return b.String()
}
