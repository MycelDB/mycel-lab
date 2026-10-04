package env

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
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
	return NewCapabilitySet(CapabilityArtifacts, CapabilityPerNodeEndpoints, CapabilityNodeExec, CapabilityNodeRestart, CapabilityRollingRestart, CapabilityLogs)
}

func (d K3DDriver) Validate(environment spec.EnvironmentSpec) error {
	return rejectUnknownOptions(environment.Options, "clusterNamePrefix", "createArguments", "consolePortBase")
}

func (d K3DDriver) Preflight(ctx context.Context, _ spec.EnvironmentSpec) error {
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
	environment := Environment{Name: clusterName, Driver: "k3d", Namespace: namespace, Context: "k3d-" + clusterName, Metadata: map[string]string{"nodeCount": strconv.Itoa(scenario.Cluster.Nodes), "consolePortBase": strconv.Itoa(intOption(environmentSpec.Options, "consolePortBase", DefaultConsolePortBase))}}
	if err := d.importLocalImageIfPresent(ctx, environment, scenario.Cluster.Image); err != nil {
		_ = d.Delete(context.Background(), environment)
		return Environment{}, err
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
	_, err := runner.Run(ctx, "kubectl", "--context", environment.Context, "-n", environment.Namespace, "wait", "pods", "-l", "app=myceld", "--for=condition=Ready", timeoutArg)
	return err
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

func (d K3DDriver) CaptureState(ctx context.Context, environment Environment, sink *artifacts.Sink) error {
	if _, err := sink.WriteJSON("environment/state.json", environment); err != nil {
		return err
	}
	runner := d.runner()
	captures := []struct {
		name string
		args []string
	}{
		{name: "environment/kubernetes-resources.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "get", "all,pvc", "-o", "wide"}},
		{name: "environment/pods-describe.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "describe", "pods"}},
		{name: "environment/pod-logs.txt", args: []string{"--context", environment.Context, "-n", environment.Namespace, "logs", "statefulset/myceld", "--all-containers=true", "--tail=200"}},
	}
	for _, capture := range captures {
		result, err := runner.Run(ctx, "kubectl", capture.args...)
		content := formatCommandResult(result, err)
		if _, writeErr := sink.WriteText(capture.name, content); writeErr != nil {
			return writeErr
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
	return NewCapabilitySet(CapabilityArtifacts, CapabilityPerNodeEndpoints, CapabilityNodeRestart, CapabilityRollingRestart, CapabilityLogs, CapabilityObjectStoreFixture)
}

func (d ComposeDriver) Validate(environment spec.EnvironmentSpec) error {
	return rejectUnknownOptions(environment.Options, "composeFile", "composeFiles", "projectName", "projectNamePrefix", "serviceNames", "grpcPorts", "objectStoreFixture")
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
	projectName := stringOption(environmentSpec.Options, "projectName", "")
	if projectName == "" {
		projectName = composeProjectName(scenario.Metadata.Name, stringOption(environmentSpec.Options, "projectNamePrefix", "mycel-lab"))
	}
	metadata := map[string]string{"composeFile": composeFiles[0], "composeFiles": strings.Join(composeFiles, string(os.PathListSeparator)), "projectName": projectName, "nodeCount": strconv.Itoa(scenario.Cluster.Nodes)}
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
	args := []string{"exec", service}
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
	for _, service := range composeServiceNames(environment) {
		result, err := d.compose(ctx, environment, "logs", "--no-color", "--tail=200", service)
		if _, writeErr := sink.WriteText(filepath.Join("environment", "logs", service+".log"), formatCommandResult(result, err)); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func (d ComposeDriver) Delete(ctx context.Context, environment Environment) error {
	_, err := d.compose(ctx, environment, "down", "--volumes", "--remove-orphans")
	return err
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
	return d.runner().Run(ctx, "docker", append([]string{"compose"}, base...)...)
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

func k3dClusterName(name, prefix string) string {
	base := sanitizeName(name)
	if base == "" {
		base = "scenario"
	}
	if prefix == "" {
		prefix = "mlab"
	}
	if len(base) > 14 {
		base = strings.Trim(base[:14], "-")
	}
	stamp := time.Now().UTC().Format("060102150405")
	return sanitizeName(prefix) + "-" + base + "-" + stamp
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
