package env

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type Environment struct {
	Name      string `json:"name"`
	Driver    string `json:"driver"`
	Namespace string `json:"namespace"`
	Context   string `json:"context,omitempty"`
}

type EnvironmentDriver interface {
	Preflight(ctx context.Context) error
	Create(ctx context.Context, scenario spec.ResolvedScenario) (Environment, error)
	Delete(ctx context.Context, environment Environment) error
	CaptureState(ctx context.Context, environment Environment, sink *artifacts.Sink) error
}

type Options struct {
	ConfirmDestructive bool
	DryRun             bool
}

func RequireDestructiveConfirmation(opts Options) error {
	if opts.DryRun || opts.ConfirmDestructive {
		return nil
	}
	return errors.New("destructive environment operations require --confirm-destructive")
}

type DryRunDriver struct{}

func (DryRunDriver) Preflight(context.Context) error { return nil }

func (DryRunDriver) Create(_ context.Context, scenario spec.ResolvedScenario) (Environment, error) {
	name := scenario.Metadata.Name
	if name == "" {
		name = "mycel-lab"
	}
	namespace := scenario.Environment.Namespace
	if namespace == "" {
		namespace = "mycel-lab"
	}
	return Environment{Name: name, Driver: "dry-run", Namespace: namespace, Context: "dry-run"}, nil
}

func (DryRunDriver) Delete(context.Context, Environment) error { return nil }

func (DryRunDriver) CaptureState(_ context.Context, environment Environment, sink *artifacts.Sink) error {
	_, err := sink.WriteJSON("environment/state.json", environment)
	return err
}

type K3DDriver struct {
	Confirmed       bool
	Runner          executil.Runner
	WaitTimeout     time.Duration
	CreateArguments []string
}

func (d K3DDriver) Preflight(ctx context.Context) error {
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

func (d K3DDriver) Create(ctx context.Context, scenario spec.ResolvedScenario) (Environment, error) {
	if !d.Confirmed {
		return Environment{}, RequireDestructiveConfirmation(Options{})
	}
	runner := d.runner()
	clusterName := k3dClusterName(scenario.Metadata.Name)
	namespace := scenario.Environment.Namespace
	if namespace == "" {
		namespace = "mycel-lab"
	}
	args := []string{"cluster", "create", clusterName, "--wait"}
	args = append(args, d.CreateArguments...)
	if _, err := runner.Run(ctx, "k3d", args...); err != nil {
		return Environment{}, err
	}
	environment := Environment{Name: clusterName, Driver: "k3d", Namespace: namespace, Context: "k3d-" + clusterName}
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
	if err := d.waitReady(ctx, environment); err != nil {
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

func (d K3DDriver) waitReady(ctx context.Context, environment Environment) error {
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

func (d K3DDriver) runner() executil.Runner {
	if d.Runner != nil {
		return d.Runner
	}
	return executil.LocalRunner{}
}

func k3dClusterName(name string) string {
	base := sanitizeName(name)
	if base == "" {
		base = "scenario"
	}
	if len(base) > 14 {
		base = strings.Trim(base[:14], "-")
	}
	stamp := time.Now().UTC().Format("060102150405")
	return "mlab-" + base + "-" + stamp
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
	b.WriteString("$ ")
	b.WriteString(result.Command)
	b.WriteString("\n")
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
		b.WriteString("\n")
	}
	return b.String()
}
