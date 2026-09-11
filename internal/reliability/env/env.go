package env

import (
	"context"
	"errors"
	"fmt"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
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
	Confirmed bool
}

func (d K3DDriver) Preflight(context.Context) error {
	if !d.Confirmed {
		return RequireDestructiveConfirmation(Options{})
	}
	return nil
}

func (d K3DDriver) Create(context.Context, spec.ResolvedScenario) (Environment, error) {
	if !d.Confirmed {
		return Environment{}, RequireDestructiveConfirmation(Options{})
	}
	return Environment{}, fmt.Errorf("k3d environment creation is not implemented until RH5 integration wiring")
}

func (d K3DDriver) Delete(context.Context, Environment) error { return nil }

func (d K3DDriver) CaptureState(_ context.Context, environment Environment, sink *artifacts.Sink) error {
	_, err := sink.WriteJSON("environment/state.json", environment)
	return err
}
