package env

import (
	"context"
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

type ProviderEmulatorController interface {
	ProviderEmulatorGate(context.Context, Environment, string, string) (ExecResult, error)
}

func gateArguments(environment Environment, name, action string) ([]string, error) {
	if environment.Metadata["providerEmulator"] != "true" || !provider.ValidEmulatorGate(name) || action != "get" && action != "release" {
		return nil, errors.New("provider gate requires a configured emulator and a valid finite control")
	}
	return []string{"/mycel-provider-emulator", "gate", "--name", name, "--action", action}, nil
}

func (d ComposeDriver) ProviderEmulatorGate(ctx context.Context, environment Environment, name, action string) (ExecResult, error) {
	args, err := gateArguments(environment, name, action)
	if err != nil {
		return ExecResult{}, err
	}
	return d.compose(ctx, environment, append([]string{"exec", "-T", "mycel-provider-emulator"}, args...)...)
}

func (d K3DDriver) ProviderEmulatorGate(ctx context.Context, environment Environment, name, action string) (ExecResult, error) {
	args, err := gateArguments(environment, name, action)
	if err != nil {
		return ExecResult{}, err
	}
	return d.runner().Run(ctx, "kubectl", append([]string{"--context", environment.Context, "-n", environment.Namespace, "exec", "deployment/mycel-provider-emulator", "--"}, args...)...)
}
