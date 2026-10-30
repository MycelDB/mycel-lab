package runner

import (
	"context"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/releasebundle"
)

func rejectIndependentReleaseOverrides(opts Options) error {
	var overrides []string
	if opts.EnvironmentOverride != nil {
		overrides = append(overrides, "--environment-*")
	}
	if strings.TrimSpace(opts.ImageOverride) != "" {
		overrides = append(overrides, "--image")
	}
	if strings.TrimSpace(opts.ProviderProfileOverride) != "" {
		overrides = append(overrides, "--provider-profile")
	}
	if opts.KeepEnvironmentOnFailure {
		overrides = append(overrides, "--keep-environment-on-failure")
	}
	if opts.ConsolePortBase != 0 {
		overrides = append(overrides, "--console-port-base")
	}
	if opts.ConsoleEndpoints {
		overrides = append(overrides, "--console-endpoints")
	}
	if len(opts.ProfileDirs) > 0 {
		overrides = append(overrides, "--profile-dir")
	}
	if opts.EnvironmentDriver != nil && opts.releaseImageInspector == nil {
		overrides = append(overrides, "custom environment driver")
	}
	if len(overrides) > 0 {
		return releasebundle.ValidationError{Findings: []releasebundle.ValidationFinding{{Stage: "runtime", Subject: "independent-overrides", Message: "release execution bundles are authoritative; remove " + strings.Join(overrides, ", ")}}}
	}
	return nil
}

func hermeticCandidateSuite(name string) bool {
	switch strings.TrimSpace(name) {
	case "compose-procedure-inference-control-emulator", "k3d-procedure-inference-recovery", "k3d-procedure-inference-backup-restore", "cluster-release-gate", "semantic-live-provider":
		return true
	default:
		return false
	}
}

func hermeticCandidateScenario(name string) bool {
	switch strings.TrimSpace(name) {
	case "compose-procedure-inference-control-emulator", "k3d-procedure-inference-recovery", "k3d-procedure-inference-backup-restore", "compose-cluster-validation", "k3d-cluster-validation", "k3d-system-backup-restore", "compose-semantic-live-provider":
		return true
	default:
		return false
	}
}

func releaseRequiredTools(driver string) []string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "compose":
		return []string{"git", "docker", "docker-buildx", "docker-compose"}
	case "k3d":
		return []string{"git", "docker", "docker-buildx", "docker-compose", "k3d", "kubectl"}
	default:
		return []string{strings.ToLower(strings.TrimSpace(driver))}
	}
}

func cloneStringAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	copy := make(map[string]any, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func currentReleaseHarnessIdentity(ctx context.Context, bundle *releasebundle.Bundle) releasebundle.HarnessIdentity {
	return releasebundle.CurrentHarnessIdentityContext(ctx, bundle)
}
