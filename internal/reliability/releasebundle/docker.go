package releasebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type DockerInspector struct {
	runner     CommandRunner
	dockerPath string
	buildxPath string
}

func NewDockerInspector(runner CommandRunner) *DockerInspector {
	if runner == nil {
		runner = commandRunner{}
	}
	return &DockerInspector{runner: runner, dockerPath: "docker"}
}

func NewDockerInspectorWithToolPaths(runner CommandRunner, dockerPath, buildxPath string) *DockerInspector {
	inspector := NewDockerInspector(runner)
	inspector.dockerPath = dockerPath
	inspector.buildxPath = buildxPath
	return inspector
}

type commandRunner struct{}

func (commandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func (d *DockerInspector) Inspect(ctx context.Context, image ImageIdentity) (ResolvedImage, error) {
	if image.Local {
		return d.inspectLocal(ctx, image)
	}
	return d.inspectRegistry(ctx, image)
}

type dockerImageInspect struct {
	ID           string `json:"Id"`
	Architecture string `json:"Architecture"`
	OS           string `json:"Os"`
	Config       struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func (d *DockerInspector) inspectLocal(ctx context.Context, image ImageIdentity) (ResolvedImage, error) {
	output, err := d.runner.Run(ctx, d.dockerPath, "image", "inspect", image.Reference)
	if err != nil {
		return ResolvedImage{}, fmt.Errorf("inspect local image %s: %w", image.Reference, err)
	}
	var inspected []dockerImageInspect
	if err := json.Unmarshal(output, &inspected); err != nil || len(inspected) != 1 {
		if err == nil {
			err = fmt.Errorf("expected one image, got %d", len(inspected))
		}
		return ResolvedImage{}, fmt.Errorf("decode docker image inspect for %s: %w", image.Reference, err)
	}
	item := inspected[0]
	return ResolvedImage{
		Name: image.Name, Reference: image.Reference, ImageID: item.ID,
		Platforms: []ResolvedPlatform{{OS: item.OS, Architecture: item.Architecture, ConfigDigest: item.ID, Labels: item.Config.Labels}},
	}, nil
}

type ociIndex struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	Config        struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Manifests []struct {
		Digest   string `json:"digest"`
		Platform struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
		} `json:"platform"`
		Annotations map[string]string `json:"annotations"`
	} `json:"manifests"`
}

type ociImageConfig struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Config       struct {
		Labels map[string]string `json:"Labels"`
	} `json:"config"`
}

func (d *DockerInspector) inspectRegistry(ctx context.Context, image ImageIdentity) (ResolvedImage, error) {
	configDir, err := os.MkdirTemp("", "mycel-lab-anonymous-docker-")
	if err != nil {
		return ResolvedImage{}, err
	}
	defer os.RemoveAll(configDir)
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte("{}\n"), 0o600); err != nil {
		return ResolvedImage{}, err
	}
	base := []string{"--config", configDir, "buildx", "imagetools", "inspect"}
	if d.buildxPath != "" {
		output, verifyErr := d.runner.Run(ctx, d.dockerPath, "info", "--format", "{{range .ClientInfo.Plugins}}{{if eq .Name \"buildx\"}}{{.Path}}{{end}}{{end}}")
		if verifyErr != nil {
			return ResolvedImage{}, fmt.Errorf("resolve selected buildx plugin: %w", verifyErr)
		}
		selected, resolveErr := filepath.EvalSymlinks(strings.TrimSpace(string(output)))
		if resolveErr != nil || selected != d.buildxPath {
			return ResolvedImage{}, fmt.Errorf("selected buildx plugin %q does not equal bound executable %q", selected, d.buildxPath)
		}
	}
	raw, err := d.runner.Run(ctx, d.dockerPath, append(base, "--raw", image.Reference)...)
	if err != nil {
		return ResolvedImage{}, fmt.Errorf("anonymous pullability/manifest inspection failed for %s: %w", image.Reference, err)
	}
	digest := sha256.Sum256(raw)
	resolvedDigest := "sha256:" + hex.EncodeToString(digest[:])
	trimmed := strings.TrimSpace(string(raw))
	if trimmedDigest := sha256.Sum256([]byte(trimmed)); "sha256:"+hex.EncodeToString(trimmedDigest[:]) == image.IndexDigest {
		resolvedDigest = image.IndexDigest
	}
	resolved := ResolvedImage{Name: image.Name, Reference: image.Reference, IndexDigest: resolvedDigest}
	var index ociIndex
	if err := json.Unmarshal(raw, &index); err != nil {
		return ResolvedImage{}, fmt.Errorf("decode OCI manifest for %s: %w", image.Reference, err)
	}
	if len(index.Manifests) == 0 {
		if len(image.Platforms) != 1 {
			return ResolvedImage{}, fmt.Errorf("single-platform manifest for %s cannot satisfy %d bound platforms", image.Reference, len(image.Platforms))
		}
		platform := image.Platforms[0]
		labels, osName, arch, err := d.inspectRemoteConfig(ctx, configDir, image.Reference)
		if err != nil {
			return ResolvedImage{}, err
		}
		if osName == "" {
			osName = platform.OS
		}
		if arch == "" {
			arch = platform.Architecture
		}
		resolved.Platforms = append(resolved.Platforms, ResolvedPlatform{OS: osName, Architecture: arch, ManifestDigest: image.IndexDigest, ConfigDigest: index.Config.Digest, Labels: labels})
		return resolved, nil
	}
	for _, manifest := range index.Manifests {
		platform := ResolvedPlatform{OS: manifest.Platform.OS, Architecture: manifest.Platform.Architecture, ManifestDigest: manifest.Digest}
		if platform.OS == "" {
			platform.OS = "unknown"
		}
		if platform.Architecture == "" {
			platform.Architecture = "unknown"
		}
		if platform.OS == "unknown" || platform.Architecture == "unknown" {
			resolved.SideManifests = append(resolved.SideManifests, manifest.Digest)
			continue
		}
		manifestReference := strings.SplitN(image.Reference, "@", 2)[0] + "@" + manifest.Digest
		componentRaw, err := d.runner.Run(ctx, d.dockerPath, append(base, "--raw", manifestReference)...)
		if err != nil {
			return ResolvedImage{}, fmt.Errorf("inspect platform manifest %s: %w", manifestReference, err)
		}
		var component ociIndex
		if err := json.Unmarshal(componentRaw, &component); err != nil {
			return ResolvedImage{}, fmt.Errorf("decode platform manifest %s: %w", manifestReference, err)
		}
		platform.ConfigDigest = component.Config.Digest
		labels, osName, arch, err := d.inspectRemoteConfig(ctx, configDir, manifestReference)
		if err != nil {
			return ResolvedImage{}, err
		}
		if osName != "" {
			platform.OS = osName
		}
		if arch != "" {
			platform.Architecture = arch
		}
		platform.Labels = labels
		resolved.Platforms = append(resolved.Platforms, platform)
	}
	resolved.Platforms = sortedPlatforms(resolved.Platforms)
	sort.Strings(resolved.SideManifests)
	return resolved, nil
}

func (d *DockerInspector) inspectRemoteConfig(ctx context.Context, configDir, reference string) (map[string]string, string, string, error) {
	format := "{{json .Image}}"
	output, err := d.runner.Run(ctx, d.dockerPath, "--config", configDir, "buildx", "imagetools", "inspect", reference, "--format", format)
	if err != nil {
		return nil, "", "", fmt.Errorf("inspect remote image config %s: %w", reference, err)
	}
	var config ociImageConfig
	if err := json.Unmarshal(output, &config); err != nil {
		return nil, "", "", fmt.Errorf("decode remote image config %s: %w", reference, err)
	}
	return config.Config.Labels, config.OS, config.Architecture, nil
}
