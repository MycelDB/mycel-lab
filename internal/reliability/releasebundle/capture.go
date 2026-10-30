package releasebundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Capture resolves machine- and source-owned identities in a draft bundle.
// Scenario semantic identities remain caller-owned because they require catalog
// resolution; callers must fill them before ValidateStructure.
func (b *Bundle) Capture(ctx context.Context, inspector ImageInspector, runner ToolRunner) error {
	if inspector == nil {
		inspector = NewDockerInspector(nil)
	}
	if runner == nil {
		runner = localToolRunner{}
	}
	for index := range b.Sources {
		root, ok := b.ResolvedSource(b.Sources[index].Name)
		if !ok {
			return fmt.Errorf("resolve source %q", b.Sources[index].Name)
		}
		revision, err := captureGit(ctx, root, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		tree, err := captureGit(ctx, root, "rev-parse", "HEAD^{tree}")
		if err != nil {
			return err
		}
		status, err := captureGit(ctx, root, "status", "--porcelain")
		if err != nil {
			return err
		}
		if status != "" {
			return fmt.Errorf("source %q is dirty", b.Sources[index].Name)
		}
		b.Sources[index].Revision, b.Sources[index].Tree, b.Sources[index].Dirty = revision, tree, false
	}
	harnessSource := strings.TrimSpace(b.Harness.Source)
	b.Harness = CurrentHarnessIdentityContext(ctx, b)
	b.Harness.Source = harnessSource
	if b.Harness.Source == "" && len(b.Sources) == 1 {
		b.Harness.Source = b.Sources[0].Name
	}
	if source, ok := b.ResolvedSource(b.Harness.Source); ok {
		b.Harness.Revision, _ = captureGit(ctx, source, "rev-parse", "HEAD")
		b.Harness.Tree, _ = captureGit(ctx, source, "rev-parse", "HEAD^{tree}")
	}
	if b.Harness.Modified {
		return errors.New("running harness was built from modified source")
	}

	for index := range b.Files {
		root, ok := b.ResolvedSource(b.Files[index].Source)
		if !ok {
			return fmt.Errorf("file references unknown source %q", b.Files[index].Source)
		}
		path, err := resolveBoundFile(root, b.Files[index].Path)
		if err != nil {
			return err
		}
		b.Files[index].SHA256, err = fileSHA256(path)
		if err != nil {
			return err
		}
	}
	for index := range b.Tools {
		tool := &b.Tools[index]
		if (tool.Name == "docker-buildx" || tool.Name == "docker-compose") && (tool.Path == "" || tool.Path == "auto") {
			pluginName := strings.TrimPrefix(tool.Name, "docker-")
			format := fmt.Sprintf(`{{range .ClientInfo.Plugins}}{{if eq .Name %q}}{{.Path}}{{end}}{{end}}`, pluginName)
			output, err := exec.CommandContext(ctx, "docker", "info", "--format", format).Output()
			if err != nil {
				return fmt.Errorf("resolve %s: %w", tool.Name, err)
			}
			tool.Path = strings.TrimSpace(string(output))
			if tool.Path == "" {
				return fmt.Errorf("resolve %s: Docker did not report the plugin path", tool.Name)
			}
		}
		path, err := exec.LookPath(tool.Path)
		if err != nil {
			return fmt.Errorf("resolve tool %s: %w", tool.Name, err)
		}
		path, err = filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		tool.ExecutableSHA256, err = fileSHA256(path)
		if err != nil {
			return err
		}
		output, err := runner.Run(ctx, path, tool.VersionArguments...)
		if err != nil {
			return fmt.Errorf("capture tool %s version: %w", tool.Name, err)
		}
		digest := sha256.Sum256(bytes.TrimSpace(output))
		tool.VersionOutputSHA256 = hex.EncodeToString(digest[:])
	}
	images := []*ImageIdentity{&b.Images.Daemon}
	if b.Images.ProviderEmulator != nil {
		images = append(images, b.Images.ProviderEmulator)
	}
	for index := range b.Images.Helpers {
		images = append(images, &b.Images.Helpers[index])
	}
	for _, image := range images {
		if !image.Local {
			image.IndexDigest = digestFromReference(image.Reference)
		}
		resolved, err := inspector.Inspect(ctx, *image)
		if err != nil {
			return fmt.Errorf("capture image %s: %w", image.Name, err)
		}
		image.ImageID, image.IndexDigest, image.SideManifests = resolved.ImageID, resolved.IndexDigest, append([]string(nil), resolved.SideManifests...)
		image.Platforms = image.Platforms[:0]
		for _, platform := range resolved.Platforms {
			image.Platforms = append(image.Platforms, PlatformIdentity{OS: platform.OS, Architecture: platform.Architecture, ManifestDigest: platform.ManifestDigest, ConfigDigest: platform.ConfigDigest})
			if image.Provenance.Source == "" {
				image.Provenance.Source = platform.Labels["org.opencontainers.image.source"]
			}
			if image.Provenance.Revision == "" {
				image.Provenance.Revision = platform.Labels["org.opencontainers.image.revision"]
			}
			if image.Provenance.BuildContextSHA256 == "" {
				image.Provenance.BuildContextSHA256 = platform.Labels["io.myceldb.build.context-digest"]
			}
			if image.Provenance.BuildMethod == "" {
				image.Provenance.BuildMethod = platform.Labels["io.myceldb.build.command"]
			}
		}
		image.Provenance.Dirty = false
	}
	for index := range b.Scenarios {
		if b.Scenarios[index].DaemonImage == "" {
			b.Scenarios[index].DaemonImage = b.Images.Daemon.Reference
		}
		if b.Images.ProviderEmulator != nil && b.Scenarios[index].ProviderEmulatorImage == "" {
			b.Scenarios[index].ProviderEmulatorImage = b.Images.ProviderEmulator.Reference
		}
	}
	return nil
}

func captureGit(ctx context.Context, root string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git -C %s %s: %w: %s", root, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func (b *Bundle) Write(path string) error {
	raw, err := yaml.Marshal(b)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}
