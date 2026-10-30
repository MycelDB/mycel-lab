package releasebundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"
)

type ImageInspector interface {
	Inspect(context.Context, ImageIdentity) (ResolvedImage, error)
}

type ToolRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

func CurrentHarnessIdentity(bundle *Bundle) HarnessIdentity {
	return CurrentHarnessIdentityContext(context.Background(), bundle)
}

func CurrentHarnessIdentityContext(ctx context.Context, bundle *Bundle) HarnessIdentity {
	identity := HarnessIdentity{Modified: true}
	if executable, err := os.Executable(); err == nil {
		identity.ExecutableSHA256, _ = fileSHA256(executable)
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		identity.GoVersion = info.GoVersion
		identity.ModulePath = info.Main.Path
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				identity.Revision = strings.TrimSpace(setting.Value)
			case "vcs.modified":
				identity.Modified = setting.Value != "false"
			}
		}
	}
	if bundle == nil {
		return identity
	}
	sourceRoot, ok := bundle.ResolvedSource(bundle.Harness.Source)
	if !ok {
		return identity
	}
	if output, err := exec.CommandContext(ctx, "git", "-C", sourceRoot, "rev-parse", "HEAD^{tree}").Output(); err == nil {
		identity.Tree = strings.TrimSpace(string(output))
	}
	if output, err := exec.CommandContext(ctx, "git", "-C", sourceRoot, "rev-parse", "HEAD").Output(); err == nil && identity.Revision == "" {
		identity.Revision = strings.TrimSpace(string(output))
	}
	if output, err := exec.CommandContext(ctx, "git", "-C", sourceRoot, "status", "--porcelain").Output(); err != nil || strings.TrimSpace(string(output)) != "" {
		identity.Modified = true
	}
	return identity
}

type localToolRunner struct{}

func (localToolRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	return command.CombinedOutput()
}

func (b *Bundle) Preflight(ctx context.Context, harness HarnessIdentity, inspector ImageInspector, runner ToolRunner) (PreflightRecord, error) {
	if b.preflight == nil {
		b.preflight = &preflightState{}
	}
	state := b.preflight
	state.once.Do(func() {
		record := PreflightRecord{
			APIVersion:  APIVersion,
			Kind:        "ReleaseExecutionBundlePreflight",
			BundleName:  b.Metadata.Name,
			CandidateID: b.Metadata.CandidateID,
			BundlePath:  b.path,
			SourceRoot:  b.sourcePath,
			CheckedAt:   time.Now().UTC(),
		}
		record.BundleSHA256 = b.bundleSHA256
		if raw, err := os.ReadFile(b.path); err != nil {
			state.err = fmt.Errorf("read release execution bundle: %w", err)
			record.Findings = append(record.Findings, ValidationFinding{Stage: "bundle", Subject: b.path, Message: state.err.Error()})
			state.result = record
			return
		} else {
			digest := sha256.Sum256(raw)
			current := hex.EncodeToString(digest[:])
			if current != b.bundleSHA256 {
				state.err = ValidationError{Findings: []ValidationFinding{{Stage: "bundle", Subject: b.path, Message: fmt.Sprintf("bundle bytes changed after load: loaded %s, current %s", b.bundleSHA256, current)}}}
				record.Findings = append(record.Findings, state.err.(ValidationError).Findings...)
				state.result = record
				return
			}
		}
		var findings []ValidationFinding
		add := func(stage, subject, message string) {
			findings = append(findings, ValidationFinding{Stage: stage, Subject: subject, Message: message})
		}
		if harness.ExecutableSHA256 != b.Harness.ExecutableSHA256 {
			add("harness", "executableSHA256", fmt.Sprintf("resolved %q, bundle requires %q", harness.ExecutableSHA256, b.Harness.ExecutableSHA256))
		}
		if harness.Revision != b.Harness.Revision {
			add("harness", "revision", fmt.Sprintf("resolved %q, bundle requires %q", harness.Revision, b.Harness.Revision))
		}
		if harness.ModulePath != b.Harness.ModulePath {
			add("harness", "modulePath", fmt.Sprintf("resolved %q, bundle requires %q", harness.ModulePath, b.Harness.ModulePath))
		}
		if harness.GoVersion != b.Harness.GoVersion {
			add("harness", "goVersion", fmt.Sprintf("resolved %q, bundle requires %q", harness.GoVersion, b.Harness.GoVersion))
		}
		if harness.Modified || b.Harness.Modified {
			add("harness", "modified", "modified harness/source identities are forbidden")
		}
		if runner == nil {
			runner = localToolRunner{}
		}
		toolPaths := validateTools(ctx, b.Tools, runner, add)
		if toolBin, toolErr := materializeToolBin(toolPaths, b.Tools); toolErr != nil {
			add("tool", "runtime-path", toolErr.Error())
		} else {
			state.toolBin = toolBin
		}
		gitPath := toolPaths["git"]

		for _, source := range b.Sources {
			path, _ := b.ResolvedSource(source.Name)
			revision, revisionErr := gitOutput(ctx, gitPath, path, "rev-parse", "HEAD")
			tree, treeErr := gitOutput(ctx, gitPath, path, "rev-parse", "HEAD^{tree}")
			status, statusErr := gitOutput(ctx, gitPath, path, "status", "--porcelain")
			if revisionErr != nil || revision != source.Revision {
				add("source", source.Name, fmt.Sprintf("revision mismatch: resolved %q, bundle requires %q", revision, source.Revision))
			}
			if treeErr != nil || tree != source.Tree {
				add("source", source.Name, fmt.Sprintf("tree mismatch: resolved %q, bundle requires %q", tree, source.Tree))
			}
			if statusErr != nil || status != "" || source.Dirty {
				add("source", source.Name, "source tree is dirty or cleanliness could not be verified")
			}
		}
		for _, file := range b.Files {
			sourceRoot, _ := b.ResolvedSource(file.Source)
			path, pathErr := resolveBoundFile(sourceRoot, file.Path)
			if pathErr != nil {
				add("file", file.Source+":"+file.Path, pathErr.Error())
				continue
			}
			actual, err := fileSHA256(path)
			if err != nil {
				add("file", file.Path, err.Error())
				continue
			}
			if actual != file.SHA256 {
				add("file", file.Path, fmt.Sprintf("SHA-256 mismatch: resolved %s, bundle requires %s", actual, file.SHA256))
			}
		}
		if root, files, snapshotErr := b.materializeBoundFiles(); snapshotErr != nil {
			add("file", "runtime-snapshots", snapshotErr.Error())
		} else {
			state.boundFileRoot, state.boundFiles = root, files
		}
		if inspector == nil {
			inspector = NewDockerInspectorWithToolPaths(nil, toolPaths["docker"], toolPaths["docker-buildx"])
		}
		images := []ImageIdentity{b.Images.Daemon}
		if b.Images.ProviderEmulator != nil {
			images = append(images, *b.Images.ProviderEmulator)
		}
		images = append(images, b.Images.Helpers...)
		for _, image := range images {
			if variable := image.RuntimeEnvironmentVariable; variable != "" {
				value, exists := os.LookupEnv(variable)
				if !exists || strings.TrimSpace(value) != image.Reference {
					add("image", image.Name, fmt.Sprintf("runtime environment %s resolves %q, bundle requires %q", variable, value, image.Reference))
					continue
				}
			}
			resolved, err := inspector.Inspect(ctx, image)
			if err != nil {
				add("image", image.Name, err.Error())
				continue
			}
			if validation := validateResolvedImage(image, resolved); validation != nil {
				for _, finding := range validation {
					add("image", image.Name, finding)
				}
			}
			record.ResolvedImages = append(record.ResolvedImages, resolved)
		}
		record.Findings = findings
		record.OK = len(findings) == 0
		state.result = record
		if len(findings) > 0 {
			state.err = ValidationError{Findings: findings}
		}
	})
	return state.result, state.err
}

func (b *Bundle) VerifyBoundFile(path, role string) ([]byte, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve bound %s file: %w", role, err)
	}
	for _, file := range b.Files {
		if file.Role != role {
			continue
		}
		sourceRoot, _ := b.ResolvedSource(file.Source)
		originalPath, resolveErr := resolveBoundFile(sourceRoot, file.Path)
		snapshotPath, snapshotErr := b.boundFilePath(file.Source, file.Path)
		if resolveErr != nil || snapshotErr != nil || (originalPath != canonical && snapshotPath != canonical) {
			continue
		}
		raw, readErr := os.ReadFile(snapshotPath)
		if readErr != nil {
			return nil, readErr
		}
		digest := sha256.Sum256(raw)
		if actual := hex.EncodeToString(digest[:]); actual != file.SHA256 {
			return nil, fmt.Errorf("bound %s file SHA-256 mismatch: resolved %s, bundle requires %s", role, actual, file.SHA256)
		}
		return raw, nil
	}
	return nil, fmt.Errorf("%s file %q is not bound by the release execution bundle", role, path)
}

func (b *Bundle) ScenarioNamesForSuite(path string) ([]string, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, binding := range b.Scenarios {
		if binding.SuitePath == "" {
			continue
		}
		sourceRoot, _ := b.ResolvedSource(binding.Source)
		originalPath, resolveErr := resolveBoundFile(sourceRoot, binding.SuitePath)
		snapshotPath, snapshotErr := b.SuiteFilePath(binding)
		if resolveErr == nil && snapshotErr == nil && (originalPath == canonical || snapshotPath == canonical) {
			names = append(names, binding.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("suite %q has no scenario bindings", path)
	}
	return names, nil
}

func (b *Bundle) ValidateScenario(name string, scenarioPath string, suitePath string, resolvedScenarioSHA256 string, daemonImage string, providerEmulatorImage string, requiredTools []string) error {
	binding, ok := b.Binding(name)
	if !ok {
		return ValidationError{Findings: []ValidationFinding{{Stage: "scenario", Subject: name, Message: "is not bound by the release execution bundle"}}}
	}
	var findings []ValidationFinding
	canonical := func(path string) string {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return filepath.Clean(path)
		}
		if resolved, resolveErr := filepath.EvalSymlinks(absolute); resolveErr == nil {
			return resolved
		}
		return filepath.Clean(absolute)
	}
	expectedScenario, scenarioErr := b.ScenarioFilePath(binding)
	if scenarioErr != nil || canonical(scenarioPath) != canonical(expectedScenario) {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".scenarioPath", Message: fmt.Sprintf("resolved %q, bundle requires its bound snapshot", scenarioPath)})
	}
	if binding.SuitePath != "" {
		expectedSuite, suiteErr := b.SuiteFilePath(binding)
		if suiteErr != nil || canonical(suitePath) != canonical(expectedSuite) {
			findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".suitePath", Message: fmt.Sprintf("resolved %q, bundle requires its bound snapshot", suitePath)})
		}
	}
	if binding.SuitePath == "" && strings.TrimSpace(suitePath) != "" {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".suitePath", Message: "runtime selected an unbound suite"})
	}
	if resolvedScenarioSHA256 != binding.ResolvedScenarioSHA256 {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".resolvedScenarioSHA256", Message: fmt.Sprintf("resolved %q, bundle requires %q", resolvedScenarioSHA256, binding.ResolvedScenarioSHA256)})
	}
	if strings.TrimSpace(daemonImage) != binding.DaemonImage {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".daemonImage", Message: fmt.Sprintf("resolved %q, bundle requires %q", daemonImage, binding.DaemonImage)})
	}
	if strings.TrimSpace(providerEmulatorImage) != binding.ProviderEmulatorImage {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".providerEmulatorImage", Message: fmt.Sprintf("resolved %q, bundle requires %q", providerEmulatorImage, binding.ProviderEmulatorImage)})
	}
	expected := strings.Join(binding.RequiredTools, "\x00")
	actual := strings.Join(requiredTools, "\x00")
	if actual != expected {
		findings = append(findings, ValidationFinding{Stage: "scenario", Subject: name + ".requiredTools", Message: fmt.Sprintf("resolved %v, bundle requires %v", requiredTools, binding.RequiredTools)})
	}
	if len(findings) > 0 {
		return ValidationError{Findings: findings}
	}
	return nil
}

func validateResolvedImage(expected ImageIdentity, actual ResolvedImage) []string {
	var findings []string
	if actual.Name != expected.Name {
		findings = append(findings, fmt.Sprintf("name mismatch: resolved %q, bundle requires %q", actual.Name, expected.Name))
	}
	if actual.Reference != expected.Reference {
		findings = append(findings, fmt.Sprintf("reference mismatch: resolved %q, bundle requires %q", actual.Reference, expected.Reference))
	}
	if expected.Local {
		if actual.ImageID != expected.ImageID {
			findings = append(findings, fmt.Sprintf("image ID mismatch: resolved %q, bundle requires %q", actual.ImageID, expected.ImageID))
		}
	} else if actual.IndexDigest != expected.IndexDigest {
		findings = append(findings, fmt.Sprintf("index digest mismatch: resolved %q, bundle requires %q", actual.IndexDigest, expected.IndexDigest))
	}
	actualPlatforms := map[string]ResolvedPlatform{}
	for _, platform := range actual.Platforms {
		key := platform.OS + "/" + platform.Architecture
		if _, duplicate := actualPlatforms[key]; duplicate {
			findings = append(findings, "duplicate resolved platform manifest: "+key)
			continue
		}
		actualPlatforms[key] = platform
	}
	for _, platform := range expected.Platforms {
		key := platform.OS + "/" + platform.Architecture
		resolved, ok := actualPlatforms[key]
		if !ok {
			findings = append(findings, "required platform is missing: "+key)
			continue
		}
		if !expected.Local && resolved.ManifestDigest != platform.ManifestDigest {
			findings = append(findings, fmt.Sprintf("%s manifest mismatch: resolved %q, bundle requires %q", key, resolved.ManifestDigest, platform.ManifestDigest))
		}
		if resolved.ConfigDigest != platform.ConfigDigest {
			findings = append(findings, fmt.Sprintf("%s config mismatch: resolved %q, bundle requires %q", key, resolved.ConfigDigest, platform.ConfigDigest))
		}
		labels := resolved.Labels
		if labels["org.opencontainers.image.source"] != expected.Provenance.Source {
			findings = append(findings, fmt.Sprintf("%s source provenance mismatch", key))
		}
		if labels["org.opencontainers.image.revision"] != expected.Provenance.Revision {
			findings = append(findings, fmt.Sprintf("%s revision provenance mismatch", key))
		}
		if labels["io.myceldb.source.dirty"] != "false" {
			findings = append(findings, fmt.Sprintf("%s dirty provenance is not false", key))
		}
		if labels["io.myceldb.build.context-digest"] != expected.Provenance.BuildContextSHA256 {
			findings = append(findings, fmt.Sprintf("%s build-context provenance mismatch", key))
		}
		if labels["io.myceldb.build.command"] != expected.Provenance.BuildMethod {
			findings = append(findings, fmt.Sprintf("%s build-method provenance mismatch", key))
		}
		delete(actualPlatforms, key)
	}
	if len(actualPlatforms) > 0 {
		keys := make([]string, 0, len(actualPlatforms))
		for key := range actualPlatforms {
			keys = append(keys, key)
		}
		findings = append(findings, "unexpected platform manifests: "+strings.Join(keys, ", "))
	}
	expectedSide := append([]string(nil), expected.SideManifests...)
	actualSide := append([]string(nil), actual.SideManifests...)
	sort.Strings(expectedSide)
	sort.Strings(actualSide)
	if strings.Join(actualSide, "\x00") != strings.Join(expectedSide, "\x00") {
		findings = append(findings, fmt.Sprintf("side manifest mismatch: resolved %v, bundle requires %v", actualSide, expectedSide))
	}
	return findings
}

func materializeToolBin(paths map[string]string, tools []ToolIdentity) (string, error) {
	root, err := os.MkdirTemp("", "mycel-lab-release-tools-")
	if err != nil {
		return "", err
	}
	expected := make(map[string]string, len(tools))
	for _, tool := range tools {
		expected[tool.Name] = tool.ExecutableSHA256
	}
	for name, target := range paths {
		if filepath.Base(name) != name {
			_ = os.RemoveAll(root)
			return "", fmt.Errorf("invalid tool activation name %q", name)
		}
		raw, readErr := os.ReadFile(target)
		if readErr != nil {
			_ = os.RemoveAll(root)
			return "", readErr
		}
		digest := sha256.Sum256(raw)
		if actual := hex.EncodeToString(digest[:]); actual != expected[name] {
			_ = os.RemoveAll(root)
			return "", fmt.Errorf("tool %s changed while snapshotting", name)
		}
		if writeErr := os.WriteFile(filepath.Join(root, name), raw, 0o500); writeErr != nil {
			_ = os.RemoveAll(root)
			return "", writeErr
		}
	}
	if err := os.Chmod(root, 0o500); err != nil {
		_ = os.RemoveAll(root)
		return "", err
	}
	return root, nil
}

func (b *Bundle) materializeBoundFiles() (string, map[string]string, error) {
	root, err := os.MkdirTemp("", "mycel-lab-release-files-")
	if err != nil {
		return "", nil, err
	}
	files := make(map[string]string, len(b.Files))
	for _, file := range b.Files {
		sourceRoot, _ := b.ResolvedSource(file.Source)
		sourcePath, resolveErr := resolveBoundFile(sourceRoot, file.Path)
		if resolveErr != nil {
			_ = os.RemoveAll(root)
			return "", nil, resolveErr
		}
		raw, readErr := os.ReadFile(sourcePath)
		if readErr != nil {
			_ = os.RemoveAll(root)
			return "", nil, readErr
		}
		digest := sha256.Sum256(raw)
		if actual := hex.EncodeToString(digest[:]); actual != file.SHA256 {
			_ = os.RemoveAll(root)
			return "", nil, fmt.Errorf("bound file %s changed while snapshotting", file.Path)
		}
		destination := filepath.Join(root, file.Source, filepath.Clean(file.Path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			_ = os.RemoveAll(root)
			return "", nil, err
		}
		if err := os.WriteFile(destination, raw, 0o400); err != nil {
			_ = os.RemoveAll(root)
			return "", nil, err
		}
		files[file.Source+":"+filepath.Clean(file.Path)] = destination
	}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return os.Chmod(path, 0o500)
		}
		return nil
	}); err != nil {
		_ = os.RemoveAll(root)
		return "", nil, err
	}
	return root, files, nil
}

func validateTools(ctx context.Context, tools []ToolIdentity, runner ToolRunner, add func(string, string, string)) map[string]string {
	paths := make(map[string]string, len(tools))
	for _, tool := range tools {
		path, err := exec.LookPath(tool.Path)
		if err != nil {
			add("tool", tool.Name, fmt.Sprintf("resolve %s: %v", tool.Path, err))
			continue
		}
		path, err = filepath.EvalSymlinks(path)
		if err != nil {
			add("tool", tool.Name, fmt.Sprintf("resolve executable symlink: %v", err))
			continue
		}
		actual, err := fileSHA256(path)
		if err != nil {
			add("tool", tool.Name, err.Error())
			continue
		}
		if actual != tool.ExecutableSHA256 {
			add("tool", tool.Name, fmt.Sprintf("executable SHA-256 mismatch: resolved %s, bundle requires %s", actual, tool.ExecutableSHA256))
			continue
		}
		output, err := runner.Run(ctx, path, tool.VersionArguments...)
		if err != nil {
			add("tool", tool.Name, fmt.Sprintf("version command failed: %v", err))
			continue
		}
		digest := sha256.Sum256(bytes.TrimSpace(output))
		if versionDigest := hex.EncodeToString(digest[:]); versionDigest != tool.VersionOutputSHA256 {
			add("tool", tool.Name, fmt.Sprintf("version output SHA-256 mismatch: resolved %s, bundle requires %s", versionDigest, tool.VersionOutputSHA256))
			continue
		}
		paths[tool.Name] = path
	}
	return paths
}

func gitOutput(ctx context.Context, gitPath, root string, args ...string) (string, error) {
	if gitPath == "" {
		return "", fmt.Errorf("verified git executable is unavailable")
	}
	commandArgs := append([]string{"-C", root}, args...)
	output, err := exec.CommandContext(ctx, gitPath, commandArgs...).Output()
	return strings.TrimSpace(string(output)), err
}

func resolveBoundFile(root, relative string) (string, error) {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve source root %s: %w", root, err)
	}
	resolvedPath, err := filepath.EvalSymlinks(filepath.Join(root, filepath.Clean(relative)))
	if err != nil {
		return "", fmt.Errorf("resolve bound file %s: %w", relative, err)
	}
	rel, err := filepath.Rel(resolvedRoot, resolvedPath)
	if err != nil || escapesRoot(rel) {
		return "", fmt.Errorf("bound file %s resolves outside source root", relative)
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("bound file %s is not a regular file", relative)
	}
	return resolvedPath, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
