package releasebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	testRevision    = "1111111111111111111111111111111111111111"
	testTree        = "2222222222222222222222222222222222222222"
	testIndex       = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testManifest    = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testConfig      = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testArmManifest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	testArmConfig   = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

type staticInspector struct {
	resolved ResolvedImage
	err      error
}

func (s staticInspector) Inspect(context.Context, ImageIdentity) (ResolvedImage, error) {
	return s.resolved, s.err
}

type staticToolRunner struct {
	output []byte
	err    error
}

func (s staticToolRunner) Run(context.Context, string, ...string) ([]byte, error) {
	return s.output, s.err
}

func TestCanonicalSchemaIsValidAndVersionLocked(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "reliability", "release-bundles", "v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	properties, _ := schema["properties"].(map[string]any)
	apiVersion, _ := properties["apiVersion"].(map[string]any)
	if apiVersion["const"] != APIVersion {
		t.Fatalf("schema apiVersion=%v, want %s", apiVersion["const"], APIVersion)
	}
	for _, required := range []string{"sources", "harness", "images", "files", "tools", "scenarios"} {
		if _, ok := properties[required]; !ok {
			t.Fatalf("schema missing %s", required)
		}
	}
}

func TestMaintainedDraftTemplatesStrictDecode(t *testing.T) {
	for _, name := range []string{"cluster-release-gate.draft.yaml.tmpl", "semantic-live-provider.draft.yaml.tmpl"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "reliability", "release-bundles", name))
		if err != nil {
			t.Fatal(err)
		}
		var draft Bundle
		decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
		decoder.KnownFields(true)
		if err := decoder.Decode(&draft); err != nil {
			t.Fatalf("%s strict decode: %v", name, err)
		}
		if len(draft.Scenarios) == 0 {
			t.Fatalf("%s has no scenarios", name)
		}
		for _, scenario := range draft.Scenarios {
			if err := scenario.Finalization.validate(); err != nil {
				t.Fatalf("%s scenario %s finalization: %v", name, scenario.Name, err)
			}
		}
	}
}

func TestLoadRejectsUnknownAndMutableFields(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "bundle.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: "+APIVersion+"\nkind: "+Kind+"\nunknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("Load() error=%v, want strict unknown-field rejection", err)
	}

	bundle := validBundle(t, root)
	helper := bundle.Images.Daemon
	helper.Name = "helper"
	bundle.Images.Helpers = []ImageIdentity{helper}
	writeBundle(t, path, bundle)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "runtimeEnvironmentVariable") {
		t.Fatalf("Load() error=%v, want unbound helper runtime rejection", err)
	}

	bundle.Images.Helpers = nil
	bundle.Images.Daemon.Reference = "docker.io/myceldb/mycel:latest"
	bundle.Images.Daemon.IndexDigest = ""
	writeBundle(t, path, bundle)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "mutable/default") {
		t.Fatalf("Load() error=%v, want mutable image rejection", err)
	}

	bundle.Images.Daemon.Reference = "docker.io/myceldb/mycel@" + testIndex
	bundle.Images.Daemon.IndexDigest = testIndex
	bundle.Scenarios[0].OuterTimeout = MaximumOuterBound + time.Second
	bundle.Scenarios[0].Finalization.ActorStop = MaximumFinalizationBound + time.Second
	writeBundle(t, path, bundle)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "no greater than") {
		t.Fatalf("Load() error=%v, want execution bound rejection", err)
	}
}

func TestValidateStructureRejectsDaemonRevisionNotBoundToSource(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	bundle.Images.Daemon.Provenance.Revision = strings.Repeat("f", 40)
	if err := bundle.ValidateStructure(); err == nil || !strings.Contains(err.Error(), "must equal the provenance evidence source revision") {
		t.Fatalf("ValidateStructure() error=%v", err)
	}
}

func TestPreflightValidatesFilesHarnessToolsAndImages(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedDaemon(bundle.Images.Daemon)
	record, err := loaded.Preflight(context.Background(), bundle.Harness, staticInspector{resolved: resolved}, staticToolRunner{output: []byte("fake tool v1\n")})
	if err != nil {
		t.Fatalf("Preflight() error=%v findings=%+v", err, record.Findings)
	}
	if !record.OK || len(record.ResolvedImages) != 1 || record.BundleSHA256 == "" {
		t.Fatalf("record=%+v", record)
	}
}

func TestPreflightSnapshotsRuntimeFilesAndTools(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loaded.Close() })
	if _, err := loaded.Preflight(context.Background(), bundle.Harness, staticInspector{resolved: resolvedDaemon(bundle.Images.Daemon)}, staticToolRunner{output: []byte("fake tool v1\n")}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "lab", "compose.yml"), []byte("services:\n  replaced: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "lab", "fake-tool"), []byte("#!/bin/sh\necho replaced\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtimePath, err := loaded.RuntimeFilePath(bundle.Scenarios[0].Environment.RuntimeFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "services: {}\n" {
		t.Fatalf("runtime snapshot=%q", raw)
	}
	restore, err := loaded.ActivateVerifiedTools()
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	output, err := exec.Command("docker-compose", "version").CombinedOutput()
	if err != nil {
		t.Fatalf("snapshotted tool: %v: %s", err, output)
	}
	if strings.TrimSpace(string(output)) != "fake tool v1" {
		t.Fatalf("snapshotted tool output=%q", output)
	}
}

func TestPreflightRejectsBundleReplacementAfterLoad(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Metadata.CandidateID = "candidate-2"
	writeBundle(t, path, bundle)
	record, err := loaded.Preflight(context.Background(), bundle.Harness, staticInspector{resolved: resolvedDaemon(bundle.Images.Daemon)}, staticToolRunner{output: []byte("fake tool v1\n")})
	if err == nil || !strings.Contains(err.Error(), "changed after load") || record.OK {
		t.Fatalf("Preflight() record=%+v error=%v", record, err)
	}
}

func TestLoadRejectsSourceSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	bundle := validBundle(t, root)
	if err := os.Rename(filepath.Join(root, "lab"), filepath.Join(external, "lab")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(external, "lab"), filepath.Join(root, "lab")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "outside sourceRoot") {
		t.Fatalf("Load() error=%v, want symlink escape rejection", err)
	}
}

func TestPreflightRejectsEveryIdentityMismatchBeforeProvisioning(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	harness := bundle.Harness
	harness.Revision = "9999999999999999999999999999999999999999"
	resolved := resolvedDaemon(bundle.Images.Daemon)
	resolved.Platforms[0].ConfigDigest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	record, err := loaded.Preflight(context.Background(), harness, staticInspector{resolved: resolved}, staticToolRunner{output: []byte("wrong version")})
	if err == nil {
		t.Fatal("Preflight() error=nil, want fail-closed rejection")
	}
	joined := findingsText(record.Findings)
	for _, expected := range []string{"harness", "revision", "version output", "config mismatch"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("findings %q missing %q", joined, expected)
		}
	}
	if record.OK {
		t.Fatalf("record=%+v, want OK=false", record)
	}
}

func TestValidateScenarioRejectsUnboundSourceAndRuntime(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle(t, root)
	path := filepath.Join(root, "bundle.yaml")
	writeBundle(t, path, bundle)
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	err = loaded.ValidateScenario("scenario-a", filepath.Join(root, "other.yaml"), "", strings.Repeat("3", 64), "docker.io/myceldb/mycel@"+testIndex, "local/emulator:candidate-1", []string{"docker", "k3d"})
	if err == nil {
		t.Fatal("ValidateScenario() error=nil")
	}
	for _, expected := range []string{"scenarioPath", "providerEmulatorImage", "requiredTools"} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("error=%q missing %q", err, expected)
		}
	}
}

func validBundle(t *testing.T, root string) Bundle {
	t.Helper()
	sourceRoot := filepath.Join(root, "lab")
	if err := os.MkdirAll(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	scenarioRaw := []byte("kind: Scenario\nmetadata:\n  name: scenario-a\n")
	provenanceRaw := []byte("immutable provenance evidence\n")
	composeRaw := []byte("services: {}\n")
	if err := os.WriteFile(filepath.Join(sourceRoot, "scenario.yaml"), scenarioRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "provenance.txt"), provenanceRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "compose.yml"), composeRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	toolPath := filepath.Join(sourceRoot, "fake-tool")
	if err := os.WriteFile(toolPath, []byte("#!/bin/sh\necho fake tool v1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", sourceRoot+string(os.PathListSeparator)+os.Getenv("PATH"))
	git := func(args ...string) string {
		command := exec.Command("git", append([]string{"-C", sourceRoot}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	git("init", "-q")
	git("config", "user.email", "test@myceldb.local")
	git("config", "user.name", "Mycel Lab Test")
	git("add", ".")
	git("commit", "-q", "-m", "fixture")
	revision := git("rev-parse", "HEAD")
	tree := git("rev-parse", "HEAD^{tree}")
	versionDigest := sha256.Sum256([]byte("fake tool v1"))
	return Bundle{
		APIVersion: APIVersion, Kind: Kind,
		Metadata:   Metadata{Name: "candidate-one", CandidateID: "candidate-1", CreatedAt: "2026-10-31T00:00:00Z"},
		SourceRoot: ".",
		Sources:    []SourceIdentity{{Name: "lab", Path: "lab", Revision: revision, Tree: tree}},
		Harness:    HarnessIdentity{Source: "lab", ExecutableSHA256: strings.Repeat("1", 64), Revision: revision, Tree: tree, ModulePath: "github.com/MycelDB/mycel-lab", GoVersion: "go1.25.3"},
		Images: Images{Daemon: ImageIdentity{
			Name: "daemon", Reference: "docker.io/myceldb/mycel@" + testIndex, IndexDigest: testIndex, AnonymousPull: true,
			Platforms:  []PlatformIdentity{{OS: "linux", Architecture: "amd64", ManifestDigest: testManifest, ConfigDigest: testConfig}, {OS: "linux", Architecture: "arm64", ManifestDigest: testArmManifest, ConfigDigest: testArmConfig}},
			Provenance: ProvenanceIdentity{Source: "https://github.com/MycelDB/mycel", Revision: revision, BuildMethod: "docker-buildx", BuildContextSHA256: strings.Repeat("2", 64), EvidenceSource: "lab", EvidencePath: "provenance.txt"},
		}},
		Files: []FileIdentity{{Source: "lab", Role: "scenario", Path: "scenario.yaml", SHA256: digestBytes(scenarioRaw)}, {Source: "lab", Role: "provenance", Path: "provenance.txt", SHA256: digestBytes(provenanceRaw)}, {Source: "lab", Role: "runtime-config", Path: "compose.yml", SHA256: digestBytes(composeRaw)}},
		Tools: []ToolIdentity{
			{Name: "git", Path: "git", ExecutableSHA256: digestExecutable(t, "git"), VersionArguments: []string{"version"}, VersionOutputSHA256: hex.EncodeToString(versionDigest[:])},
			{Name: "docker", Path: "fake-tool", ExecutableSHA256: digestFile(t, toolPath), VersionArguments: []string{"version"}, VersionOutputSHA256: hex.EncodeToString(versionDigest[:])},
			{Name: "docker-buildx", Path: "fake-tool", ExecutableSHA256: digestFile(t, toolPath), VersionArguments: []string{"version"}, VersionOutputSHA256: hex.EncodeToString(versionDigest[:])},
			{Name: "docker-compose", Path: "fake-tool", ExecutableSHA256: digestFile(t, toolPath), VersionArguments: []string{"version"}, VersionOutputSHA256: hex.EncodeToString(versionDigest[:])},
		},
		Scenarios: []ScenarioBinding{{
			Name: "scenario-a", Source: "lab", ScenarioPath: "scenario.yaml", ResolvedScenarioSHA256: strings.Repeat("3", 64), Environment: EnvironmentBinding{Driver: "compose", Namespace: "candidate-1", Options: map[string]any{"projectName": "candidate-1"}, RuntimeFiles: []RuntimeFileBinding{{Option: "composeFile", Source: "lab", Path: "compose.yml"}}},
			DaemonImage: "docker.io/myceldb/mycel@" + testIndex, RequiredTools: []string{"git", "docker", "docker-buildx", "docker-compose"}, OuterTimeout: 5 * time.Minute,
			Finalization: FinalizationBounds{SensitiveHostCleanup: time.Second, ActorStop: time.Second, ProviderObservation: time.Second, ClusterAssertions: time.Second, FinalOracle: time.Second, PortForwardStop: time.Second, ScenarioCleanup: time.Second, EnvironmentDelete: time.Second, DeletionVerification: time.Second},
		}},
	}
}

func resolvedDaemon(image ImageIdentity) ResolvedImage {
	resolved := ResolvedImage{Name: image.Name, Reference: image.Reference, IndexDigest: image.IndexDigest}
	for _, platform := range image.Platforms {
		resolved.Platforms = append(resolved.Platforms, ResolvedPlatform{OS: platform.OS, Architecture: platform.Architecture, ManifestDigest: platform.ManifestDigest, ConfigDigest: platform.ConfigDigest, Labels: map[string]string{"org.opencontainers.image.source": image.Provenance.Source, "org.opencontainers.image.revision": image.Provenance.Revision, "io.myceldb.source.dirty": "false", "io.myceldb.build.context-digest": image.Provenance.BuildContextSHA256, "io.myceldb.build.command": image.Provenance.BuildMethod}})
	}
	return resolved
}

func writeBundle(t *testing.T, path string, bundle Bundle) {
	t.Helper()
	raw, err := yaml.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
func digestBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
func digestExecutable(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Fatal(err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return digestFile(t, path)
}

func digestFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return digestBytes(raw)
}
func findingsText(findings []ValidationFinding) string {
	var values []string
	for _, finding := range findings {
		values = append(values, finding.Stage+" "+finding.Subject+" "+finding.Message)
	}
	return strings.Join(values, " | ")
}
