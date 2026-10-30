package releasebundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion               = "myceldb.io/release-execution-bundle/v1"
	Kind                     = "ReleaseExecutionBundle"
	MaximumOuterBound        = 6 * time.Hour
	MaximumFinalizationBound = 15 * time.Minute
)

type Bundle struct {
	APIVersion string            `json:"apiVersion" yaml:"apiVersion"`
	Kind       string            `json:"kind" yaml:"kind"`
	Metadata   Metadata          `json:"metadata" yaml:"metadata"`
	SourceRoot string            `json:"sourceRoot" yaml:"sourceRoot"`
	Sources    []SourceIdentity  `json:"sources" yaml:"sources"`
	Harness    HarnessIdentity   `json:"harness" yaml:"harness"`
	Images     Images            `json:"images" yaml:"images"`
	Files      []FileIdentity    `json:"files" yaml:"files"`
	Tools      []ToolIdentity    `json:"tools" yaml:"tools"`
	Scenarios  []ScenarioBinding `json:"scenarios" yaml:"scenarios"`

	path         string
	sourcePath   string
	bundleSHA256 string
	preflight    *preflightState
}

type preflightState struct {
	once          sync.Once
	result        PreflightRecord
	err           error
	toolBin       string
	boundFileRoot string
	boundFiles    map[string]string
}

var toolPathActivation sync.Mutex

type Metadata struct {
	Name        string `json:"name" yaml:"name"`
	CandidateID string `json:"candidateId" yaml:"candidateId"`
	CreatedAt   string `json:"createdAt" yaml:"createdAt"`
}

type SourceIdentity struct {
	Name     string `json:"name" yaml:"name"`
	Path     string `json:"path" yaml:"path"`
	Revision string `json:"revision" yaml:"revision"`
	Tree     string `json:"tree" yaml:"tree"`
	Dirty    bool   `json:"dirty" yaml:"dirty"`
}

type HarnessIdentity struct {
	Source           string `json:"source" yaml:"source"`
	ExecutableSHA256 string `json:"executableSHA256" yaml:"executableSHA256"`
	Revision         string `json:"revision" yaml:"revision"`
	Tree             string `json:"tree" yaml:"tree"`
	ModulePath       string `json:"modulePath" yaml:"modulePath"`
	GoVersion        string `json:"goVersion" yaml:"goVersion"`
	Modified         bool   `json:"modified" yaml:"modified"`
}

type Images struct {
	Daemon           ImageIdentity   `json:"daemon" yaml:"daemon"`
	ProviderEmulator *ImageIdentity  `json:"providerEmulator,omitempty" yaml:"providerEmulator,omitempty"`
	Helpers          []ImageIdentity `json:"helpers,omitempty" yaml:"helpers,omitempty"`
}

type ImageIdentity struct {
	Name                       string             `json:"name" yaml:"name"`
	Reference                  string             `json:"reference" yaml:"reference"`
	Local                      bool               `json:"local,omitempty" yaml:"local,omitempty"`
	ImageID                    string             `json:"imageId,omitempty" yaml:"imageId,omitempty"`
	IndexDigest                string             `json:"indexDigest,omitempty" yaml:"indexDigest,omitempty"`
	Platforms                  []PlatformIdentity `json:"platforms" yaml:"platforms"`
	SideManifests              []string           `json:"sideManifests,omitempty" yaml:"sideManifests,omitempty"`
	AnonymousPull              bool               `json:"anonymousPull,omitempty" yaml:"anonymousPull,omitempty"`
	RuntimeEnvironmentVariable string             `json:"runtimeEnvironmentVariable,omitempty" yaml:"runtimeEnvironmentVariable,omitempty"`
	Provenance                 ProvenanceIdentity `json:"provenance" yaml:"provenance"`
}

type PlatformIdentity struct {
	OS             string `json:"os" yaml:"os"`
	Architecture   string `json:"architecture" yaml:"architecture"`
	ManifestDigest string `json:"manifestDigest" yaml:"manifestDigest"`
	ConfigDigest   string `json:"configDigest" yaml:"configDigest"`
}

type ProvenanceIdentity struct {
	Source             string `json:"source" yaml:"source"`
	Revision           string `json:"revision" yaml:"revision"`
	Dirty              bool   `json:"dirty" yaml:"dirty"`
	BuildMethod        string `json:"buildMethod" yaml:"buildMethod"`
	BuildContextSHA256 string `json:"buildContextSHA256" yaml:"buildContextSHA256"`
	EvidenceSource     string `json:"evidenceSource" yaml:"evidenceSource"`
	EvidencePath       string `json:"evidencePath" yaml:"evidencePath"`
}

type FileIdentity struct {
	Source string `json:"source" yaml:"source"`
	Role   string `json:"role" yaml:"role"`
	Path   string `json:"path" yaml:"path"`
	SHA256 string `json:"sha256" yaml:"sha256"`
}

type ToolIdentity struct {
	Name                string   `json:"name" yaml:"name"`
	Path                string   `json:"path" yaml:"path"`
	ExecutableSHA256    string   `json:"executableSHA256" yaml:"executableSHA256"`
	VersionArguments    []string `json:"versionArguments" yaml:"versionArguments"`
	VersionOutputSHA256 string   `json:"versionOutputSHA256" yaml:"versionOutputSHA256"`
}

type ScenarioBinding struct {
	Name                   string             `json:"name" yaml:"name"`
	Source                 string             `json:"source" yaml:"source"`
	ScenarioPath           string             `json:"scenarioPath" yaml:"scenarioPath"`
	SuitePath              string             `json:"suitePath,omitempty" yaml:"suitePath,omitempty"`
	ResolvedScenarioSHA256 string             `json:"resolvedScenarioSHA256" yaml:"resolvedScenarioSHA256"`
	Environment            EnvironmentBinding `json:"environment" yaml:"environment"`
	DaemonImage            string             `json:"daemonImage" yaml:"daemonImage"`
	ProviderEmulatorImage  string             `json:"providerEmulatorImage,omitempty" yaml:"providerEmulatorImage,omitempty"`
	RequiredTools          []string           `json:"requiredTools" yaml:"requiredTools"`
	OuterTimeout           time.Duration      `json:"outerTimeout" yaml:"outerTimeout"`
	Finalization           FinalizationBounds `json:"finalization" yaml:"finalization"`
}

type EnvironmentBinding struct {
	Driver        string               `json:"driver" yaml:"driver"`
	Namespace     string               `json:"namespace" yaml:"namespace"`
	KeepOnFailure bool                 `json:"keepOnFailure" yaml:"keepOnFailure"`
	Options       map[string]any       `json:"options" yaml:"options"`
	RuntimeFiles  []RuntimeFileBinding `json:"runtimeFiles,omitempty" yaml:"runtimeFiles,omitempty"`
}

type RuntimeFileBinding struct {
	Option string `json:"option" yaml:"option"`
	Source string `json:"source" yaml:"source"`
	Path   string `json:"path" yaml:"path"`
}

type FinalizationBounds struct {
	SensitiveHostCleanup time.Duration `json:"sensitiveHostCleanup" yaml:"sensitiveHostCleanup"`
	ActorStop            time.Duration `json:"actorStop" yaml:"actorStop"`
	ProviderObservation  time.Duration `json:"providerObservation" yaml:"providerObservation"`
	ClusterAssertions    time.Duration `json:"clusterAssertions" yaml:"clusterAssertions"`
	FinalOracle          time.Duration `json:"finalOracle" yaml:"finalOracle"`
	PortForwardStop      time.Duration `json:"portForwardStop" yaml:"portForwardStop"`
	ScenarioCleanup      time.Duration `json:"scenarioCleanup" yaml:"scenarioCleanup"`
	EnvironmentDelete    time.Duration `json:"environmentDelete" yaml:"environmentDelete"`
	DeletionVerification time.Duration `json:"deletionVerification" yaml:"deletionVerification"`
}

type ValidationFinding struct {
	Stage   string `json:"stage"`
	Subject string `json:"subject,omitempty"`
	Message string `json:"message"`
}

type ResolvedImage struct {
	Name          string             `json:"name"`
	Reference     string             `json:"reference"`
	ImageID       string             `json:"imageId,omitempty"`
	IndexDigest   string             `json:"indexDigest,omitempty"`
	Platforms     []ResolvedPlatform `json:"platforms"`
	SideManifests []string           `json:"sideManifests,omitempty"`
}

type ResolvedPlatform struct {
	OS             string            `json:"os"`
	Architecture   string            `json:"architecture"`
	ManifestDigest string            `json:"manifestDigest,omitempty"`
	ConfigDigest   string            `json:"configDigest,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type PreflightRecord struct {
	APIVersion     string              `json:"apiVersion"`
	Kind           string              `json:"kind"`
	BundleName     string              `json:"bundleName"`
	CandidateID    string              `json:"candidateId"`
	BundlePath     string              `json:"bundlePath"`
	BundleSHA256   string              `json:"bundleSHA256"`
	SourceRoot     string              `json:"sourceRoot"`
	CheckedAt      time.Time           `json:"checkedAt"`
	OK             bool                `json:"ok"`
	ResolvedImages []ResolvedImage     `json:"resolvedImages,omitempty"`
	Findings       []ValidationFinding `json:"findings,omitempty"`
}

func Load(path string) (*Bundle, error)      { return load(path, true) }
func LoadDraft(path string) (*Bundle, error) { return load(path, false) }

func load(path string, validate bool) (*Bundle, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("release execution bundle path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read release execution bundle: %w", err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	var bundle Bundle
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode release execution bundle: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("release execution bundle must contain exactly one document")
		}
		return nil, fmt.Errorf("decode release execution bundle trailing document: %w", err)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	bundle.path = absolute
	digest := sha256.Sum256(raw)
	bundle.bundleSHA256 = hex.EncodeToString(digest[:])
	root := strings.TrimSpace(bundle.SourceRoot)
	if root == "" {
		return nil, errors.New("sourceRoot is required")
	}
	if filepath.IsAbs(root) || escapesRoot(root) {
		return nil, fmt.Errorf("sourceRoot %q must be a relative path within the bundle directory", bundle.SourceRoot)
	}
	bundleDir, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("resolve bundle directory: %w", err)
	}
	bundle.sourcePath, err = filepath.EvalSymlinks(filepath.Join(bundleDir, root))
	if err != nil {
		return nil, fmt.Errorf("resolve sourceRoot: %w", err)
	}
	if !pathWithin(bundleDir, bundle.sourcePath) {
		return nil, fmt.Errorf("sourceRoot %q resolves outside the bundle directory", bundle.SourceRoot)
	}
	for _, source := range bundle.Sources {
		resolved, resolveErr := filepath.EvalSymlinks(filepath.Join(bundle.sourcePath, source.Path))
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve source %q: %w", source.Name, resolveErr)
		}
		if !pathWithin(bundle.sourcePath, resolved) {
			return nil, fmt.Errorf("source %q resolves outside sourceRoot", source.Name)
		}
	}
	bundle.preflight = &preflightState{}
	if validate {
		if err := bundle.ValidateStructure(); err != nil {
			return nil, err
		}
	}
	return &bundle, nil
}

func (b *Bundle) Path() string               { return b.path }
func (b *Bundle) ResolvedSourceRoot() string { return b.sourcePath }
func (b *Bundle) ResolvedSource(name string) (string, bool) {
	for _, source := range b.Sources {
		if source.Name == name {
			return filepath.Join(b.sourcePath, filepath.Clean(source.Path)), true
		}
	}
	return "", false
}

func (b *Bundle) BindsFileDigest(role, digest string) bool {
	digest = strings.TrimPrefix(strings.TrimSpace(digest), "sha256:")
	for _, file := range b.Files {
		if file.Role == role && file.SHA256 == digest {
			return true
		}
	}
	return false
}

func (b *Bundle) MaximumOuterTimeout() time.Duration {
	var maximum time.Duration
	for _, scenario := range b.Scenarios {
		if scenario.OuterTimeout > maximum {
			maximum = scenario.OuterTimeout
		}
	}
	return maximum
}

func (b *Bundle) RuntimeFilePath(binding RuntimeFileBinding) (string, error) {
	return b.boundFilePath(binding.Source, binding.Path)
}

func (b *Bundle) ScenarioFilePath(binding ScenarioBinding) (string, error) {
	return b.boundFilePath(binding.Source, binding.ScenarioPath)
}

func (b *Bundle) SuiteFilePath(binding ScenarioBinding) (string, error) {
	if strings.TrimSpace(binding.SuitePath) == "" {
		return "", errors.New("scenario has no bound suite path")
	}
	return b.boundFilePath(binding.Source, binding.SuitePath)
}

func (b *Bundle) boundFilePath(source, path string) (string, error) {
	key := source + ":" + filepath.Clean(path)
	if b.preflight != nil && b.preflight.result.OK {
		resolved, ok := b.preflight.boundFiles[key]
		if !ok {
			return "", fmt.Errorf("bound file snapshot %s is unavailable", key)
		}
		return resolved, nil
	}
	root, ok := b.ResolvedSource(source)
	if !ok {
		return "", fmt.Errorf("file references unknown source %q", source)
	}
	return resolveBoundFile(root, path)
}

func (b *Bundle) ProviderEmulatorRuntimeReference() string {
	if b.Images.ProviderEmulator == nil {
		return ""
	}
	if b.Images.ProviderEmulator.Local {
		return b.Images.ProviderEmulator.ImageID
	}
	return b.Images.ProviderEmulator.Reference
}

func (b *Bundle) Binding(name string) (ScenarioBinding, bool) {
	for _, binding := range b.Scenarios {
		if binding.Name == name {
			return binding, true
		}
	}
	return ScenarioBinding{}, false
}

func (b *Bundle) ActivateVerifiedTools() (func(), error) {
	if b.preflight == nil || b.preflight.err != nil || !b.preflight.result.OK || b.preflight.toolBin == "" {
		return nil, errors.New("release bundle tools cannot be activated before successful preflight")
	}
	for _, tool := range b.Tools {
		actual, err := fileSHA256(filepath.Join(b.preflight.toolBin, tool.Name))
		if err != nil || actual != tool.ExecutableSHA256 {
			return nil, fmt.Errorf("verified runtime tool %s changed after preflight", tool.Name)
		}
	}
	toolPathActivation.Lock()
	previous := os.Getenv("PATH")
	if err := os.Setenv("PATH", b.preflight.toolBin+string(os.PathListSeparator)+previous); err != nil {
		toolPathActivation.Unlock()
		return nil, err
	}
	return func() { _ = os.Setenv("PATH", previous); toolPathActivation.Unlock() }, nil
}

func (b *Bundle) Close() error {
	if b.preflight == nil {
		return nil
	}
	var errs []error
	if b.preflight.toolBin != "" {
		errs = append(errs, os.RemoveAll(b.preflight.toolBin))
	}
	if b.preflight.boundFileRoot != "" {
		errs = append(errs, os.RemoveAll(b.preflight.boundFileRoot))
	}
	return errors.Join(errs...)
}

func (b *Bundle) MarshalEvidence() ([]byte, error) {
	return json.MarshalIndent(b, "", "  ")
}

func (b *Bundle) ValidateStructure() error {
	var findings []ValidationFinding
	add := func(subject, message string) {
		findings = append(findings, ValidationFinding{Stage: "structure", Subject: subject, Message: message})
	}
	if b.APIVersion != APIVersion {
		add("apiVersion", fmt.Sprintf("must equal %q", APIVersion))
	}
	if b.Kind != Kind {
		add("kind", fmt.Sprintf("must equal %q", Kind))
	}
	if strings.TrimSpace(b.Metadata.Name) == "" {
		add("metadata.name", "is required")
	}
	if strings.TrimSpace(b.Metadata.CandidateID) == "" {
		add("metadata.candidateId", "is required")
	}
	if _, err := time.Parse(time.RFC3339, b.Metadata.CreatedAt); err != nil {
		add("metadata.createdAt", "must be RFC3339")
	}
	validateDigest := func(subject, value string) {
		if !validSHA256(value) {
			add(subject, "must be a lowercase 64-character SHA-256")
		}
	}
	sourceNames := map[string]SourceIdentity{}
	for index, source := range b.Sources {
		subject := fmt.Sprintf("sources[%d]", index)
		if source.Name == "" {
			add(subject+".name", "is required")
		} else if _, exists := sourceNames[source.Name]; exists {
			add(subject+".name", "must be unique")
		} else {
			sourceNames[source.Name] = source
		}
		if source.Path == "" || filepath.IsAbs(source.Path) || escapesRoot(source.Path) {
			add(subject+".path", "must be a relative path within sourceRoot")
		}
		if !validGitObject(source.Revision) {
			add(subject+".revision", "must be a full lowercase Git object ID")
		}
		if !validGitObject(source.Tree) {
			add(subject+".tree", "must be a full lowercase Git tree ID")
		}
		if source.Dirty {
			add(subject+".dirty", "must be false")
		}
	}
	if len(b.Sources) == 0 {
		add("sources", "at least one exact source tree is required")
	}
	if source, exists := sourceNames[b.Harness.Source]; !exists {
		add("harness.source", "must reference a bound source tree")
	} else {
		if source.Revision != b.Harness.Revision {
			add("harness.revision", "must equal the bound harness source revision")
		}
		if source.Tree != b.Harness.Tree {
			add("harness.tree", "must equal the bound harness source tree")
		}
	}
	validateDigest("harness.executableSHA256", b.Harness.ExecutableSHA256)
	if !validGitObject(b.Harness.Revision) {
		add("harness.revision", "must be a full lowercase 40-character Git object ID")
	}
	if !validGitObject(b.Harness.Tree) {
		add("harness.tree", "must be a full lowercase 40-character Git tree ID")
	}
	if strings.TrimSpace(b.Harness.ModulePath) == "" {
		add("harness.modulePath", "is required")
	}
	if strings.TrimSpace(b.Harness.GoVersion) == "" {
		add("harness.goVersion", "is required")
	}
	if b.Harness.Modified {
		add("harness.modified", "must be false")
	}

	if b.Images.Daemon.Local {
		add("images.daemon.local", "daemon release images must be digest-pinned registry indexes")
	}
	if b.Images.Daemon.RuntimeEnvironmentVariable != "" {
		add("images.daemon.runtimeEnvironmentVariable", "is supported only for helpers")
	}
	if b.Images.ProviderEmulator != nil && b.Images.ProviderEmulator.RuntimeEnvironmentVariable != "" {
		add("images.providerEmulator.runtimeEnvironmentVariable", "is supported only for helpers")
	}
	helperVariables := map[string]struct{}{}
	for index, helper := range b.Images.Helpers {
		if helper.Local {
			add(fmt.Sprintf("images.helpers[%d].local", index), "helper release images must be digest-pinned registry indexes")
		}
		variable := strings.TrimSpace(helper.RuntimeEnvironmentVariable)
		if !validEnvironmentVariableName(variable) {
			add(fmt.Sprintf("images.helpers[%d].runtimeEnvironmentVariable", index), "must be a valid nonempty environment variable name")
		} else if _, exists := helperVariables[variable]; exists {
			add(fmt.Sprintf("images.helpers[%d].runtimeEnvironmentVariable", index), "must be unique")
		} else {
			helperVariables[variable] = struct{}{}
		}
	}
	images := []ImageIdentity{b.Images.Daemon}
	if b.Images.ProviderEmulator != nil {
		images = append(images, *b.Images.ProviderEmulator)
	}
	images = append(images, b.Images.Helpers...)
	if len(images) == 0 || strings.TrimSpace(b.Images.Daemon.Name) == "" {
		add("images.daemon", "is required")
	}
	imageNames := map[string]struct{}{}
	for index, image := range images {
		subject := fmt.Sprintf("images[%d]", index)
		if image.Name == "" {
			add(subject+".name", "is required")
		} else if _, exists := imageNames[image.Name]; exists {
			add(subject+".name", "must be unique")
		} else {
			imageNames[image.Name] = struct{}{}
		}
		if strings.TrimSpace(image.Reference) == "" {
			add(subject+".reference", "is required")
		}
		if mutableDefaultReference(image.Reference) {
			add(subject+".reference", "mutable/default image references are forbidden")
		}
		if image.Local {
			if !explicitTaggedReference(image.Reference) {
				add(subject+".reference", "local images require an explicit non-default tag")
			}
			if !validDigest(image.ImageID) {
				add(subject+".imageId", "local images require an exact sha256 image ID")
			}
			if image.AnonymousPull {
				add(subject+".anonymousPull", "local images cannot require anonymous pull")
			}
		} else {
			if !isNamedDigestReference(image.Reference) {
				add(subject+".reference", "registry images must use a complete named @sha256 digest")
			}
			if !validDigest(image.IndexDigest) {
				add(subject+".indexDigest", "registry images require an exact sha256 index digest")
			}
			if image.IndexDigest != digestFromReference(image.Reference) {
				add(subject+".indexDigest", "must equal the reference digest")
			}
			if !image.AnonymousPull {
				add(subject+".anonymousPull", "registry release images must be anonymously pullable")
			}
		}
		if len(image.Platforms) == 0 {
			add(subject+".platforms", "at least one exact platform identity is required")
		}
		if image.Name == b.Images.Daemon.Name || helperImageNamed(b.Images.Helpers, image.Name) {
			requiredPlatforms := map[string]bool{"linux/amd64": false, "linux/arm64": false}
			for _, platform := range image.Platforms {
				if _, required := requiredPlatforms[platform.OS+"/"+platform.Architecture]; required {
					requiredPlatforms[platform.OS+"/"+platform.Architecture] = true
				}
			}
			for platform, present := range requiredPlatforms {
				if !present {
					add(subject+".platforms", "missing required native platform "+platform)
				}
			}
		}
		seenSideManifests := map[string]struct{}{}
		for sideIndex, digest := range image.SideManifests {
			if !validDigest(digest) {
				add(fmt.Sprintf("%s.sideManifests[%d]", subject, sideIndex), "must be an exact sha256 digest")
			}
			if _, exists := seenSideManifests[digest]; exists {
				add(fmt.Sprintf("%s.sideManifests[%d]", subject, sideIndex), "must be unique")
			}
			seenSideManifests[digest] = struct{}{}
		}
		platforms := map[string]struct{}{}
		for pIndex, platform := range image.Platforms {
			pSubject := fmt.Sprintf("%s.platforms[%d]", subject, pIndex)
			key := platform.OS + "/" + platform.Architecture
			if platform.OS == "" || platform.Architecture == "" {
				add(pSubject, "os and architecture are required")
			} else if _, exists := platforms[key]; exists {
				add(pSubject, "platform must be unique")
			} else {
				platforms[key] = struct{}{}
			}
			if !validDigest(platform.ManifestDigest) && !image.Local {
				add(pSubject+".manifestDigest", "must be an exact sha256 digest")
			}
			if !validDigest(platform.ConfigDigest) {
				add(pSubject+".configDigest", "must be an exact sha256 digest")
			}
		}
		provenance := image.Provenance
		if strings.TrimSpace(provenance.Source) == "" {
			add(subject+".provenance.source", "is required")
		}
		if !validGitObject(provenance.Revision) {
			add(subject+".provenance.revision", "must be a full lowercase Git object ID")
		}
		if provenance.Dirty {
			add(subject+".provenance.dirty", "must be false")
		}
		bindsProductSource := index == 0 || (b.Images.ProviderEmulator != nil && index == 1)
		if bindsProductSource || provenance.BuildMethod != "" || provenance.BuildContextSHA256 != "" {
			if strings.TrimSpace(provenance.BuildMethod) == "" {
				add(subject+".provenance.buildMethod", "is required")
			}
			if !validSHA256(provenance.BuildContextSHA256) {
				add(subject+".provenance.buildContextSHA256", "must be a lowercase 64-character SHA-256")
			}
		}
		evidenceSource, evidenceSourceExists := sourceNames[provenance.EvidenceSource]
		if !evidenceSourceExists {
			add(subject+".provenance.evidenceSource", "must reference a bound source tree")
		}
		if bindsProductSource && evidenceSourceExists && provenance.Revision != evidenceSource.Revision {
			add(subject+".provenance.revision", "must equal the provenance evidence source revision")
		}
		if strings.TrimSpace(provenance.EvidencePath) == "" {
			add(subject+".provenance.evidencePath", "is required")
		}
	}

	filePaths := map[string]FileIdentity{}
	for index, file := range b.Files {
		subject := fmt.Sprintf("files[%d]", index)
		if _, exists := sourceNames[file.Source]; !exists {
			add(subject+".source", "must reference a bound source tree")
		}
		if !validFileRole(file.Role) {
			add(subject+".role", "is not a recognized release input role")
		}
		key := file.Source + ":" + filepath.Clean(file.Path)
		if file.Path == "" || filepath.IsAbs(file.Path) || escapesRoot(file.Path) {
			add(subject+".path", "must be a relative path within its source")
		} else if _, exists := filePaths[key]; exists {
			add(subject+".path", "must be unique within its source")
		} else {
			filePaths[key] = file
		}
		validateDigest(subject+".sha256", file.SHA256)
	}
	for _, image := range images {
		key := image.Provenance.EvidenceSource + ":" + filepath.Clean(image.Provenance.EvidencePath)
		file, exists := filePaths[key]
		if !exists || file.Role != "provenance" {
			add("images."+image.Name+".provenance.evidencePath", "must reference a files entry with role provenance")
		}
	}
	if len(b.Files) == 0 {
		add("files", "must bind release source and configuration inputs")
	}

	toolNames := map[string]struct{}{}
	for index, tool := range b.Tools {
		subject := fmt.Sprintf("tools[%d]", index)
		if tool.Name == "" {
			add(subject+".name", "is required")
		} else if _, exists := toolNames[tool.Name]; exists {
			add(subject+".name", "must be unique")
		} else {
			toolNames[tool.Name] = struct{}{}
		}
		if strings.TrimSpace(tool.Path) == "" {
			add(subject+".path", "is required")
		}
		validateDigest(subject+".executableSHA256", tool.ExecutableSHA256)
		if len(tool.VersionArguments) == 0 {
			add(subject+".versionArguments", "is required")
		}
		validateDigest(subject+".versionOutputSHA256", tool.VersionOutputSHA256)
	}
	for _, required := range []string{"git", "docker", "docker-buildx", "docker-compose"} {
		if _, exists := toolNames[required]; !exists {
			add("tools", fmt.Sprintf("must bind required executable %q", required))
		}
	}

	scenarioNames := map[string]struct{}{}
	for index, scenario := range b.Scenarios {
		subject := fmt.Sprintf("scenarios[%d]", index)
		if scenario.Name == "" {
			add(subject+".name", "is required")
		} else if _, exists := scenarioNames[scenario.Name]; exists {
			add(subject+".name", "must be unique")
		} else {
			scenarioNames[scenario.Name] = struct{}{}
		}
		if _, exists := sourceNames[scenario.Source]; !exists {
			add(subject+".source", "must reference a bound source tree")
		}
		validateBoundPath := func(field, path, role string, required bool) {
			if path == "" && !required {
				return
			}
			file, exists := filePaths[scenario.Source+":"+filepath.Clean(path)]
			if path == "" || filepath.IsAbs(path) || escapesRoot(path) || !exists || file.Role != role {
				add(subject+field, "must reference a bound "+role+" file")
			}
		}
		validateBoundPath(".scenarioPath", scenario.ScenarioPath, "scenario", true)
		validateBoundPath(".suitePath", scenario.SuitePath, "suite", false)
		validateDigest(subject+".resolvedScenarioSHA256", scenario.ResolvedScenarioSHA256)
		if scenario.Environment.Driver == "" {
			add(subject+".environment.driver", "is required")
		}
		if scenario.Environment.Namespace == "" {
			add(subject+".environment.namespace", "is required")
		}
		if scenario.Environment.Options == nil {
			add(subject+".environment.options", "is required")
		}
		if scenario.Environment.KeepOnFailure {
			add(subject+".environment.keepOnFailure", "must be false for authoritative cleanup")
		}
		for _, forbidden := range []string{"imageOverride", "providerEmulatorImage"} {
			if _, exists := scenario.Environment.Options[forbidden]; exists {
				add(subject+".environment.options."+forbidden, "must be derived from the bundle image identity, not duplicated in runtime options")
			}
		}
		runtimeOptionCounts := map[string]int{}
		for runtimeIndex, runtimeFile := range scenario.Environment.RuntimeFiles {
			runtimeSubject := fmt.Sprintf("%s.environment.runtimeFiles[%d]", subject, runtimeIndex)
			if strings.TrimSpace(runtimeFile.Option) == "" {
				add(runtimeSubject+".option", "is required")
			}
			runtimeOptionCounts[runtimeFile.Option]++
			if runtimeFile.Option != "composeFiles" && runtimeOptionCounts[runtimeFile.Option] > 1 {
				add(runtimeSubject+".option", "must be unique")
			}
			if _, duplicated := scenario.Environment.Options[runtimeFile.Option]; duplicated {
				add(runtimeSubject+".option", "must not be duplicated in environment options")
			}
			file, exists := filePaths[runtimeFile.Source+":"+filepath.Clean(runtimeFile.Path)]
			if !exists || (file.Role != "runtime-config" && file.Role != "source") {
				add(runtimeSubject+".path", "must reference a bound runtime-config file")
			}
		}
		if scenario.Environment.Driver == "compose" && runtimeOptionCounts["composeFile"] == 0 && runtimeOptionCounts["composeFiles"] == 0 {
			add(subject+".environment.runtimeFiles", "compose environments must bind composeFile or composeFiles runtime inputs")
		}
		if mutableDefaultReference(scenario.DaemonImage) || scenario.DaemonImage != b.Images.Daemon.Reference {
			add(subject+".daemonImage", "must equal the immutable daemon image reference")
		}
		if scenario.ProviderEmulatorImage != "" {
			if b.Images.ProviderEmulator == nil || scenario.ProviderEmulatorImage != b.Images.ProviderEmulator.Reference {
				add(subject+".providerEmulatorImage", "must equal the bound provider emulator image")
			}
		}
		if scenario.OuterTimeout <= 0 || scenario.OuterTimeout > MaximumOuterBound {
			add(subject+".outerTimeout", fmt.Sprintf("must be positive and no greater than %s", MaximumOuterBound))
		}
		if err := scenario.Finalization.validate(); err != nil {
			add(subject+".finalization", err.Error())
		}
		seenTools := map[string]struct{}{}
		for _, tool := range scenario.RequiredTools {
			if _, duplicate := seenTools[tool]; duplicate {
				add(subject+".requiredTools", "must not contain duplicates")
			}
			seenTools[tool] = struct{}{}
			if _, exists := toolNames[tool]; !exists {
				add(subject+".requiredTools", fmt.Sprintf("references unknown tool %q", tool))
			}
		}
		if len(scenario.RequiredTools) == 0 {
			add(subject+".requiredTools", "must not be empty")
		}
	}
	if len(b.Scenarios) == 0 {
		add("scenarios", "at least one scenario binding is required")
	}
	if len(findings) > 0 {
		return ValidationError{Findings: findings}
	}
	return nil
}

func (bounds FinalizationBounds) validate() error {
	values := []time.Duration{bounds.SensitiveHostCleanup, bounds.ActorStop, bounds.ProviderObservation, bounds.ClusterAssertions, bounds.FinalOracle, bounds.PortForwardStop, bounds.ScenarioCleanup, bounds.EnvironmentDelete, bounds.DeletionVerification}
	for _, value := range values {
		if value <= 0 || value > MaximumFinalizationBound {
			return fmt.Errorf("all finalization bounds must be positive and no greater than %s", MaximumFinalizationBound)
		}
	}
	return nil
}

type ValidationError struct{ Findings []ValidationFinding }

func (e ValidationError) Error() string {
	parts := make([]string, 0, len(e.Findings))
	for _, finding := range e.Findings {
		parts = append(parts, finding.Subject+": "+finding.Message)
	}
	return "release execution bundle validation failed: " + strings.Join(parts, "; ")
}

func helperImageNamed(helpers []ImageIdentity, name string) bool {
	for _, helper := range helpers {
		if helper.Name == name {
			return true
		}
	}
	return false
}

func validEnvironmentVariableName(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range value {
		if character == '_' || character >= 'A' && character <= 'Z' || index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func validFileRole(role string) bool {
	switch role {
	case "source", "suite", "scenario", "cluster", "actor-profile", "provider-profile", "workflow", "runtime-config", "provenance":
		return true
	default:
		return false
	}
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validSHA256(strings.TrimPrefix(value, "sha256:"))
}
func validGitObject(value string) bool {
	if len(value) != 40 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func isNamedDigestReference(value string) bool {
	parts := strings.Split(value, "@")
	return len(parts) == 2 && parts[0] != "" && !strings.ContainsAny(parts[0], " \t\r\n") && validDigest(parts[1])
}
func digestFromReference(value string) string {
	if index := strings.LastIndex(value, "@"); index >= 0 {
		return value[index+1:]
	}
	return ""
}
func explicitTaggedReference(value string) bool {
	name := strings.SplitN(strings.TrimSpace(value), "@", 2)[0]
	component := name
	if slash := strings.LastIndex(component, "/"); slash >= 0 {
		component = component[slash+1:]
	}
	colon := strings.LastIndex(component, ":")
	return colon > 0 && colon < len(component)-1
}

func mutableDefaultReference(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	name := strings.SplitN(value, "@", 2)[0]
	for _, suffix := range []string{":dev", ":latest", ":head", ":main", ":master", ":edge", ":snapshot"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
func pathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && !escapesRoot(relative)
}

func escapesRoot(path string) bool {
	clean := filepath.Clean(path)
	return clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func sortedPlatforms(platforms []ResolvedPlatform) []ResolvedPlatform {
	out := append([]ResolvedPlatform(nil), platforms...)
	sort.Slice(out, func(i, j int) bool { return out[i].OS+"/"+out[i].Architecture < out[j].OS+"/"+out[j].Architecture })
	return out
}
