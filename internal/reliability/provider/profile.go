package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion              = "reliability.mycel.io/v1alpha1"
	Kind                    = "ProviderProfile"
	maxProviderProfileBytes = 256 << 10
)

type Mode string

const (
	ModeDeterministic Mode = "deterministic"
	ModeEmulator      Mode = "emulator"
	ModeLive          Mode = "live"
)

type Enforcement string

const (
	EnforcementRequired Enforcement = "required"
	EnforcementAdvisory Enforcement = "advisory"
)

type TypeMeta struct {
	APIVersion string `yaml:"apiVersion" json:"apiVersion"`
	Kind       string `yaml:"kind" json:"kind"`
}

type Metadata struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return errors.New("duration must be a string")
	}
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	d.Duration = value
	return nil
}

func (d Duration) MarshalYAML() (any, error)    { return d.String(), nil }
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}
	d.Duration = parsed
	return nil
}

type Profile struct {
	TypeMeta     `yaml:",inline" json:",inline"`
	Metadata     Metadata       `yaml:"metadata" json:"metadata"`
	Mode         Mode           `yaml:"mode" json:"mode"`
	Capabilities Capabilities   `yaml:"capabilities" json:"capabilities"`
	Budgets      Budget         `yaml:"budgets" json:"budgets"`
	Artifacts    ArtifactPolicy `yaml:"artifacts" json:"artifacts"`
}

type Capabilities struct {
	Embedding        *Capability `yaml:"embedding,omitempty" json:"embedding,omitempty"`
	Generation       *Capability `yaml:"generation,omitempty" json:"generation,omitempty"`
	Enrichment       *Capability `yaml:"enrichment,omitempty" json:"enrichment,omitempty"`
	StructuredOutput *Capability `yaml:"structuredOutput,omitempty" json:"structuredOutput,omitempty"`
	Routing          *Capability `yaml:"routing,omitempty" json:"routing,omitempty"`
}

// ProtocolEmulatorCapability returns the single capability that owns the
// protocol-emulator script. Profiles with multiple scripts are rejected so one
// deployed emulator always has one finite, reviewable outcome contract.
func (p Profile) ProtocolEmulatorCapability() (*Capability, string, error) {
	candidates := []struct {
		name  string
		value *Capability
	}{
		{"embedding", p.Capabilities.Embedding},
		{"generation", p.Capabilities.Generation},
		{"enrichment", p.Capabilities.Enrichment},
		{"structured-output", p.Capabilities.StructuredOutput},
		{"routing", p.Capabilities.Routing},
	}
	var selected *Capability
	var name string
	for _, candidate := range candidates {
		if candidate.value == nil || candidate.value.EmulatorScript == nil {
			continue
		}
		if selected != nil {
			return nil, "", errors.New("provider emulator profile must define exactly one protocol script")
		}
		selected, name = candidate.value, candidate.name
	}
	if selected == nil {
		return nil, "", errors.New("provider emulator profile requires a protocol script")
	}
	return selected, name, nil
}

type Capability struct {
	ConnectorType     string                  `yaml:"connectorType" json:"connectorType"`
	EndpointKey       string                  `yaml:"endpointKey" json:"endpointKey"`
	BaseURL           string                  `yaml:"baseURL,omitempty" json:"baseURL,omitempty"`
	ModelKey          string                  `yaml:"modelKey" json:"modelKey"`
	ProviderModelName string                  `yaml:"providerModelName,omitempty" json:"providerModelName,omitempty"`
	Dimensions        int                     `yaml:"dimensions,omitempty" json:"dimensions,omitempty"`
	VectorSpaceKey    string                  `yaml:"vectorSpaceKey,omitempty" json:"vectorSpaceKey,omitempty"`
	FixtureStrategy   string                  `yaml:"fixtureStrategy,omitempty" json:"fixtureStrategy,omitempty"`
	EmulatorScript    *ProtocolEmulatorScript `yaml:"emulatorScript,omitempty" json:"emulatorScript,omitempty"`
	SecretRef         *SecretReference        `yaml:"secretRef,omitempty" json:"secretRef,omitempty"`
	Batching          Batching                `yaml:"batching" json:"batching"`
}

type ProtocolEmulatorScript struct {
	Version      string                        `yaml:"version" json:"version"`
	RequireMatch bool                          `yaml:"requireMatch" json:"require_match"`
	MaxRequests  int                           `yaml:"maxRequests" json:"max_requests"`
	Entries      []ProtocolEmulatorScriptEntry `yaml:"entries" json:"entries"`
}

type ProtocolEmulatorScriptEntry struct {
	InputSHA256 string                    `yaml:"inputSHA256" json:"input_sha256"`
	Outcomes    []ProtocolEmulatorOutcome `yaml:"outcomes" json:"outcomes"`
}

type ProtocolEmulatorOutcome struct {
	Kind         string `yaml:"kind" json:"kind"`
	Status       int    `yaml:"status,omitempty" json:"status,omitempty"`
	DelayMS      int    `yaml:"delayMS,omitempty" json:"delay_ms,omitempty"`
	RetryAfterMS int    `yaml:"retryAfterMS,omitempty" json:"retry_after_ms,omitempty"`
	Dimensions   int    `yaml:"dimensions,omitempty" json:"dimensions,omitempty"`
	Model        string `yaml:"model,omitempty" json:"model,omitempty"`
	RequestID    string `yaml:"requestID,omitempty" json:"request_id,omitempty"`
}

type SecretReference struct {
	Env string `yaml:"env" json:"env"`
}

type Batching struct {
	Mode     string `yaml:"mode" json:"mode"`
	MaxItems int    `yaml:"maxItems" json:"maxItems"`
}

type Budget struct {
	Enforcement             Enforcement `yaml:"enforcement" json:"enforcement"`
	MaxOperations           int64       `yaml:"maxOperations,omitempty" json:"maxOperations,omitempty"`
	MaxRequests             int64       `yaml:"maxRequests,omitempty" json:"maxRequests,omitempty"`
	MaxConcurrentOperations int64       `yaml:"maxConcurrentOperations,omitempty" json:"maxConcurrentOperations,omitempty"`
	MaxConcurrentRequests   int64       `yaml:"maxConcurrentRequests,omitempty" json:"maxConcurrentRequests,omitempty"`
	MaxRetriesPerRequest    int64       `yaml:"maxRetriesPerRequest,omitempty" json:"maxRetriesPerRequest,omitempty"`
	MaxInputTokens          int64       `yaml:"maxInputTokens,omitempty" json:"maxInputTokens,omitempty"`
	MaxOutputTokens         int64       `yaml:"maxOutputTokens,omitempty" json:"maxOutputTokens,omitempty"`
	MaxEstimatedCostUSD     float64     `yaml:"maxEstimatedCostUSD,omitempty" json:"maxEstimatedCostUSD,omitempty"`
	RequestTimeout          Duration    `yaml:"requestTimeout" json:"requestTimeout"`
	TotalTimeout            Duration    `yaml:"totalTimeout" json:"totalTimeout"`
}

type ArtifactPolicy struct {
	CaptureBodies  bool `yaml:"captureBodies" json:"captureBodies"`
	CaptureHeaders bool `yaml:"captureHeaders" json:"captureHeaders"`
}

type EnforcementCapability string

const (
	EnforceHarnessOperations           EnforcementCapability = "harness.operations"
	EnforceHarnessConcurrentOperations EnforcementCapability = "harness.concurrent-operations"
	EnforceHarnessRequestTimeout       EnforcementCapability = "harness.request-timeout"
	EnforceHarnessTotalTimeout         EnforcementCapability = "harness.total-timeout"
	EnforceProviderRequests            EnforcementCapability = "provider.requests"
	EnforceProviderConcurrency         EnforcementCapability = "provider.concurrency"
	EnforceProviderRetries             EnforcementCapability = "provider.retries"
	EnforceProviderInputTokens         EnforcementCapability = "provider.input-tokens"
	EnforceProviderOutputTokens        EnforcementCapability = "provider.output-tokens"
	EnforceProviderEstimatedCost       EnforcementCapability = "provider.estimated-cost-usd"
	EnforceProviderBatchItems          EnforcementCapability = "provider.batch-items"
)

type CapabilityManifest struct {
	Capabilities map[EnforcementCapability]string
}

func ParseEnforcementCapability(value string) (EnforcementCapability, error) {
	capability := EnforcementCapability(strings.TrimSpace(value))
	switch capability {
	case EnforceHarnessOperations, EnforceHarnessConcurrentOperations, EnforceHarnessRequestTimeout, EnforceHarnessTotalTimeout,
		EnforceProviderRequests, EnforceProviderConcurrency, EnforceProviderRetries, EnforceProviderInputTokens,
		EnforceProviderOutputTokens, EnforceProviderEstimatedCost, EnforceProviderBatchItems:
		return capability, nil
	default:
		return "", fmt.Errorf("unknown enforcement capability %q", value)
	}
}

func BuiltInHarnessManifest() CapabilityManifest {
	return CapabilityManifest{Capabilities: map[EnforcementCapability]string{
		EnforceHarnessOperations:           "mycel-lab-ledger",
		EnforceHarnessConcurrentOperations: "mycel-lab-ledger",
		EnforceHarnessRequestTimeout:       "mycel-lab-context",
		EnforceHarnessTotalTimeout:         "mycel-lab-context",
	}}
}

func (m CapabilityManifest) With(capability EnforcementCapability, source string) CapabilityManifest {
	out := CapabilityManifest{Capabilities: make(map[EnforcementCapability]string, len(m.Capabilities)+1)}
	for key, value := range m.Capabilities {
		out.Capabilities[key] = value
	}
	out.Capabilities[capability] = strings.TrimSpace(source)
	return out
}

type EnforcementStatus struct {
	Limit      string                `json:"limit"`
	Capability EnforcementCapability `json:"capability"`
	Status     string                `json:"status"`
	Source     string                `json:"source,omitempty"`
}

type ResolvedProfile struct {
	Profile           Profile             `json:"profile"`
	SourceSHA256      string              `json:"sourceSHA256"`
	Selection         string              `json:"selection"`
	EnforcementStatus []EnforcementStatus `json:"enforcementStatus"`
	secretValues      []string
}

func (r ResolvedProfile) RuntimeSecretValues() []string {
	return append([]string(nil), r.secretValues...)
}

type ResolveOptions struct {
	DryRun               bool
	Selection            string
	RequiredCapabilities []string
	Manifest             CapabilityManifest
	LookupEnv            func(string) (string, bool)
}

func Load(raw []byte) (Profile, error) {
	if len(raw) == 0 || len(raw) > maxProviderProfileBytes {
		return Profile{}, fmt.Errorf("provider profile must be between 1 and %d bytes", maxProviderProfileBytes)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Profile{}, errors.New("provider profile must contain exactly one document")
		}
		return Profile{}, err
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

func LoadFile(path string) (Profile, []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return Profile{}, nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxProviderProfileBytes+1))
	if err != nil {
		return Profile{}, nil, err
	}
	profile, err := Load(raw)
	return profile, raw, err
}

func (p Profile) Validate() error {
	if p.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if p.Kind != Kind {
		return fmt.Errorf("kind must be %q", Kind)
	}
	if err := boundedRequiredString("metadata.name", p.Metadata.Name, 128); err != nil {
		return err
	}
	if len(p.Metadata.Description) > 2048 {
		return errors.New("metadata.description exceeds 2048 bytes")
	}
	switch p.Mode {
	case ModeDeterministic, ModeEmulator, ModeLive:
	default:
		return fmt.Errorf("mode %q is not supported", p.Mode)
	}
	capabilities := p.Capabilities.named()
	if len(capabilities) == 0 {
		return errors.New("at least one capability is required")
	}
	for _, named := range capabilities {
		if err := validateCapability(p.Mode, named.name, named.value); err != nil {
			return err
		}
	}
	if err := p.Budgets.Validate(); err != nil {
		return err
	}
	if p.Mode == ModeLive && (p.Budgets.MaxRequests == 0 || p.Budgets.MaxConcurrentRequests == 0 || p.Budgets.MaxInputTokens == 0) {
		return errors.New("live profiles require positive maxRequests, maxConcurrentRequests, and maxInputTokens")
	}
	if p.Artifacts.CaptureBodies || p.Artifacts.CaptureHeaders {
		return errors.New("provider body/header artifact capture is unsupported in v1alpha1")
	}
	return nil
}

type namedCapability struct {
	name  string
	value *Capability
}

func (c Capabilities) named() []namedCapability {
	values := []namedCapability{
		{name: "embedding", value: c.Embedding},
		{name: "generation", value: c.Generation},
		{name: "enrichment", value: c.Enrichment},
		{name: "structured-output", value: c.StructuredOutput},
		{name: "routing", value: c.Routing},
	}
	out := values[:0]
	for _, value := range values {
		if value.value != nil {
			out = append(out, value)
		}
	}
	return out
}

func validateCapability(mode Mode, name string, capability *Capability) error {
	prefix := "capabilities." + name
	requiredStrings := []struct{ field, value string }{
		{field: "connectorType", value: capability.ConnectorType},
		{field: "endpointKey", value: capability.EndpointKey},
		{field: "modelKey", value: capability.ModelKey},
	}
	for _, item := range requiredStrings {
		if err := boundedRequiredString(prefix+"."+item.field, item.value, 256); err != nil {
			return err
		}
	}
	optionalStrings := []struct{ field, value string }{
		{field: "baseURL", value: capability.BaseURL},
		{field: "providerModelName", value: capability.ProviderModelName},
		{field: "vectorSpaceKey", value: capability.VectorSpaceKey},
		{field: "fixtureStrategy", value: capability.FixtureStrategy},
	}
	for _, item := range optionalStrings {
		if len(item.value) > 2048 {
			return fmt.Errorf("%s.%s exceeds 2048 bytes", prefix, item.field)
		}
	}
	if capability.Batching.Mode != "single" && capability.Batching.Mode != "batch" {
		return fmt.Errorf("%s batching.mode must be single or batch", prefix)
	}
	if capability.Batching.MaxItems <= 0 || capability.Batching.MaxItems > 1024 {
		return fmt.Errorf("%s batching.maxItems must be between 1 and 1024", prefix)
	}
	if capability.Batching.Mode == "single" && capability.Batching.MaxItems != 1 {
		return fmt.Errorf("%s single batching requires maxItems 1", prefix)
	}
	if name == "embedding" {
		maximumDimensions := 65536
		if mode == ModeDeterministic {
			maximumDimensions = 4096
		}
		if capability.Dimensions <= 0 || capability.Dimensions > maximumDimensions {
			return fmt.Errorf("%s dimensions must be between 1 and %d", prefix, maximumDimensions)
		}
		if strings.TrimSpace(capability.VectorSpaceKey) == "" {
			return fmt.Errorf("%s vectorSpaceKey is required", prefix)
		}
	} else if capability.Dimensions != 0 || capability.VectorSpaceKey != "" {
		return fmt.Errorf("%s dimensions/vectorSpaceKey are valid only for embedding", prefix)
	}
	if mode == ModeDeterministic {
		if capability.ConnectorType != "fake" || strings.TrimSpace(capability.FixtureStrategy) == "" {
			return fmt.Errorf("%s deterministic capability requires fake connectorType and fixtureStrategy", prefix)
		}
	}
	if capability.EmulatorScript != nil {
		if mode != ModeEmulator || capability.ConnectorType != "openai-compatible" {
			return fmt.Errorf("%s emulatorScript requires emulator openai-compatible mode", prefix)
		}
		if err := validateProtocolEmulatorScript(prefix+".emulatorScript", capability.EmulatorScript); err != nil {
			return err
		}
	}
	if (mode == ModeDeterministic || mode == ModeEmulator) && capability.SecretRef != nil {
		return fmt.Errorf("%s %s mode cannot declare secretRef", prefix, mode)
	}
	if mode == ModeLive && capability.SecretRef == nil {
		return fmt.Errorf("%s live mode requires secretRef", prefix)
	}
	if capability.SecretRef != nil {
		if err := boundedRequiredString(prefix+".secretRef.env", capability.SecretRef.Env, 128); err != nil {
			return err
		}
		if !validEnvironmentName(capability.SecretRef.Env) {
			return fmt.Errorf("%s.secretRef.env must be a portable environment variable name", prefix)
		}
	}
	return nil
}

func ValidateProtocolEmulatorScript(script *ProtocolEmulatorScript) error {
	if script == nil {
		return errors.New("emulator script is required")
	}
	return validateProtocolEmulatorScript("emulatorScript", script)
}

func validateProtocolEmulatorScript(prefix string, script *ProtocolEmulatorScript) error {
	if script.Version != "v1" {
		return fmt.Errorf("%s.version must be v1", prefix)
	}
	if script.MaxRequests <= 0 || script.MaxRequests > 100000 {
		return fmt.Errorf("%s.maxRequests must be between 1 and 100000", prefix)
	}
	if len(script.Entries) > 256 {
		return fmt.Errorf("%s.entries must contain at most 256 items", prefix)
	}
	seen := make(map[string]struct{}, len(script.Entries))
	for entryIndex, entry := range script.Entries {
		digest := strings.TrimSpace(entry.InputSHA256)
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			return fmt.Errorf("%s.entries[%d].inputSHA256 must be lowercase SHA-256", prefix, entryIndex)
		}
		if _, exists := seen[digest]; exists {
			return fmt.Errorf("%s.entries[%d].inputSHA256 is duplicated", prefix, entryIndex)
		}
		seen[digest] = struct{}{}
		if len(entry.Outcomes) == 0 || len(entry.Outcomes) > 32 {
			return fmt.Errorf("%s.entries[%d].outcomes must contain between 1 and 32 items", prefix, entryIndex)
		}
		for outcomeIndex, outcome := range entry.Outcomes {
			switch outcome.Kind {
			case "success", "http-error", "malformed-json", "truncated-json", "refusal", "wrong-dimensions":
			default:
				return fmt.Errorf("%s.entries[%d].outcomes[%d].kind is unsupported", prefix, entryIndex, outcomeIndex)
			}
			if outcome.DelayMS < 0 || outcome.DelayMS > 30000 || outcome.RetryAfterMS < 0 || outcome.RetryAfterMS > 30000 {
				return fmt.Errorf("%s.entries[%d].outcomes[%d] delay bounds exceed 30000ms", prefix, entryIndex, outcomeIndex)
			}
			if outcome.Kind == "http-error" {
				if outcome.Status < 400 || outcome.Status > 599 {
					return fmt.Errorf("%s.entries[%d].outcomes[%d].status must be between 400 and 599", prefix, entryIndex, outcomeIndex)
				}
			} else if outcome.Status != 0 {
				return fmt.Errorf("%s.entries[%d].outcomes[%d].status is valid only for http-error", prefix, entryIndex, outcomeIndex)
			}
			if outcome.Dimensions < 0 || outcome.Dimensions > 65536 {
				return fmt.Errorf("%s.entries[%d].outcomes[%d].dimensions is out of range", prefix, entryIndex, outcomeIndex)
			}
			if len(outcome.Model) > 256 || len(outcome.RequestID) > 128 {
				return fmt.Errorf("%s.entries[%d].outcomes[%d] identity exceeds bounds", prefix, entryIndex, outcomeIndex)
			}
			for _, value := range []string{outcome.Model, outcome.RequestID} {
				for _, character := range value {
					if character < 0x21 || character > 0x7e {
						return fmt.Errorf("%s.entries[%d].outcomes[%d] identity contains unsafe characters", prefix, entryIndex, outcomeIndex)
					}
				}
			}
		}
	}
	return nil
}

func validEnvironmentName(value string) bool {
	if value == "" || !((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z') || value[0] == '_') {
		return false
	}
	for index := 1; index < len(value); index++ {
		character := value[index]
		if !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '_') {
			return false
		}
	}
	return true
}

func boundedRequiredString(name, value string, maximum int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > maximum {
		return fmt.Errorf("%s exceeds %d bytes", name, maximum)
	}
	return nil
}

func (b Budget) Validate() error {
	if b.Enforcement != EnforcementRequired && b.Enforcement != EnforcementAdvisory {
		return errors.New("budgets.enforcement must be required or advisory")
	}
	integers := []struct {
		name  string
		value int64
	}{
		{name: "maxOperations", value: b.MaxOperations},
		{name: "maxRequests", value: b.MaxRequests},
		{name: "maxConcurrentOperations", value: b.MaxConcurrentOperations},
		{name: "maxConcurrentRequests", value: b.MaxConcurrentRequests},
		{name: "maxRetriesPerRequest", value: b.MaxRetriesPerRequest},
		{name: "maxInputTokens", value: b.MaxInputTokens},
		{name: "maxOutputTokens", value: b.MaxOutputTokens},
	}
	for _, item := range integers {
		if item.value < 0 || item.value > 1_000_000_000 {
			return fmt.Errorf("budgets.%s must be between 0 and 1000000000", item.name)
		}
	}
	if b.MaxEstimatedCostUSD < 0 || math.IsNaN(b.MaxEstimatedCostUSD) || math.IsInf(b.MaxEstimatedCostUSD, 0) || b.MaxEstimatedCostUSD > 1_000_000 {
		return errors.New("budgets.maxEstimatedCostUSD must be finite and between 0 and 1000000")
	}
	if b.MaxOperations == 0 || b.MaxConcurrentOperations == 0 {
		return errors.New("budgets.maxOperations and maxConcurrentOperations must be positive")
	}
	if b.RequestTimeout.Duration <= 0 || b.TotalTimeout.Duration <= 0 || b.RequestTimeout.Duration > b.TotalTimeout.Duration {
		return errors.New("budgets requestTimeout and totalTimeout must be positive and requestTimeout must not exceed totalTimeout")
	}
	if b.TotalTimeout.Duration > 24*time.Hour {
		return errors.New("budgets.totalTimeout must not exceed 24h")
	}
	return nil
}

func Resolve(profile Profile, source []byte, opts ResolveOptions) (ResolvedProfile, error) {
	if err := profile.Validate(); err != nil {
		return ResolvedProfile{}, err
	}
	required := append([]string(nil), opts.RequiredCapabilities...)
	if len(required) == 0 {
		required = []string{"embedding"}
	}
	if len(required) != 1 || (required[0] != "embedding" && required[0] != "generation") {
		return ResolvedProfile{}, fmt.Errorf("required provider capabilities %v are not available in this harness version", required)
	}
	var selected *Capability
	switch required[0] {
	case "embedding":
		selected = profile.Capabilities.Embedding
	case "generation":
		selected = profile.Capabilities.Generation
	}
	if selected == nil {
		return ResolvedProfile{}, fmt.Errorf("provider profile does not provide required %s capability", required[0])
	}
	for _, named := range profile.Capabilities.named() {
		if named.name != required[0] {
			return ResolvedProfile{}, fmt.Errorf("provider capability %s is outside the selected %s contract", named.name, required[0])
		}
	}
	switch required[0] {
	case "embedding":
		switch profile.Mode {
		case ModeDeterministic:
			if selected.ConnectorType != "fake" || selected.FixtureStrategy != "token-basis-v1" {
				return ResolvedProfile{}, errors.New("deterministic embedding requires supported fake token-basis-v1 fixture")
			}
		case ModeEmulator, ModeLive:
			if selected.ConnectorType != "openai-compatible" {
				return ResolvedProfile{}, fmt.Errorf("%s embedding connector %q is not supported", profile.Mode, selected.ConnectorType)
			}
		}
	case "generation":
		if profile.Mode == ModeDeterministic || selected.ConnectorType != "openai-compatible" {
			return ResolvedProfile{}, fmt.Errorf("%s generation connector %q is not supported", profile.Mode, selected.ConnectorType)
		}
	}
	if (profile.Mode == ModeEmulator || profile.Mode == ModeLive) && (strings.TrimSpace(selected.BaseURL) == "" || strings.TrimSpace(selected.ProviderModelName) == "") {
		return ResolvedProfile{}, fmt.Errorf("%s %s requires baseURL and providerModelName", profile.Mode, required[0])
	}
	manifest := BuiltInHarnessManifest()
	if profile.Mode == ModeDeterministic && selected.ConnectorType == "fake" && selected.Batching.Mode == "single" && selected.Batching.MaxItems == 1 {
		manifest = manifest.With(EnforceProviderBatchItems, "daemon-fake-connector-single-item-contract")
	}
	for capability, source := range opts.Manifest.Capabilities {
		manifest = manifest.With(capability, source)
	}
	status := make([]EnforcementStatus, 0, 12)
	for _, requirement := range profile.Budgets.requirements(profile.Capabilities) {
		source, available := manifest.Capabilities[requirement.capability]
		item := EnforcementStatus{Limit: requirement.limit, Capability: requirement.capability, Status: "unavailable"}
		if available && strings.TrimSpace(source) != "" {
			item.Status = "enforced"
			item.Source = source
		} else if profile.Budgets.Enforcement == EnforcementAdvisory {
			item.Status = "advisory"
		}
		status = append(status, item)
		if profile.Budgets.Enforcement == EnforcementRequired && item.Status != "enforced" {
			return ResolvedProfile{}, fmt.Errorf("required provider limit %s lacks advertised enforcement capability %s", requirement.limit, requirement.capability)
		}
	}
	sort.Slice(status, func(i, j int) bool { return status[i].Limit < status[j].Limit })
	lookup := opts.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var secrets []string
	if profile.Mode == ModeLive && !opts.DryRun {
		seen := map[string]struct{}{}
		for _, named := range profile.Capabilities.named() {
			envName := named.value.SecretRef.Env
			if _, ok := seen[envName]; ok {
				continue
			}
			seen[envName] = struct{}{}
			value, ok := lookup(envName)
			if !ok || value == "" {
				return ResolvedProfile{}, fmt.Errorf("required provider credential environment variable %q is not set", envName)
			}
			secrets = append(secrets, value)
		}
	}
	hash := sha256.Sum256(source)
	selection := strings.TrimSpace(opts.Selection)
	if selection == "" {
		selection = "scenario"
	}
	return ResolvedProfile{Profile: profile, SourceSHA256: "sha256:" + hex.EncodeToString(hash[:]), Selection: selection, EnforcementStatus: status, secretValues: secrets}, nil
}

type budgetRequirement struct {
	limit      string
	capability EnforcementCapability
}

func (b Budget) requirements(capabilities Capabilities) []budgetRequirement {
	out := []budgetRequirement{
		{limit: "maxOperations", capability: EnforceHarnessOperations},
		{limit: "maxConcurrentOperations", capability: EnforceHarnessConcurrentOperations},
		{limit: "requestTimeout", capability: EnforceHarnessRequestTimeout},
		{limit: "totalTimeout", capability: EnforceHarnessTotalTimeout},
	}
	if b.MaxRequests > 0 {
		out = append(out, budgetRequirement{limit: "maxRequests", capability: EnforceProviderRequests})
	}
	if b.MaxConcurrentRequests > 0 {
		out = append(out, budgetRequirement{limit: "maxConcurrentRequests", capability: EnforceProviderConcurrency})
	}
	if b.MaxRetriesPerRequest > 0 {
		out = append(out, budgetRequirement{limit: "maxRetriesPerRequest", capability: EnforceProviderRetries})
	}
	if b.MaxInputTokens > 0 {
		out = append(out, budgetRequirement{limit: "maxInputTokens", capability: EnforceProviderInputTokens})
	}
	if b.MaxOutputTokens > 0 {
		out = append(out, budgetRequirement{limit: "maxOutputTokens", capability: EnforceProviderOutputTokens})
	}
	if b.MaxEstimatedCostUSD > 0 {
		out = append(out, budgetRequirement{limit: "maxEstimatedCostUSD", capability: EnforceProviderEstimatedCost})
	}
	for _, named := range capabilities.named() {
		if named.value.Batching.MaxItems > 0 {
			out = append(out, budgetRequirement{limit: "capabilities." + named.name + ".batching.maxItems", capability: EnforceProviderBatchItems})
		}
	}
	return out
}
