package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// StageSpec is a provider-neutral linear dataflow, not a claim that the pinned
// daemon executes every declared capability. graph is the only external input.
type StageSpec struct {
	Name       string `yaml:"name" json:"name"`
	Capability string `yaml:"capability" json:"capability"`
	ProfileRef string `yaml:"profileRef" json:"profileRef"`
	InputFrom  string `yaml:"inputFrom" json:"inputFrom"`
}

type CompositionStage struct {
	StageSpec       `yaml:",inline" json:",inline"`
	ProfileName     string `json:"profileName"`
	ProfileSHA256   string `json:"profileSHA256"`
	Mode            Mode   `json:"mode"`
	ConnectorType   string `json:"connectorType"`
	EndpointKey     string `json:"endpointKey"`
	ModelKey        string `json:"modelKey"`
	Dimensions      int    `json:"dimensions,omitempty"`
	VectorSpaceKey  string `json:"vectorSpaceKey,omitempty"`
	InputKind       string `json:"inputKind"`
	OutputKind      string `json:"outputKind"`
	RequiredAdapter string `json:"requiredAdapter"`
}

type CompositionPlan struct {
	SchemaVersion                int                `json:"schemaVersion"`
	Status                       string             `json:"status"` // planned, never passed: no provider was invoked
	Stages                       []CompositionStage `json:"stages"`
	MaximumWallClockMilliseconds int64              `json:"maximumWallClockMilliseconds"`
	RuntimeBlockers              []string           `json:"runtimeBlockers"`
}

type CompositionCapabilityError struct{ Missing []string }

func (e *CompositionCapabilityError) Error() string {
	return fmt.Sprintf("semantic provider composition is not executable: missing adapters %v", e.Missing)
}
func (p CompositionPlan) RequireExecutable() error {
	if len(p.RuntimeBlockers) > 0 {
		return &CompositionCapabilityError{Missing: append([]string(nil), p.RuntimeBlockers...)}
	}
	if p.SchemaVersion != 1 || p.Status != "planned" || len(p.Stages) == 0 {
		return errors.New("composition has no validated plan")
	}
	return nil
}

func ValidateStages(stages []StageSpec) error {
	if len(stages) < 1 || len(stages) > 4 {
		return errors.New("providerStages must contain 1..4 stages")
	}
	previous := "graph"
	names := map[string]bool{"graph": true}
	for i, stage := range stages {
		if !ValidEmulatorGate(stage.Name) || names[stage.Name] || stage.InputFrom != previous {
			return errors.New("providerStages require unique bounded names and ordered explicit inputs without cycles")
		}
		if err := boundedRequiredString("stage.profileRef", stage.ProfileRef, 256); err != nil {
			return err
		}
		switch stage.Capability {
		case "embedding", "generation", "enrichment", "structured-output", "routing":
		default:
			return errors.New("unknown provider stage capability")
		}
		if (stage.Capability == "embedding") != (i == len(stages)-1) {
			return errors.New("exactly one embedding stage must terminate the composition")
		}
		names[stage.Name] = true
		previous = stage.Name
	}
	return nil
}

// PlanComposition validates all profiles before any credential lookup. Callers
// supply source bytes and trusted implemented-adapter capabilities, not an
// operator assertion that unsupported daemon behavior exists. Runtime binding
// remains fail-closed until every stage adapter is implemented.
func PlanComposition(stages []StageSpec, sources map[string][]byte, adapters map[string]bool) (CompositionPlan, error) {
	result := CompositionPlan{SchemaVersion: 1, Status: "planned"}
	if err := ValidateStages(stages); err != nil {
		return result, err
	}
	kind := "text"
	var total time.Duration
	for _, stage := range stages {
		raw, ok := sources[stage.ProfileRef]
		if !ok {
			return result, errors.New("composition provider profile is missing")
		}
		profile, err := Load(raw)
		if err != nil {
			return result, fmt.Errorf("composition stage %s: %w", stage.Name, err)
		}
		var capability *Capability
		for _, named := range profile.Capabilities.named() {
			if named.name == stage.Capability {
				capability = named.value
			}
		}
		if capability == nil {
			return result, fmt.Errorf("composition stage %s lacks its declared capability", stage.Name)
		}
		if profile.Mode != ModeDeterministic && (capability.BaseURL == "" || capability.ProviderModelName == "") {
			return result, errors.New("network stage requires explicit endpoint and provider model")
		}
		output := "text"
		switch stage.Capability {
		case "embedding":
			if kind != "text" {
				return result, errors.New("embedding requires an explicit text-producing predecessor")
			}
			output = "vector"
		case "structured-output":
			output = "json"
		case "routing":
			output = kind
		}
		next := profile.Budgets.TotalTimeout.Duration
		if next <= 0 || next > 24*time.Hour-total {
			return result, errors.New("composition wall-clock ceiling exceeds 24 hours")
		}
		total += next
		sum := sha256.Sum256(raw)
		adapter := "semantic-stage/" + stage.Capability + "/" + capability.ConnectorType
		if profile.Mode == ModeDeterministic {
			adapter += "/" + capability.FixtureStrategy
		}
		result.Stages = append(result.Stages, CompositionStage{StageSpec: stage, ProfileName: profile.Metadata.Name, ProfileSHA256: hex.EncodeToString(sum[:]), Mode: profile.Mode, ConnectorType: capability.ConnectorType, EndpointKey: capability.EndpointKey, ModelKey: capability.ModelKey, Dimensions: capability.Dimensions, VectorSpaceKey: capability.VectorSpaceKey, InputKind: kind, OutputKind: output, RequiredAdapter: adapter})
		if !adapters[adapter] {
			result.RuntimeBlockers = append(result.RuntimeBlockers, adapter)
		}
		kind = output
	}
	result.MaximumWallClockMilliseconds = total.Milliseconds()
	return result, nil
}

// SemanticStageAdapters describes actual v0.21.4 Lab provisioning support. It
// intentionally does not advertise generation/enrichment semantic execution.
func SemanticStageAdapters() map[string]bool {
	return map[string]bool{"semantic-stage/embedding/fake/token-basis-v1": true, "semantic-stage/embedding/openai-compatible": true}
}
