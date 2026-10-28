package spec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"gopkg.in/yaml.v3"
)

const APIVersion = "myceldb.io/reliability/v1"

const (
	KindClusterProfile = "ClusterProfile"
	KindActorProfile   = "ActorProfile"
	KindScenario       = "Scenario"
	KindSuite          = "Suite"
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

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && (value.Value == "" || value.Value == "0") {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value.Value, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

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

type ClusterProfile struct {
	TypeMeta `yaml:",inline" json:",inline"`
	Metadata Metadata    `yaml:"metadata" json:"metadata"`
	Cluster  ClusterSpec `yaml:"cluster" json:"cluster"`
}

type ClusterSpec struct {
	Nodes     int               `yaml:"nodes" json:"nodes"`
	Image     string            `yaml:"image" json:"image"`
	Raft      RaftSpec          `yaml:"raft" json:"raft"`
	Env       map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	Resources ResourceSpec      `yaml:"resources,omitempty" json:"resources,omitempty"`
	Storage   StorageSpec       `yaml:"storage,omitempty" json:"storage,omitempty"`
	Extra     map[string]any    `yaml:",inline" json:"-"`
}

type RaftSpec struct {
	NodeCount       int            `yaml:"nodeCount" json:"nodeCount"`
	PartitionCount  int            `yaml:"partitionCount" json:"partitionCount"`
	ReplicaFactor   int            `yaml:"replicaFactor" json:"replicaFactor"`
	LocalNodeIDMode string         `yaml:"localNodeIDMode,omitempty" json:"localNodeIDMode,omitempty"`
	ElectionTick    int            `yaml:"electionTick,omitempty" json:"electionTick,omitempty"`
	HeartbeatTick   int            `yaml:"heartbeatTick,omitempty" json:"heartbeatTick,omitempty"`
	SendTimeout     Duration       `yaml:"sendTimeout,omitempty" json:"sendTimeout,omitempty"`
	Compaction      CompactionSpec `yaml:"compaction,omitempty" json:"compaction,omitempty"`
}

type CompactionSpec struct {
	Mode                     string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	SnapshotEntries          int      `yaml:"snapshotEntries,omitempty" json:"snapshotEntries,omitempty"`
	SnapshotInterval         Duration `yaml:"snapshotInterval,omitempty" json:"snapshotInterval,omitempty"`
	SnapshotMaxLogBytes      int64    `yaml:"snapshotMaxLogBytes,omitempty" json:"snapshotMaxLogBytes,omitempty"`
	SnapshotMinRetainEntries int      `yaml:"snapshotMinRetainEntries,omitempty" json:"snapshotMinRetainEntries,omitempty"`
}

type ResourceSpec struct {
	Requests map[string]string `yaml:"requests,omitempty" json:"requests,omitempty"`
	Limits   map[string]string `yaml:"limits,omitempty" json:"limits,omitempty"`
}

type StorageSpec struct {
	Size      string `yaml:"size,omitempty" json:"size,omitempty"`
	ClassName string `yaml:"className,omitempty" json:"className,omitempty"`
}

type ActorProfile struct {
	TypeMeta `yaml:",inline" json:",inline"`
	Metadata Metadata       `yaml:"metadata" json:"metadata"`
	Behavior map[string]any `yaml:"behavior" json:"behavior"`
}

type Scenario struct {
	TypeMeta           `yaml:",inline" json:",inline"`
	Metadata           Metadata                        `yaml:"metadata" json:"metadata"`
	Seed               int64                           `yaml:"seed" json:"seed"`
	Environment        EnvironmentSpec                 `yaml:"environment" json:"environment"`
	ClusterRef         string                          `yaml:"clusterRef" json:"clusterRef"`
	ProviderProfileRef string                          `yaml:"providerProfileRef,omitempty" json:"providerProfileRef,omitempty"`
	Semantic           *SemanticScenarioSpec           `yaml:"semantic,omitempty" json:"semantic,omitempty"`
	ProcedureInference *ProcedureInferenceScenarioSpec `yaml:"procedureInference,omitempty" json:"procedureInference,omitempty"`
	ClusterOverrides   map[string]any                  `yaml:"clusterOverrides,omitempty" json:"clusterOverrides,omitempty"`
	ActorGroups        []ActorGroupSpec                `yaml:"actorGroups" json:"actorGroups"`
	Phases             []PhaseSpec                     `yaml:"phases" json:"phases"`
	Assertions         map[string]any                  `yaml:"assertions,omitempty" json:"assertions,omitempty"`
	Artifacts          map[string]bool                 `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
}

type SemanticScenarioSpec struct {
	TargetActorGroup string                  `yaml:"targetActorGroup" json:"targetActorGroup"`
	Rule             SemanticRuleSpec        `yaml:"rule" json:"rule"`
	Maintenance      SemanticMaintenanceSpec `yaml:"maintenance,omitempty" json:"maintenance,omitempty"`
}

type SemanticRuleSpec struct {
	DeferProvisioning bool     `yaml:"deferProvisioning,omitempty" json:"deferProvisioning,omitempty"`
	Key               string   `yaml:"key" json:"key"`
	DisplayName       string   `yaml:"displayName,omitempty" json:"displayName,omitempty"`
	Description       string   `yaml:"description,omitempty" json:"description,omitempty"`
	TargetLabels      []string `yaml:"targetLabels" json:"targetLabels"`
	BindingKey        string   `yaml:"bindingKey,omitempty" json:"bindingKey,omitempty"`
	Purpose           string   `yaml:"purpose,omitempty" json:"purpose,omitempty"`
	SourceMode        string   `yaml:"sourceMode,omitempty" json:"sourceMode,omitempty"`
	IncludeProperties []string `yaml:"includeProperties,omitempty" json:"includeProperties,omitempty"`
	ExcludeProperties []string `yaml:"excludeProperties,omitempty" json:"excludeProperties,omitempty"`
	MinimumTextLength int      `yaml:"minimumTextLength,omitempty" json:"minimumTextLength,omitempty"`
	PhysicalIndex     string   `yaml:"physicalIndex,omitempty" json:"physicalIndex,omitempty"`
	DirtyCooldown     Duration `yaml:"dirtyCooldown,omitempty" json:"dirtyCooldown,omitempty"`
	MaxBatchSize      int      `yaml:"maxBatchSize,omitempty" json:"maxBatchSize,omitempty"`
	WorkerConcurrency int      `yaml:"workerConcurrency,omitempty" json:"workerConcurrency,omitempty"`
}

type SemanticMaintenanceSpec struct {
	Timeout      Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	PollInterval Duration `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
	AnalyzeLimit int      `yaml:"analyzeLimit,omitempty" json:"analyzeLimit,omitempty"`
	ProcessLimit int      `yaml:"processLimit,omitempty" json:"processLimit,omitempty"`
}

func (s *SemanticScenarioSpec) setDefaults() {
	if s == nil {
		return
	}
	if strings.TrimSpace(s.Rule.DisplayName) == "" {
		s.Rule.DisplayName = s.Rule.Key
	}
	if strings.TrimSpace(s.Rule.BindingKey) == "" {
		s.Rule.BindingKey = "search"
	}
	if strings.TrimSpace(s.Rule.Purpose) == "" {
		s.Rule.Purpose = "semantic_search"
	}
	if strings.TrimSpace(s.Rule.SourceMode) == "" {
		s.Rule.SourceMode = "self"
	}
	if strings.TrimSpace(s.Rule.PhysicalIndex) == "" {
		s.Rule.PhysicalIndex = "exact"
	}
	if s.Rule.MaxBatchSize == 0 {
		s.Rule.MaxBatchSize = 16
	}
	if s.Rule.WorkerConcurrency == 0 {
		s.Rule.WorkerConcurrency = 1
	}
	if s.Maintenance.Timeout.Duration == 0 {
		s.Maintenance.Timeout.Duration = 2 * time.Minute
	}
	if s.Maintenance.PollInterval.Duration == 0 {
		s.Maintenance.PollInterval.Duration = 250 * time.Millisecond
	}
	if s.Maintenance.AnalyzeLimit == 0 {
		s.Maintenance.AnalyzeLimit = 256
	}
	if s.Maintenance.ProcessLimit == 0 {
		s.Maintenance.ProcessLimit = 256
	}
}

func (s SemanticScenarioSpec) Validate(actorGroups map[string]bool, providerProfileRef string) error {
	if strings.TrimSpace(providerProfileRef) == "" {
		return errors.New("semantic provisioning requires providerProfileRef")
	}
	if !actorGroups[strings.TrimSpace(s.TargetActorGroup)] {
		return fmt.Errorf("semantic.targetActorGroup %q does not name an actor group", s.TargetActorGroup)
	}
	if strings.TrimSpace(s.Rule.Key) == "" {
		return errors.New("semantic.rule.key is required")
	}
	if len(s.Rule.TargetLabels) == 0 {
		return errors.New("semantic.rule.targetLabels must not be empty")
	}
	for _, label := range s.Rule.TargetLabels {
		if strings.TrimSpace(label) == "" {
			return errors.New("semantic.rule.targetLabels contains an empty label")
		}
	}
	if s.Rule.MinimumTextLength < 0 || s.Rule.MaxBatchSize <= 0 || s.Rule.WorkerConcurrency <= 0 {
		return errors.New("semantic rule limits must be non-negative and concurrency/batch size positive")
	}
	if s.Maintenance.Timeout.Duration <= 0 || s.Maintenance.PollInterval.Duration <= 0 || s.Maintenance.PollInterval.Duration > s.Maintenance.Timeout.Duration {
		return errors.New("semantic maintenance timeout and pollInterval must be positive and bounded")
	}
	if s.Maintenance.AnalyzeLimit <= 0 || s.Maintenance.ProcessLimit <= 0 || s.Maintenance.AnalyzeLimit > 4096 || s.Maintenance.ProcessLimit > 4096 {
		return errors.New("semantic maintenance limits must be between 1 and 4096")
	}
	return nil
}

type EnvironmentSpec struct {
	Driver        string                 `yaml:"driver" json:"driver"`
	Namespace     string                 `yaml:"namespace,omitempty" json:"namespace,omitempty"`
	KeepOnFailure bool                   `yaml:"keepOnFailure,omitempty" json:"keepOnFailure,omitempty"`
	Options       map[string]any         `yaml:"options,omitempty" json:"options,omitempty"`
	Capabilities  CapabilityRequirements `yaml:"capabilities,omitempty" json:"capabilities,omitempty"`
}

type CapabilityRequirements struct {
	Required []string `yaml:"required,omitempty" json:"required,omitempty"`
}

type ActorGroupSpec struct {
	Name       string         `yaml:"name" json:"name"`
	ProfileRef string         `yaml:"profileRef" json:"profileRef"`
	Count      int            `yaml:"count" json:"count"`
	Rate       RateSpec       `yaml:"rate" json:"rate"`
	Target     map[string]any `yaml:"target,omitempty" json:"target,omitempty"`
	Overrides  map[string]any `yaml:"overrides,omitempty" json:"overrides,omitempty"`
}

type RateSpec struct {
	Mode             string  `yaml:"mode,omitempty" json:"mode,omitempty"`
	CommitsPerSecond float64 `yaml:"commitsPerSecond,omitempty" json:"commitsPerSecond,omitempty"`
	QueriesPerSecond float64 `yaml:"queriesPerSecond,omitempty" json:"queriesPerSecond,omitempty"`
}

type PhaseSpec struct {
	Name                string                   `yaml:"name" json:"name"`
	Duration            Duration                 `yaml:"duration" json:"duration"`
	Events              []EventSpec              `yaml:"events,omitempty" json:"events,omitempty"`
	Outcome             map[string]string        `yaml:"outcome,omitempty" json:"outcome,omitempty"`
	ActorGroupOverrides map[string]ActorOverride `yaml:"actorGroupOverrides,omitempty" json:"actorGroupOverrides,omitempty"`
	Assertions          map[string]any           `yaml:"assertions,omitempty" json:"assertions,omitempty"`
}

type ActorOverride struct {
	Rate RateSpec `yaml:"rate,omitempty" json:"rate,omitempty"`
}

type EventSpec struct {
	Type     string         `yaml:"type" json:"type"`
	Target   map[string]any `yaml:"target" json:"target"`
	Duration Duration       `yaml:"duration,omitempty" json:"duration,omitempty"`
	Repeat   int            `yaml:"repeat,omitempty" json:"repeat,omitempty"`
	Interval Duration       `yaml:"interval,omitempty" json:"interval,omitempty"`
}

type Suite struct {
	TypeMeta  `yaml:",inline" json:",inline"`
	Metadata  Metadata        `yaml:"metadata" json:"metadata"`
	Scenarios []SuiteScenario `yaml:"scenarios" json:"scenarios"`
	Execution SuiteExecution  `yaml:"execution" json:"execution"`
}

type SuiteScenario struct {
	Path string `yaml:"path" json:"path"`
	Ref  string `yaml:"ref,omitempty" json:"ref,omitempty"`
}

type SuiteExecution struct {
	Mode          string `yaml:"mode" json:"mode"`
	StopOnFailure bool   `yaml:"stopOnFailure" json:"stopOnFailure"`
}

type ResolvedScenario struct {
	TypeMeta           `yaml:",inline" json:",inline"`
	Metadata           Metadata                        `yaml:"metadata" json:"metadata"`
	Seed               int64                           `yaml:"seed" json:"seed"`
	Environment        EnvironmentSpec                 `yaml:"environment" json:"environment"`
	ClusterRef         string                          `yaml:"clusterRef" json:"clusterRef"`
	Cluster            ClusterSpec                     `yaml:"cluster" json:"cluster"`
	ProviderProfileRef string                          `yaml:"providerProfileRef,omitempty" json:"providerProfileRef,omitempty"`
	ProviderProfile    *provider.ResolvedProfile       `yaml:"providerProfile,omitempty" json:"providerProfile,omitempty"`
	Semantic           *SemanticScenarioSpec           `yaml:"semantic,omitempty" json:"semantic,omitempty"`
	ProcedureInference *ProcedureInferenceScenarioSpec `yaml:"procedureInference,omitempty" json:"procedureInference,omitempty"`
	ActorGroups        []ResolvedActorGroup            `yaml:"actorGroups" json:"actorGroups"`
	Phases             []PhaseSpec                     `yaml:"phases" json:"phases"`
	Assertions         map[string]any                  `yaml:"assertions,omitempty" json:"assertions,omitempty"`
	Artifacts          map[string]bool                 `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
}

type ResolvedActorGroup struct {
	ActorGroupSpec `yaml:",inline" json:",inline"`
	Profile        ActorProfile `yaml:"profile" json:"profile"`
}

func LoadFile(path string) (any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Load(raw)
}

func Load(raw []byte) (any, error) {
	var tm TypeMeta
	if err := yaml.Unmarshal(raw, &tm); err != nil {
		return nil, err
	}
	switch tm.Kind {
	case KindClusterProfile:
		var out ClusterProfile
		if err := yaml.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		return out, out.Validate()
	case KindActorProfile:
		var out ActorProfile
		if err := yaml.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		return out, out.Validate()
	case KindScenario:
		var out Scenario
		if err := yaml.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		DefaultScenario(&out)
		return out, out.Validate()
	case KindSuite:
		var out Suite
		if err := yaml.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		DefaultSuite(&out)
		return out, out.Validate()
	default:
		return nil, fmt.Errorf("unsupported kind %q", tm.Kind)
	}
}

func MarshalYAML(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

func DecodeYAML(raw []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(false)
	return dec.Decode(out)
}

func (m TypeMeta) validate(kind string) error {
	if m.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if m.Kind != kind {
		return fmt.Errorf("kind must be %q", kind)
	}
	return nil
}

func validateMetadata(m Metadata) error {
	if m.Name == "" {
		return errors.New("metadata.name is required")
	}
	return nil
}

func (p ClusterProfile) Validate() error {
	if err := p.TypeMeta.validate(KindClusterProfile); err != nil {
		return err
	}
	if err := validateMetadata(p.Metadata); err != nil {
		return err
	}
	return p.Cluster.Validate()
}

func (c ClusterSpec) Validate() error {
	if c.Nodes <= 0 {
		return errors.New("cluster.nodes must be positive")
	}
	if c.Image == "" {
		return errors.New("cluster.image is required")
	}
	if c.Raft.NodeCount <= 0 {
		return errors.New("cluster.raft.nodeCount must be positive")
	}
	if c.Raft.PartitionCount <= 0 {
		return errors.New("cluster.raft.partitionCount must be positive")
	}
	if c.Raft.ReplicaFactor <= 0 {
		return errors.New("cluster.raft.replicaFactor must be positive")
	}
	if c.Raft.ReplicaFactor > c.Raft.NodeCount {
		return errors.New("cluster.raft.replicaFactor cannot exceed nodeCount")
	}
	return nil
}

func (p ActorProfile) Validate() error {
	if err := p.TypeMeta.validate(KindActorProfile); err != nil {
		return err
	}
	if err := validateMetadata(p.Metadata); err != nil {
		return err
	}
	if len(p.Behavior) == 0 {
		return errors.New("behavior is required")
	}
	if typ, _ := p.Behavior["type"].(string); typ == "" {
		return errors.New("behavior.type is required")
	}
	return nil
}

func NormalizeActorGroupResourceName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func DefaultScenario(s *Scenario) {
	if s.Environment.Driver == "" {
		s.Environment.Driver = "k3d"
	}
	for i := range s.ActorGroups {
		if s.ActorGroups[i].Rate.Mode == "" {
			s.ActorGroups[i].Rate.Mode = "group-total"
		}
	}
	s.Semantic.setDefaults()
	s.ProcedureInference.setDefaults()
	for i := range s.Phases {
		for name, override := range s.Phases[i].ActorGroupOverrides {
			if override.Rate.Mode == "" && (override.Rate.CommitsPerSecond != 0 || override.Rate.QueriesPerSecond != 0) {
				override.Rate.Mode = "group-total"
				s.Phases[i].ActorGroupOverrides[name] = override
			}
		}
	}
}

func (s Scenario) Validate() error {
	if err := s.TypeMeta.validate(KindScenario); err != nil {
		return err
	}
	if err := validateMetadata(s.Metadata); err != nil {
		return err
	}
	if s.Seed == 0 {
		return errors.New("seed is required")
	}
	if s.ClusterRef == "" {
		return errors.New("clusterRef is required")
	}
	if err := validateEnvironmentSpec(s.Environment); err != nil {
		return err
	}
	if len(s.ActorGroups) == 0 {
		return errors.New("actorGroups must not be empty")
	}
	seenActors := map[string]bool{}
	seenActorResourceNames := map[string]string{}
	for _, group := range s.ActorGroups {
		if strings.TrimSpace(group.Name) == "" {
			return errors.New("actorGroups[].name is required")
		}
		if seenActors[group.Name] {
			return fmt.Errorf("duplicate actor group %q", group.Name)
		}
		seenActors[group.Name] = true
		resourceName := NormalizeActorGroupResourceName(group.Name)
		if resourceName == "" {
			return fmt.Errorf("actor group %q does not contain a resource-safe letter or digit", group.Name)
		}
		if existing, ok := seenActorResourceNames[resourceName]; ok {
			return fmt.Errorf("actor groups %q and %q normalize to the same resource name %q", existing, group.Name, resourceName)
		}
		seenActorResourceNames[resourceName] = group.Name
		if group.ProfileRef == "" {
			return fmt.Errorf("actor group %q profileRef is required", group.Name)
		}
		if group.Count <= 0 {
			return fmt.Errorf("actor group %q count must be positive", group.Name)
		}
		if group.Rate.Mode == "" {
			return fmt.Errorf("actor group %q rate.mode is required", group.Name)
		}
		if group.Rate.CommitsPerSecond == 0 && group.Rate.QueriesPerSecond == 0 {
			return fmt.Errorf("actor group %q rate must specify commitsPerSecond or queriesPerSecond", group.Name)
		}
	}
	if s.Semantic != nil && s.ProcedureInference != nil {
		return errors.New("semantic and procedureInference contracts must use separate scenarios")
	}
	if s.Semantic != nil {
		if err := s.Semantic.Validate(seenActors, s.ProviderProfileRef); err != nil {
			return err
		}
	}
	if s.ProcedureInference != nil {
		if err := s.ProcedureInference.Validate(seenActors, s.ProviderProfileRef); err != nil {
			return err
		}
	}
	if len(s.Phases) == 0 {
		return errors.New("phases must not be empty")
	}
	seenPhases := map[string]bool{}
	for _, phase := range s.Phases {
		if phase.Name == "" {
			return errors.New("phases[].name is required")
		}
		if seenPhases[phase.Name] {
			return fmt.Errorf("duplicate phase %q", phase.Name)
		}
		seenPhases[phase.Name] = true
		if phase.Duration.Duration <= 0 {
			return fmt.Errorf("phase %q duration must be positive", phase.Name)
		}
		for _, event := range phase.Events {
			if event.Type == "" {
				return fmt.Errorf("phase %q event type is required", phase.Name)
			}
			if !isSupportedEventType(event.Type) {
				return fmt.Errorf("phase %q event type %q is not supported", phase.Name, event.Type)
			}
			if len(event.Target) == 0 {
				return fmt.Errorf("phase %q event %q target is required", phase.Name, event.Type)
			}
			if strings.HasPrefix(event.Type, "semantic-") {
				if err := ValidateSemanticEventTarget(event.Type, event.Target); err != nil {
					return fmt.Errorf("phase %q: %w", phase.Name, err)
				}
			}
			if strings.HasPrefix(event.Type, "procedure-inference-") {
				if err := ValidateProcedureInferenceEventTarget(event.Type, event.Target); err != nil {
					return fmt.Errorf("phase %q: %w", phase.Name, err)
				}
			}
		}
	}
	return nil
}

func validateEnvironmentSpec(environment EnvironmentSpec) error {
	switch environment.Driver {
	case "", "dry-run", "k3d", "compose":
	default:
		return fmt.Errorf("environment.driver %q is not supported", environment.Driver)
	}
	for _, capability := range environment.Capabilities.Required {
		if strings.TrimSpace(capability) == "" {
			return errors.New("environment.capabilities.required contains an empty capability")
		}
	}
	return nil
}

func isSupportedEventType(eventType string) bool {
	switch eventType {
	case "pod-stop", "pod-restart", "pod-delete", "node-restart", "node-stop", "rolling-restart", "statefulset-image-upgrade", "commonfolio-automation-seed", "commonfolio-automation-await-terminal", "commonfolio-automation-diagnostics", "cluster-recovery-wait", "log-assert-absent", "environment-reset", "host-command", "user-backup-fixture", "user-backup-export", "user-backup-validate", "user-backup-import", "user-backup-verify-restored", "user-backup-assert-safety", "system-backup-fixture", "cluster-backup-create", "cluster-backup-validate", "cluster-restore-apply", "cluster-restore-verify", "cluster-backup-cleanup", "raft-snapshot-create", "raft-pvc-replace-node", "raft-snapshot-verify-rejoined", "identity-duplicate-principal-replay", "procedure-inference-setup", "procedure-inference-node-create", "procedure-inference-await-terminal", "procedure-inference-control", "procedure-inference-capacity-diagnostics-assert", "procedure-inference-emulator-assert", "semantic-node-create", "semantic-node-update", "semantic-node-delete", "semantic-transaction-negative", "semantic-rule-create", "semantic-backfill", "semantic-maintenance-await", "semantic-search-assert", "semantic-unauthorized-search", "semantic-rule-enable", "semantic-rule-projection-update", "semantic-reliability-assert", "semantic-rebuild", "semantic-provider-budget-reconcile":
		return true
	default:
		return false
	}
}

func DefaultSuite(s *Suite) {
	if s.Execution.Mode == "" {
		s.Execution.Mode = "sequential"
	}
}

func (s Suite) Validate() error {
	if err := s.TypeMeta.validate(KindSuite); err != nil {
		return err
	}
	if err := validateMetadata(s.Metadata); err != nil {
		return err
	}
	if len(s.Scenarios) == 0 {
		return errors.New("scenarios must not be empty")
	}
	if s.Execution.Mode != "sequential" {
		return errors.New("execution.mode must be sequential")
	}
	for i, scenario := range s.Scenarios {
		if scenario.Path == "" && scenario.Ref == "" {
			return fmt.Errorf("scenarios[%d] requires path or ref", i)
		}
	}
	return nil
}
