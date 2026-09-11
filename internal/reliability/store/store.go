package store

import (
	"context"
	"time"
)

type DefinitionKind string

const (
	KindClusterProfile DefinitionKind = "cluster-profile"
	KindActorProfile   DefinitionKind = "actor-profile"
	KindScenario       DefinitionKind = "scenario"
	KindSuite          DefinitionKind = "suite"
)

type Definition struct {
	ID             int64
	Kind           DefinitionKind
	Name           string
	Version        int
	SpecHash       string
	SpecJSON       []byte
	ResolvedJSON   []byte
	UsedByRunCount int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (d Definition) CanEdit() bool   { return d.UsedByRunCount == 0 }
func (d Definition) CanDelete() bool { return d.UsedByRunCount == 0 }

type UpsertStatus string

const (
	UpsertCreated UpsertStatus = "created"
	UpsertUpdated UpsertStatus = "updated"
	UpsertNoop    UpsertStatus = "noop"
)

type UpsertResult struct {
	Definition Definition
	Status     UpsertStatus
}

type Run struct {
	ID              string
	ScenarioName    string
	ScenarioVersion int
	Status          string
	Seed            int64
	ResolvedJSON    []byte
	StartedAt       time.Time
	FinishedAt      *time.Time
	CreatedAt       time.Time
}

type RunActorProfile struct {
	ActorGroupName      string
	ActorProfileName    string
	ActorProfileVersion int
	SpecSnapshotJSON    []byte
}

type RunPhase struct {
	Name             string
	Status           string
	SpecSnapshotJSON []byte
}

type RunEvent struct {
	RunID     string
	PhaseName string
	EventType string
	ActorID   string
	Payload   []byte
}

type RunMetric struct {
	RunID  string
	Name   string
	Value  float64
	Labels []byte
}

type RunArtifact struct {
	RunID     string
	Type      string
	Path      string
	MediaType string
	SizeBytes *int64
	Metadata  []byte
}

type RunDetails struct {
	Run       Run
	Events    []RunEvent
	Metrics   []RunMetric
	Artifacts []RunArtifact
}

type Store interface {
	UpsertDefinition(ctx context.Context, def Definition) (UpsertResult, error)
	ListDefinitions(ctx context.Context, kind DefinitionKind) ([]Definition, error)
	GetDefinition(ctx context.Context, kind DefinitionKind, name string, version int) (Definition, error)
	LatestDefinition(ctx context.Context, kind DefinitionKind, name string) (Definition, error)
	DeleteDefinition(ctx context.Context, kind DefinitionKind, name string, version int) error
	MarkDefinitionUsed(ctx context.Context, kind DefinitionKind, name string, version int) error
	CreateRun(ctx context.Context, run Run, actors []RunActorProfile, phases []RunPhase) error
	FinishRun(ctx context.Context, runID string, status string) error
	ListRuns(ctx context.Context) ([]Run, error)
	GetRun(ctx context.Context, runID string) (RunDetails, error)
	AppendEvent(ctx context.Context, event RunEvent) error
	AppendMetric(ctx context.Context, metric RunMetric) error
	AppendArtifact(ctx context.Context, artifact RunArtifact) error
	Close() error
}
