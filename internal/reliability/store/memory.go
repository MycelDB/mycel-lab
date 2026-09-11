package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type MemoryStore struct {
	mu        sync.Mutex
	nextID    int64
	defs      map[DefinitionKind]map[string][]Definition
	runs      map[string]Run
	events    []RunEvent
	metrics   []RunMetric
	artifacts []RunArtifact
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		nextID: 1,
		defs:   map[DefinitionKind]map[string][]Definition{},
		runs:   map[string]Run{},
	}
}

func (s *MemoryStore) Close() error { return nil }

func (s *MemoryStore) UpsertDefinition(_ context.Context, def Definition) (UpsertResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if def.Name == "" {
		return UpsertResult{}, errors.New("definition name is required")
	}
	byName := s.defsForKind(def.Kind)
	versions := byName[def.Name]
	now := time.Now().UTC()
	if len(versions) == 0 {
		def.ID = s.nextID
		s.nextID++
		def.Version = 1
		def.CreatedAt = now
		def.UpdatedAt = now
		byName[def.Name] = []Definition{def}
		return UpsertResult{Definition: def, Status: UpsertCreated}, nil
	}
	latest := versions[len(versions)-1]
	if latest.SpecHash == def.SpecHash {
		return UpsertResult{Definition: latest, Status: UpsertNoop}, nil
	}
	if latest.CanEdit() {
		latest.SpecHash = def.SpecHash
		latest.SpecJSON = cloneBytes(def.SpecJSON)
		latest.ResolvedJSON = cloneBytes(def.ResolvedJSON)
		latest.UpdatedAt = now
		versions[len(versions)-1] = latest
		byName[def.Name] = versions
		return UpsertResult{Definition: latest, Status: UpsertUpdated}, nil
	}
	def.ID = s.nextID
	s.nextID++
	def.Version = latest.Version + 1
	def.CreatedAt = now
	def.UpdatedAt = now
	byName[def.Name] = append(versions, def)
	return UpsertResult{Definition: def, Status: UpsertCreated}, nil
}

func (s *MemoryStore) ListDefinitions(_ context.Context, kind DefinitionKind) ([]Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byName := s.defs[kind]
	var out []Definition
	for _, versions := range byName {
		out = append(out, versions...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Version < out[j].Version
		}
		return out[i].Name < out[j].Name
	})
	return cloneDefinitions(out), nil
}

func (s *MemoryStore) GetDefinition(_ context.Context, kind DefinitionKind, name string, version int) (Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, def := range s.defs[kind][name] {
		if def.Version == version {
			return cloneDefinition(def), nil
		}
	}
	return Definition{}, ErrNotFound
}

func (s *MemoryStore) LatestDefinition(_ context.Context, kind DefinitionKind, name string) (Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.defs[kind][name]
	if len(versions) == 0 {
		return Definition{}, ErrNotFound
	}
	return cloneDefinition(versions[len(versions)-1]), nil
}

func (s *MemoryStore) DeleteDefinition(_ context.Context, kind DefinitionKind, name string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.defs[kind][name]
	for i, def := range versions {
		if def.Version != version {
			continue
		}
		if !def.CanDelete() {
			return ErrDefinitionInUse
		}
		versions = append(versions[:i], versions[i+1:]...)
		if len(versions) == 0 {
			delete(s.defs[kind], name)
		} else {
			s.defs[kind][name] = versions
		}
		return nil
	}
	return ErrNotFound
}

func (s *MemoryStore) MarkDefinitionUsed(_ context.Context, kind DefinitionKind, name string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.defs[kind][name]
	for i, def := range versions {
		if def.Version == version {
			def.UsedByRunCount++
			def.UpdatedAt = time.Now().UTC()
			versions[i] = def
			s.defs[kind][name] = versions
			return nil
		}
	}
	return ErrNotFound
}

func (s *MemoryStore) CreateRun(_ context.Context, run Run, _ []RunActorProfile, _ []RunPhase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run.ID == "" {
		return errors.New("run id is required")
	}
	if _, exists := s.runs[run.ID]; exists {
		return fmt.Errorf("run %q already exists", run.ID)
	}
	s.runs[run.ID] = run
	return nil
}

func (s *MemoryStore) AppendEvent(_ context.Context, event RunEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *MemoryStore) AppendMetric(_ context.Context, metric RunMetric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, metric)
	return nil
}

func (s *MemoryStore) AppendArtifact(_ context.Context, artifact RunArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts = append(s.artifacts, artifact)
	return nil
}

func (s *MemoryStore) defsForKind(kind DefinitionKind) map[string][]Definition {
	byName := s.defs[kind]
	if byName == nil {
		byName = map[string][]Definition{}
		s.defs[kind] = byName
	}
	return byName
}

func cloneDefinitions(in []Definition) []Definition {
	out := make([]Definition, len(in))
	for i, def := range in {
		out[i] = cloneDefinition(def)
	}
	return out
}

func cloneDefinition(def Definition) Definition {
	def.SpecJSON = cloneBytes(def.SpecJSON)
	def.ResolvedJSON = cloneBytes(def.ResolvedJSON)
	return def
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
