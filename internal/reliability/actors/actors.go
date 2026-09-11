package actors

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type ClientConfig struct {
	Endpoint      string
	AdminUser     string
	AdminPassword string
}

type Actor interface {
	Start(ctx context.Context) error
	UpdateRate(ctx context.Context, rate spec.RateSpec) error
	Stop(ctx context.Context) error
}

type Instance struct {
	GroupName string
	Index     int
	ID        string
	Seed      int64
	Rate      spec.RateSpec
	Actor     Actor
}

type Scheduler struct {
	mu        sync.Mutex
	scenario  spec.ResolvedScenario
	instances []Instance
	factory   Factory
	recorder  EventRecorder
}

type Factory func(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec) Actor

func NewScheduler(scenario spec.ResolvedScenario, factory Factory) *Scheduler {
	return NewSchedulerWithRecorder(scenario, factory, nil)
}

func NewSchedulerWithRecorder(scenario spec.ResolvedScenario, factory Factory, recorder EventRecorder) *Scheduler {
	if factory == nil {
		factory = defaultFactory(recorder)
	}
	return &Scheduler{scenario: scenario, factory: factory, recorder: recorder}
}

func defaultFactory(recorder EventRecorder) Factory {
	return func(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec) Actor {
		if behaviorType, _ := group.Profile.Behavior["type"].(string); behaviorType == "graph-transaction" {
			actor, err := NewGraphActor(group, index, seed, rate, recorder)
			if err == nil {
				return actor
			}
		}
		return &NoopActor{GroupName: group.Name, Index: index, Seed: seed, Rate: rate}
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if len(s.instances) != 0 {
		s.mu.Unlock()
		return nil
	}
	for _, group := range s.scenario.ActorGroups {
		rates := DistributeRate(group.Rate, group.Count)
		for i := 0; i < group.Count; i++ {
			seed := InstanceSeed(s.scenario.Seed, group.Name, i)
			actor := s.factory(group, i, seed, rates[i])
			inst := Instance{GroupName: group.Name, Index: i, ID: fmt.Sprintf("%s-%d", group.Name, i), Seed: seed, Rate: rates[i], Actor: actor}
			s.instances = append(s.instances, inst)
		}
	}
	instances := append([]Instance(nil), s.instances...)
	s.mu.Unlock()
	for _, inst := range instances {
		if err := inst.Actor.Start(ctx); err != nil {
			return fmt.Errorf("start actor %s: %w", inst.ID, err)
		}
	}
	return nil
}

func (s *Scheduler) ApplyPhase(ctx context.Context, phase spec.PhaseSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for groupName, override := range phase.ActorGroupOverrides {
		group, ok := findGroup(s.scenario.ActorGroups, groupName)
		if !ok {
			return fmt.Errorf("phase %q overrides unknown actor group %q", phase.Name, groupName)
		}
		rate := group.Rate
		if override.Rate.Mode != "" || override.Rate.CommitsPerSecond != 0 || override.Rate.QueriesPerSecond != 0 {
			rate = override.Rate
			if rate.Mode == "" {
				rate.Mode = group.Rate.Mode
			}
		}
		distributed := DistributeRate(rate, group.Count)
		idx := 0
		for i := range s.instances {
			if s.instances[i].GroupName != groupName {
				continue
			}
			s.instances[i].Rate = distributed[idx]
			if err := s.instances[i].Actor.UpdateRate(ctx, distributed[idx]); err != nil {
				return fmt.Errorf("update actor %s: %w", s.instances[i].ID, err)
			}
			idx++
		}
	}
	return nil
}

func (s *Scheduler) RestoreBaseRates(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, group := range s.scenario.ActorGroups {
		distributed := DistributeRate(group.Rate, group.Count)
		idx := 0
		for i := range s.instances {
			if s.instances[i].GroupName != group.Name {
				continue
			}
			s.instances[i].Rate = distributed[idx]
			if err := s.instances[i].Actor.UpdateRate(ctx, distributed[idx]); err != nil {
				return fmt.Errorf("restore actor %s: %w", s.instances[i].ID, err)
			}
			idx++
		}
	}
	return nil
}

func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	instances := append([]Instance(nil), s.instances...)
	s.mu.Unlock()
	var firstErr error
	for _, inst := range instances {
		if err := inst.Actor.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("stop actor %s: %w", inst.ID, err)
		}
	}
	return firstErr
}

func (s *Scheduler) Instances() []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Instance(nil), s.instances...)
}

func DistributeRate(rate spec.RateSpec, count int) []spec.RateSpec {
	if count <= 0 {
		return nil
	}
	mode := rate.Mode
	if mode == "" {
		mode = "group-total"
	}
	out := make([]spec.RateSpec, count)
	for i := range out {
		out[i] = rate
		out[i].Mode = mode
		if mode == "group-total" {
			out[i].CommitsPerSecond = roundRate(rate.CommitsPerSecond / float64(count))
			out[i].QueriesPerSecond = roundRate(rate.QueriesPerSecond / float64(count))
		}
	}
	return out
}

func InstanceSeed(scenarioSeed int64, groupName string, index int) int64 {
	h := fnv.New64a()
	var seedBytes [8]byte
	binary.LittleEndian.PutUint64(seedBytes[:], uint64(scenarioSeed))
	_, _ = h.Write(seedBytes[:])
	_, _ = h.Write([]byte(groupName))
	binary.LittleEndian.PutUint64(seedBytes[:], uint64(index))
	_, _ = h.Write(seedBytes[:])
	return int64(h.Sum64() & math.MaxInt64)
}

func findGroup(groups []spec.ResolvedActorGroup, name string) (spec.ResolvedActorGroup, bool) {
	for _, group := range groups {
		if group.Name == name {
			return group, true
		}
	}
	return spec.ResolvedActorGroup{}, false
}

func roundRate(value float64) float64 {
	return math.Round(value*1000) / 1000
}

type NoopActor struct {
	GroupName string
	Index     int
	Seed      int64
	Rate      spec.RateSpec
	Started   bool
	Stopped   bool
	Updates   []spec.RateSpec
}

func (a *NoopActor) Start(context.Context) error {
	a.Started = true
	return nil
}

func (a *NoopActor) UpdateRate(_ context.Context, rate spec.RateSpec) error {
	a.Rate = rate
	a.Updates = append(a.Updates, rate)
	return nil
}

func (a *NoopActor) Stop(context.Context) error {
	a.Stopped = true
	return nil
}
