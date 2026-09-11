package actors

import (
	"context"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestGroupTotalRateDividesAcrossInstances(t *testing.T) {
	rates := DistributeRate(spec.RateSpec{Mode: "group-total", CommitsPerSecond: 9, QueriesPerSecond: 6}, 3)
	if len(rates) != 3 {
		t.Fatalf("rates len=%d, want 3", len(rates))
	}
	for _, rate := range rates {
		if rate.CommitsPerSecond != 3 || rate.QueriesPerSecond != 2 {
			t.Fatalf("rate=%+v, want commits=3 queries=2", rate)
		}
	}
}

func TestInstanceSeedIsStable(t *testing.T) {
	first := InstanceSeed(123, "writers", 7)
	second := InstanceSeed(123, "writers", 7)
	third := InstanceSeed(123, "writers", 8)
	if first != second {
		t.Fatalf("seed is not stable: %d != %d", first, second)
	}
	if first == third {
		t.Fatalf("different instance produced same seed %d", first)
	}
}

func TestPhaseRateOverridesApplyAndRestore(t *testing.T) {
	scenario := spec.ResolvedScenario{Seed: 1, ActorGroups: []spec.ResolvedActorGroup{{ActorGroupSpec: spec.ActorGroupSpec{Name: "writers", Count: 2, Rate: spec.RateSpec{Mode: "group-total", CommitsPerSecond: 10}}}}}
	scheduler := NewScheduler(scenario, nil)
	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	for _, inst := range scheduler.Instances() {
		if inst.Rate.CommitsPerSecond != 5 {
			t.Fatalf("base rate=%+v, want 5 cps", inst.Rate)
		}
	}
	phase := spec.PhaseSpec{Name: "slow", ActorGroupOverrides: map[string]spec.ActorOverride{"writers": {Rate: spec.RateSpec{Mode: "group-total", CommitsPerSecond: 2}}}}
	if err := scheduler.ApplyPhase(context.Background(), phase); err != nil {
		t.Fatalf("ApplyPhase() error = %v", err)
	}
	for _, inst := range scheduler.Instances() {
		if inst.Rate.CommitsPerSecond != 1 {
			t.Fatalf("override rate=%+v, want 1 cps", inst.Rate)
		}
	}
	if err := scheduler.RestoreBaseRates(context.Background()); err != nil {
		t.Fatalf("RestoreBaseRates() error = %v", err)
	}
	for _, inst := range scheduler.Instances() {
		if inst.Rate.CommitsPerSecond != 5 {
			t.Fatalf("restored rate=%+v, want 5 cps", inst.Rate)
		}
	}
}
