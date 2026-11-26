package spec

import (
	"reflect"
	"testing"
)

func TestParseGraphUsageTarget(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"actorGroup": "fixture", "mode": GraphUsagePending, "expectedPayloadBytes": []any{1, int64(14), float64(3)}}
	}
	got, err := ParseGraphUsageTarget(valid())
	if err != nil || got.LogicalBytes != 18 || !reflect.DeepEqual(got.PayloadBytes, []uint64{1, 14, 3}) {
		t.Fatalf("ParseGraphUsageTarget()=(%+v, %v), want 18 bytes with three contributions", got, err)
	}
	for _, tc := range []struct {
		key string
		val any
	}{
		{"actorGroup", " "}, {"mode", "auto-reconcile"}, {"expectedPayloadBytes", []any{}},
		{"expectedPayloadBytes", []any{-1}}, {"expectedPayloadBytes", []any{2.5}},
		{"expectedPayloadBytes", []any{"14"}}, {"diagnostic", true}, {"includeReconciliation", true},
	} {
		target := valid()
		target[tc.key] = tc.val
		if _, err := ParseGraphUsageTarget(target); err == nil {
			t.Fatalf("accepted invalid %s=%v", tc.key, tc.val)
		}
	}
}

func TestGraphUsageUpgradeScenarioCheckpoints(t *testing.T) {
	loaded, err := LoadFile("../../../tests/reliability/scenarios/k3d-graph-usage-upgrade/k3d-graph-usage-upgrade.yaml")
	if err != nil {
		t.Fatal(err)
	}
	scenario := loaded.(Scenario)
	var sequence []string
	var totals []uint64
	for _, phase := range scenario.Phases {
		for _, event := range phase.Events {
			if event.Type != "graph-usage-check" {
				sequence = append(sequence, event.Type)
				continue
			}
			target, err := ParseGraphUsageTarget(event.Target)
			if err != nil {
				t.Fatal(err)
			}
			sequence = append(sequence, target.Mode)
			totals = append(totals, target.LogicalBytes)
		}
	}
	want := []string{
		"semantic-node-create", "semantic-node-create", GraphUsageLegacy,
		"statefulset-image-upgrade", "cluster-recovery-wait", "semantic-node-create", "semantic-node-update", "cluster-recovery-wait", GraphUsagePending, GraphUsageBackfill,
		"semantic-node-update", GraphUsageExact, "rolling-restart", "cluster-recovery-wait", GraphUsageExact, "semantic-node-delete", GraphUsageExact,
	}
	if !reflect.DeepEqual(sequence, want) || !reflect.DeepEqual(totals, []uint64{20, 18, 18, 22, 22, 19}) {
		t.Fatalf("checkpoints=%v totals=%v, want legacy → writes/pending → backfill → update → restart → delete", sequence, totals)
	}
}
