package spec

import (
	"maps"
	"testing"
)

func TestSemanticScalingTargetBounds(t *testing.T) {
	base := map[string]any{"sweep": ScaleGraph, "sizes": []any{1000, 3000, 10000, 30000}, "samples": 1024, "timeout": "75m", "setupTimeout": "12m", "workloadTimeout": "3m", "requestTimeout": "10s"}
	if target, err := ParseSemanticScalingTarget(base); err != nil || len(target.Sizes) != 4 {
		t.Fatalf("target=%+v err=%v", target, err)
	}
	for field, value := range map[string]any{
		"sweep": "both", "sizes": []any{5000, 4000}, "samples": 1000, "timeout": "1h", "setupTimeout": "1h", "requestTimeout": "90s", "unknown": true,
	} {
		t.Run(field, func(t *testing.T) {
			candidate := maps.Clone(base)
			candidate[field] = value
			if _, err := ParseSemanticScalingTarget(candidate); err == nil {
				t.Fatalf("accepted invalid %s", field)
			}
		})
	}
	for _, sizes := range [][]int{{40}, {39, 40}, {1000, 1000}, {5000, 10000}, {1000, 30001}} {
		candidate := maps.Clone(base)
		candidate["sizes"] = sizes
		if _, err := ParseSemanticScalingTarget(candidate); err == nil {
			t.Fatalf("accepted invalid sizes %v", sizes)
		}
	}
}
