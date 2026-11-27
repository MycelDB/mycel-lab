package spec

import (
	"fmt"
	"time"
)

const SemanticScalingEvent = "semantic-search-scaling"

const (
	ScaleIndexed    = "indexed"
	ScaleGraph      = "graph-only"
	ScalingTopK     = 10
	ScalingAxes     = 4
	ScalingMinScore = 0.5
)

type SemanticScalingTarget struct {
	Sweep           string   `json:"sweep"`
	Sizes           []int    `json:"sizes"`
	Samples         int      `json:"samples"`
	Timeout         Duration `json:"timeout"`
	SetupTimeout    Duration `json:"setupTimeout"`
	WorkloadTimeout Duration `json:"workloadTimeout"`
	RequestTimeout  Duration `json:"requestTimeout"`
}

// ParseSemanticScalingTarget bounds fixture size and the number and duration
// of requests. The fixture itself is generated in Lab, never supplied as input.
func ParseSemanticScalingTarget(target map[string]any) (SemanticScalingTarget, error) {
	invalid := func(field string) (SemanticScalingTarget, error) {
		return SemanticScalingTarget{}, fmt.Errorf("%s: invalid or missing %s", SemanticScalingEvent, field)
	}
	for key := range target {
		switch key {
		case "sweep", "sizes", "samples", "timeout", "setupTimeout", "workloadTimeout", "requestTimeout":
		default:
			return invalid("target field " + key)
		}
	}
	var out SemanticScalingTarget
	out.Sweep, _ = target["sweep"].(string)
	if out.Sweep != ScaleIndexed && out.Sweep != ScaleGraph {
		return invalid("sweep")
	}
	var sizes []any
	switch v := target["sizes"].(type) {
	case []any:
		sizes = v
	case []int:
		for _, n := range v {
			sizes = append(sizes, n)
		}
	}
	if len(sizes) < 2 || len(sizes) > 6 {
		return invalid("sizes (2–6 increasing counts)")
	}
	var previous int64
	for _, raw := range sizes {
		n, ok := semanticInt64(raw)
		if !ok || n < ScalingTopK*ScalingAxes || n > 30000 || n <= previous {
			return invalid("sizes (increasing integers between 40 and 30000)")
		}
		out.Sizes = append(out.Sizes, int(n))
		previous = n
	}
	if out.Sizes[0] > 4096 {
		return invalid("first size (must fit the 4096-node pinned backfill limit)")
	}
	samples, ok := semanticInt64(target["samples"])
	// Each axis must have equal representation on each of eight clients.
	if !ok || samples < 32 || samples > 8192 || samples%32 != 0 {
		return invalid("samples (a multiple of 32 between 32 and 8192)")
	}
	out.Samples = int(samples)
	for _, field := range []struct {
		key   string
		dest  *Duration
		limit time.Duration
	}{
		{"timeout", &out.Timeout, 2 * time.Hour},
		{"setupTimeout", &out.SetupTimeout, 20 * time.Minute},
		{"workloadTimeout", &out.WorkloadTimeout, 10 * time.Minute},
		{"requestTimeout", &out.RequestTimeout, time.Minute},
	} {
		raw, _ := target[field.key].(string)
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 || d > field.limit {
			return invalid(field.key)
		}
		field.dest.Duration = d
	}
	if out.RequestTimeout.Duration >= out.WorkloadTimeout.Duration ||
		time.Duration(len(out.Sizes))*(out.SetupTimeout.Duration+2*out.WorkloadTimeout.Duration) >= out.Timeout.Duration {
		return invalid("nested timeout budgets (leave time for overhead)")
	}
	return out, nil
}
