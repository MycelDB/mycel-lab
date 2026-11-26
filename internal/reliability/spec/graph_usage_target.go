package spec

import (
	"fmt"
	"math"
	"strings"
)

const (
	GraphUsageLegacy   = "legacy"
	GraphUsagePending  = "pending"
	GraphUsageBackfill = "backfill"
	GraphUsageExact    = "exact"
)

// GraphUsageTarget describes one checkpoint in a finite graph usage scenario.
// Each entry in PayloadBytes corresponds to one live node's payload contribution.
type GraphUsageTarget struct {
	ActorGroup   string   `json:"actorGroup"`
	Mode         string   `json:"mode"`
	PayloadBytes []uint64 `json:"expectedPayloadBytes"`
	LogicalBytes uint64   `json:"expectedLogicalBytes"`
	Diagnostic   bool     `json:"diagnostic"`
}

func ParseGraphUsageTarget(target map[string]any) (GraphUsageTarget, error) {
	var out GraphUsageTarget
	for key := range target {
		switch key {
		case "actorGroup", "mode", "expectedPayloadBytes", "diagnostic":
		default:
			return out, fmt.Errorf("graph-usage-check: unknown target.%s", key)
		}
	}
	actor, _ := target["actorGroup"].(string)
	out.ActorGroup = strings.TrimSpace(actor)
	if out.ActorGroup == "" {
		return out, fmt.Errorf("graph-usage-check requires target.actorGroup")
	}
	out.Mode, _ = target["mode"].(string)
	switch out.Mode {
	case GraphUsageLegacy, GraphUsagePending, GraphUsageBackfill, GraphUsageExact:
	default:
		return out, fmt.Errorf("graph-usage-check target.mode must be legacy, pending, backfill, or exact")
	}
	if raw, ok := target["diagnostic"]; ok {
		var valid bool
		out.Diagnostic, valid = raw.(bool)
		if !valid || (out.Diagnostic && out.Mode != GraphUsageExact) {
			return out, fmt.Errorf("graph-usage-check target.diagnostic must be boolean and can only be enabled in exact mode")
		}
	}
	contributions, _ := target["expectedPayloadBytes"].([]any)
	if len(contributions) == 0 || len(contributions) > 100 {
		return out, fmt.Errorf("graph-usage-check requires 1–100 target.expectedPayloadBytes entries")
	}
	for _, raw := range contributions {
		value, ok := semanticInt64(raw)
		if !ok || value < 0 || value > math.MaxInt32 {
			return out, fmt.Errorf("graph-usage-check target.expectedPayloadBytes entries must be nonnegative integers at most %d", math.MaxInt32)
		}
		out.PayloadBytes = append(out.PayloadBytes, uint64(value))
		out.LogicalBytes += uint64(value)
	}
	return out, nil
}
