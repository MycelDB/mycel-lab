package events

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

type LogAssertAbsentResult struct {
	Resource string   `json:"resource"`
	Tail     int      `json:"tail"`
	Patterns []string `json:"patterns"`
	Matches  []string `json:"matches,omitempty"`
	Stdout   string   `json:"stdout,omitempty"`
	Stderr   string   `json:"stderr,omitempty"`
}

func executeLogAssertAbsentEvent(ctx context.Context, environment env.Environment, target map[string]any) (LogAssertAbsentResult, error) {
	resource := firstNonEmpty(stringTarget(target, "resource"), "statefulset/myceld")
	tail := intTarget(target, "tail", 1000)
	patterns := stringListTarget(target, "patterns")
	if len(patterns) == 0 {
		patterns = []string{"stale automation invocation update", "handler=graph: graph conflict", "restore raft snapshot and replay committed entries"}
	}
	args := []string{}
	if environment.Context != "" {
		args = append(args, "--context", environment.Context)
	}
	if environment.Namespace != "" {
		args = append(args, "-n", environment.Namespace)
	}
	args = append(args, "logs", resource, "--all-containers=true", fmt.Sprintf("--tail=%d", tail))
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	text := string(out)
	result := LogAssertAbsentResult{Resource: resource, Tail: tail, Patterns: patterns, Stdout: bounded(text, 8192)}
	if err != nil {
		result.Stderr = err.Error()
		return result, fmt.Errorf("kubectl logs %s: %w", resource, err)
	}
	lower := strings.ToLower(text)
	for _, pattern := range patterns {
		if strings.Contains(lower, strings.ToLower(pattern)) {
			result.Matches = append(result.Matches, pattern)
		}
	}
	if len(result.Matches) > 0 {
		return result, fmt.Errorf("logs for %s contain forbidden replay failure pattern(s): %s", resource, strings.Join(result.Matches, ", "))
	}
	return result, nil
}

func stringListTarget(target map[string]any, key string) []string {
	if target == nil {
		return nil
	}
	switch value := target[key].(type) {
	case []string:
		out := append([]string(nil), value...)
		return trimNonEmptyStrings(out)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if strings.TrimSpace(value) == "" {
			return nil
		}
		parts := strings.Split(value, ",")
		return trimNonEmptyStrings(parts)
	default:
		return nil
	}
}

func trimNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			out = append(out, text)
		}
	}
	return out
}
