package events

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

type LogAssertAbsentResult struct {
	Resource string   `json:"resource,omitempty"`
	Selector string   `json:"selector,omitempty"`
	Tail     int      `json:"tail"`
	Since    string   `json:"since,omitempty"`
	Timeout  string   `json:"timeout"`
	Patterns []string `json:"patterns"`
	Matches  []string `json:"matches,omitempty"`
	Stdout   string   `json:"stdout,omitempty"`
	Stderr   string   `json:"stderr,omitempty"`
}

func executeLogAssertAbsentEvent(ctx context.Context, environment env.Environment, target map[string]any) (LogAssertAbsentResult, error) {
	selector := stringTarget(target, "selector")
	resource := stringTarget(target, "resource")
	if selector == "" && resource == "" {
		resource = "statefulset/myceld"
	}
	tail := intTarget(target, "tail", 1000)
	if tail == 0 || tail < -1 {
		return LogAssertAbsentResult{}, fmt.Errorf("log-assert-absent tail must be -1 or a positive line count")
	}
	since := stringTarget(target, "since")
	if since != "" {
		duration, err := time.ParseDuration(since)
		if err != nil || duration <= 0 {
			return LogAssertAbsentResult{}, fmt.Errorf("log-assert-absent since must be a positive duration, got %q", since)
		}
	}
	commandTimeout, err := targetDuration(target, "timeout")
	if err != nil {
		return LogAssertAbsentResult{}, err
	}
	if commandTimeout <= 0 {
		commandTimeout = 2 * time.Minute
	}
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
	args = append(args, "logs")
	logTarget := resource
	if selector != "" {
		args = append(args, "-l", selector, "--prefix=true")
		logTarget = "selector " + selector
	} else {
		args = append(args, resource)
	}
	args = append(args, "--all-containers=true", fmt.Sprintf("--tail=%d", tail))
	if since != "" {
		args = append(args, "--since="+since)
	}
	commandCtx, commandCancel := context.WithTimeout(ctx, commandTimeout)
	defer commandCancel()
	cmd := exec.CommandContext(commandCtx, "kubectl", args...)
	capture := newForbiddenLogCapture(patterns, 16384)
	cmd.Stdout = capture
	cmd.Stderr = capture
	err = cmd.Run()
	result := LogAssertAbsentResult{Resource: resource, Selector: selector, Tail: tail, Since: since, Timeout: commandTimeout.String(), Patterns: patterns, Matches: capture.matches(), Stdout: capture.output()}
	if err != nil {
		result.Stderr = err.Error()
		return result, fmt.Errorf("kubectl logs %s: %w", logTarget, err)
	}
	if len(result.Matches) > 0 {
		return result, fmt.Errorf("logs for %s contain forbidden replay failure pattern(s): %s", logTarget, strings.Join(result.Matches, ", "))
	}
	return result, nil
}

type forbiddenLogCapture struct {
	mu             sync.Mutex
	patterns       []string
	lowerPatterns  []string
	matched        []bool
	maxPatternSize int
	carry          string
	tail           []byte
	tailLimit      int
}

func newForbiddenLogCapture(patterns []string, tailLimit int) *forbiddenLogCapture {
	capture := &forbiddenLogCapture{patterns: append([]string(nil), patterns...), tailLimit: tailLimit}
	for _, pattern := range patterns {
		lower := strings.ToLower(pattern)
		capture.lowerPatterns = append(capture.lowerPatterns, lower)
		capture.matched = append(capture.matched, false)
		if len(lower) > capture.maxPatternSize {
			capture.maxPatternSize = len(lower)
		}
	}
	return capture
}

func (c *forbiddenLogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lower := c.carry + strings.ToLower(string(p))
	for i, pattern := range c.lowerPatterns {
		if !c.matched[i] && strings.Contains(lower, pattern) {
			c.matched[i] = true
		}
	}
	carrySize := c.maxPatternSize - 1
	if carrySize > len(lower) {
		carrySize = len(lower)
	}
	if carrySize > 0 {
		c.carry = lower[len(lower)-carrySize:]
	} else {
		c.carry = ""
	}
	if c.tailLimit > 0 {
		if len(p) >= c.tailLimit {
			c.tail = append(c.tail[:0], p[len(p)-c.tailLimit:]...)
		} else {
			c.tail = append(c.tail, p...)
			if overflow := len(c.tail) - c.tailLimit; overflow > 0 {
				copy(c.tail, c.tail[overflow:])
				c.tail = c.tail[:c.tailLimit]
			}
		}
	}
	return len(p), nil
}

func (c *forbiddenLogCapture) matches() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var matches []string
	for i, matched := range c.matched {
		if matched {
			matches = append(matches, c.patterns[i])
		}
	}
	return matches
}

func (c *forbiddenLogCapture) output() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return string(append([]byte(nil), c.tail...))
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
