package events

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type FailureClass string

const (
	FailureTransient   FailureClass = "transient"
	FailureUnavailable FailureClass = "unavailable"
	FailurePermanent   FailureClass = "permanent"
	FailureCorrectness FailureClass = "correctness"
)

type Record struct {
	Time      time.Time      `json:"time"`
	Type      string         `json:"type"`
	PhaseName string         `json:"phaseName"`
	EventType string         `json:"eventType"`
	Target    map[string]any `json:"target,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type Recorder interface {
	RecordRuntimeEvent(ctx context.Context, record Record) error
}

type Runtime interface {
	ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error
}

type LocalRuntime struct{}

func (LocalRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error {
	for _, event := range phase.Events {
		if err := executeOne(ctx, phase, event, dryRun, recorder); err != nil {
			return err
		}
	}
	return nil
}

func executeOne(ctx context.Context, phase spec.PhaseSpec, event spec.EventSpec, dryRun bool, recorder Recorder) error {
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"dryRun": dryRun}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		if recorder != nil {
			_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"error": err.Error()}})
		}
		return err
	}
	if !dryRun && event.Duration.Duration > 0 {
		timer := time.NewTimer(event.Duration.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if recorder != nil {
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target})
	}
	return nil
}

func validateEvent(event spec.EventSpec) error {
	switch event.Type {
	case "pod-stop", "pod-restart", "pod-delete":
		if PodName(event.Target) == "" {
			return fmt.Errorf("%s requires target.pod", event.Type)
		}
	case "node-stop", "node-restart":
		if PodName(event.Target) == "" && nodeTargetName(event.Target) == "" && nodeTargetOrdinal(event.Target) < 0 && len(nodeTargetOrdinalSequence(event.Target)) == 0 {
			return fmt.Errorf("%s requires target.node, target.service, target.pod, target.ordinal, or target.ordinalSequence", event.Type)
		}
	case "rolling-restart":
		return nil
	case "host-command":
		if len(hostCommandTarget(event.Target)) == 0 {
			return fmt.Errorf("%s requires target.command", event.Type)
		}
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}
	return nil
}

func PodName(target map[string]any) string {
	pod, _ := target["pod"].(string)
	return pod
}

func nodeTargetName(target map[string]any) string {
	if node, _ := target["node"].(string); node != "" {
		return node
	}
	service, _ := target["service"].(string)
	return service
}

func nodeTargetOrdinal(target map[string]any) int {
	for _, key := range []string{"ordinal", "nodeOrdinal"} {
		switch v := target[key].(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return -1
}

func nodeTargetOrdinalSequence(target map[string]any) []int {
	if target == nil {
		return nil
	}
	value, ok := target["ordinalSequence"]
	if !ok {
		value = target["ordinals"]
	}
	switch v := value.(type) {
	case []int:
		return append([]int(nil), v...)
	case []any:
		out := make([]int, 0, len(v))
		for _, item := range v {
			switch n := item.(type) {
			case int:
				out = append(out, n)
			case int64:
				out = append(out, int(n))
			case float64:
				out = append(out, int(n))
			}
		}
		return out
	default:
		return nil
	}
}

func hostCommandTarget(target map[string]any) []string {
	if target == nil {
		return nil
	}
	switch value := target["command"].(type) {
	case []string:
		return append([]string(nil), value...)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			part := strings.TrimSpace(fmt.Sprint(item))
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	case string:
		if strings.TrimSpace(value) == "" {
			return nil
		}
		return []string{value}
	default:
		return nil
	}
}

type HostCommandResult struct {
	Command          string `json:"command"`
	WorkingDirectory string `json:"workingDirectory,omitempty"`
	Stdout           string `json:"stdout,omitempty"`
	Stderr           string `json:"stderr,omitempty"`
	Truncated        bool   `json:"truncated,omitempty"`
}

func ExecuteHostCommand(ctx context.Context, target map[string]any) (HostCommandResult, error) {
	command := hostCommandTarget(target)
	if len(command) == 0 {
		return HostCommandResult{}, fmt.Errorf("host-command requires target.command")
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	if cwd, _ := target["workingDirectory"].(string); strings.TrimSpace(cwd) != "" {
		cmd.Dir = cwd
	}
	if envMap, ok := target["env"].(map[string]any); ok && len(envMap) > 0 {
		env := os.Environ()
		for key, value := range envMap {
			if strings.TrimSpace(key) != "" {
				env = append(env, key+"="+fmt.Sprint(value))
			}
		}
		cmd.Env = env
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	result := HostCommandResult{Command: strings.Join(command, " "), WorkingDirectory: cmd.Dir}
	err := cmd.Run()
	result.Stdout, result.Stderr, result.Truncated = boundedOutput(stdout.String(), stderr.String(), 8192)
	if err != nil {
		return result, fmt.Errorf("host-command %s: %w", result.Command, err)
	}
	return result, nil
}

func boundedOutput(stdout, stderr string, limit int) (string, string, bool) {
	truncated := false
	if len(stdout) > limit {
		stdout = stdout[len(stdout)-limit:]
		truncated = true
	}
	if len(stderr) > limit {
		stderr = stderr[len(stderr)-limit:]
		truncated = true
	}
	return stdout, stderr, truncated
}

func IsExpectedDegradation(phase spec.PhaseSpec) bool {
	return phase.Outcome["availability"] == "expected-degradation"
}

func FailureIsMaskedByExpectedDegradation(phase spec.PhaseSpec, class FailureClass) bool {
	if !IsExpectedDegradation(phase) {
		return false
	}
	return class == FailureTransient || class == FailureUnavailable
}

func CorrectnessFailureFailsScenario(phase spec.PhaseSpec) bool {
	return !FailureIsMaskedByExpectedDegradation(phase, FailureCorrectness)
}

func ArtifactEvent(record Record) artifacts.Event {
	return artifacts.Event{Time: record.Time, Type: record.Type, PhaseName: record.PhaseName, Payload: map[string]any{"eventType": record.EventType, "target": record.Target, "payload": record.Payload}}
}
