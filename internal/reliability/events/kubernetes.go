package events

import (
	"context"
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type KubernetesRuntime struct {
	Context   string
	Namespace string
	Runner    executil.Runner
	Timeout   time.Duration
}

func NewKubernetesRuntime(contextName, namespace string, runner executil.Runner) KubernetesRuntime {
	if namespace == "" {
		namespace = "mycel-lab"
	}
	if runner == nil {
		runner = executil.LocalRunner{}
	}
	return KubernetesRuntime{Context: contextName, Namespace: namespace, Runner: runner, Timeout: 5 * time.Minute}
}

func (r KubernetesRuntime) ExecutePhaseEvents(ctx context.Context, phase spec.PhaseSpec, dryRun bool, recorder Recorder) error {
	for _, event := range phase.Events {
		if err := r.executeOne(ctx, phase, event, dryRun, recorder); err != nil {
			return err
		}
	}
	return nil
}

func (r KubernetesRuntime) executeOne(ctx context.Context, phase spec.PhaseSpec, event spec.EventSpec, dryRun bool, recorder Recorder) error {
	if recorder != nil {
		if err := recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-started", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"dryRun": dryRun, "runtime": "kubernetes"}}); err != nil {
			return err
		}
	}
	if err := validateEvent(event); err != nil {
		return r.recordFailure(ctx, recorder, phase, event, err)
	}
	if !dryRun {
		if err := r.applyEvent(ctx, event); err != nil {
			return r.recordFailure(ctx, recorder, phase, event, err)
		}
	}
	if event.Duration.Duration > 0 {
		timer := time.NewTimer(event.Duration.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if recorder != nil {
		return recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-completed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"runtime": "kubernetes"}})
	}
	return nil
}

func (r KubernetesRuntime) applyEvent(ctx context.Context, event spec.EventSpec) error {
	switch event.Type {
	case "pod-stop", "pod-restart":
		pod := PodName(event.Target)
		if _, err := r.runKubectl(ctx, "delete", "pod", pod, "--ignore-not-found=true"); err != nil {
			return err
		}
		return r.waitPodsReady(ctx)
	case "rolling-restart":
		if _, err := r.runKubectl(ctx, "rollout", "restart", "statefulset/myceld"); err != nil {
			return err
		}
		return r.waitRolloutReady(ctx)
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}
}

func (r KubernetesRuntime) waitPodsReady(ctx context.Context) error {
	timeout := r.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	_, err := r.runKubectl(ctx, "wait", "pods", "-l", "app=myceld", "--for=condition=Ready", fmt.Sprintf("--timeout=%s", timeout.Round(time.Second)))
	return err
}

func (r KubernetesRuntime) waitRolloutReady(ctx context.Context) error {
	timeout := r.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	if _, err := r.runKubectl(ctx, "rollout", "status", "statefulset/myceld", fmt.Sprintf("--timeout=%s", timeout.Round(time.Second))); err != nil {
		return err
	}
	return r.waitPodsReady(ctx)
}

func (r KubernetesRuntime) runKubectl(ctx context.Context, args ...string) (executil.Result, error) {
	base := []string{"--context", r.Context, "-n", r.Namespace}
	base = append(base, args...)
	return r.runner().Run(ctx, "kubectl", base...)
}

func (r KubernetesRuntime) runner() executil.Runner {
	if r.Runner != nil {
		return r.Runner
	}
	return executil.LocalRunner{}
}

func (r KubernetesRuntime) recordFailure(ctx context.Context, recorder Recorder, phase spec.PhaseSpec, event spec.EventSpec, err error) error {
	if recorder != nil {
		_ = recorder.RecordRuntimeEvent(ctx, Record{Time: time.Now().UTC(), Type: "event-failed", PhaseName: phase.Name, EventType: event.Type, Target: event.Target, Payload: map[string]any{"error": err.Error(), "runtime": "kubernetes"}})
	}
	return err
}
