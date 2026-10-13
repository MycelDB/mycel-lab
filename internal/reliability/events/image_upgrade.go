package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

type StatefulSetImageUpgradeResult struct {
	StatefulSet string `json:"statefulSet"`
	Container   string `json:"container"`
	Image       string `json:"image"`
	Timeout     string `json:"timeout,omitempty"`
	Stdout      string `json:"stdout,omitempty"`
	Stderr      string `json:"stderr,omitempty"`
}

func executeStatefulSetImageUpgradeEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (StatefulSetImageUpgradeResult, error) {
	statefulSet := firstStringTarget(target, "statefulSet", "statefulset", "resource")
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	statefulSet = strings.TrimPrefix(statefulSet, "statefulset/")
	container := firstStringTarget(target, "container", "containerName")
	if container == "" {
		container = "myceld"
	}
	image := stringTarget(target, "image")
	timeout, err := targetDuration(target, "rolloutTimeout", "timeout")
	if err != nil {
		return StatefulSetImageUpgradeResult{}, err
	}
	result := StatefulSetImageUpgradeResult{StatefulSet: statefulSet, Container: container, Image: image}
	if timeout > 0 {
		result.Timeout = timeout.String()
	}
	updater, ok := driver.(env.StatefulSetImageUpdater)
	if !ok {
		return result, fmt.Errorf("driver %q does not support statefulset image upgrades", driver.Name())
	}
	execResult, err := updater.UpdateStatefulSetImage(ctx, environment, env.StatefulSetImageUpdateRequest{StatefulSet: statefulSet, Container: container, Image: image, Timeout: timeout})
	result.Stdout = execResult.Stdout
	result.Stderr = execResult.Stderr
	return result, err
}

func firstStringTarget(target map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringTarget(target, key); value != "" {
			return value
		}
	}
	return ""
}

func targetDuration(target map[string]any, keys ...string) (time.Duration, error) {
	for _, key := range keys {
		value := strings.TrimSpace(fmt.Sprint(target[key]))
		if value == "" || value == "<nil>" {
			continue
		}
		duration, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("target.%s duration %q: %w", key, value, err)
		}
		return duration, nil
	}
	return 0, nil
}
