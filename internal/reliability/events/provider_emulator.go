package events

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

type SemanticProviderEmulatorResult struct {
	RequestCount           int            `json:"requestCount"`
	OutcomeCounts          map[string]int `json:"outcomeCounts"`
	StatusCounts           map[string]int `json:"statusCounts"`
	Models                 []string       `json:"models"`
	ResponseModelCounts    map[string]int `json:"responseModelCounts"`
	ProviderRequestIDCount int            `json:"providerRequestIdCount"`
	MaximumBatchItems      int            `json:"maximumBatchItems"`
	MaximumAttempts        int            `json:"maximumAttemptsPerInput"`
	AuthorizationPresent   int            `json:"authorizationPresent"`
	AuthorizationAbsent    int            `json:"authorizationAbsent"`
	Canceled               int            `json:"canceled"`
	FirstSequence          int            `json:"firstSequence"`
	LastSequence           int            `json:"lastSequence"`
}

func executeSemanticProviderEmulatorAssert(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (SemanticProviderEmulatorResult, error) {
	inspector, ok := driver.(env.ProviderEmulatorInspector)
	if !ok {
		return SemanticProviderEmulatorResult{}, fmt.Errorf("driver %q does not support provider emulator observations", driver.Name())
	}
	output, err := inspector.ProviderEmulatorObservations(ctx, environment)
	if err != nil {
		return SemanticProviderEmulatorResult{}, fmt.Errorf("read provider emulator observations: %w", err)
	}
	observations, err := emulator.DecodeObservationEvidence(output.Stdout)
	if err != nil {
		return SemanticProviderEmulatorResult{}, err
	}
	result := SemanticProviderEmulatorResult{OutcomeCounts: map[string]int{}, StatusCounts: map[string]int{}, ResponseModelCounts: map[string]int{}}
	models := map[string]struct{}{}
	providerRequestIDs := map[string]struct{}{}
	for _, observation := range observations {
		if observation.BatchItemCount > result.MaximumBatchItems {
			result.MaximumBatchItems = observation.BatchItemCount
		}
		if observation.Attempt > result.MaximumAttempts {
			result.MaximumAttempts = observation.Attempt
		}
		result.OutcomeCounts[observation.Outcome]++
		result.StatusCounts[strconv.Itoa(observation.Status)]++
		if observation.Model != "" {
			models[observation.Model] = struct{}{}
		}
		if observation.ResponseModel != "" {
			result.ResponseModelCounts[observation.ResponseModel]++
		}
		if observation.ProviderRequestID != "" {
			providerRequestIDs[observation.ProviderRequestID] = struct{}{}
			result.ProviderRequestIDCount++
		}
		if observation.AuthorizationPresent {
			result.AuthorizationPresent++
		} else {
			result.AuthorizationAbsent++
		}
		if observation.Canceled {
			result.Canceled++
		}
	}
	result.RequestCount = len(observations)
	if result.RequestCount > 0 {
		result.FirstSequence = observations[0].Sequence
		result.LastSequence = observations[len(observations)-1].Sequence
	}
	for model := range models {
		result.Models = append(result.Models, model)
	}
	sort.Strings(result.Models)

	minimum := intTarget(target, "minimumRequests", 1)
	maximum := intTarget(target, "maximumRequests", 100000)
	if result.RequestCount < minimum || result.RequestCount > maximum {
		return result, fmt.Errorf("provider emulator request count %d is outside [%d,%d]", result.RequestCount, minimum, maximum)
	}
	if configured := intTarget(target, "maximumBatchItems", 1024); result.MaximumBatchItems > configured {
		return result, fmt.Errorf("provider emulator maximum batch size %d exceeds %d", result.MaximumBatchItems, configured)
	}
	if boolTarget(target, "requireSingleItem", false) && result.MaximumBatchItems != 1 {
		return result, fmt.Errorf("provider emulator maximum batch size %d, want exactly 1", result.MaximumBatchItems)
	}
	if configured := intTarget(target, "maximumAttemptsPerInput", 1024); result.MaximumAttempts > configured {
		return result, fmt.Errorf("provider emulator maximum attempts %d exceeds %d", result.MaximumAttempts, configured)
	}
	switch targetString(target, "authorization") {
	case "present":
		if result.AuthorizationPresent != result.RequestCount {
			return result, errors.New("provider emulator did not observe authorization on every request")
		}
	case "absent":
		if result.AuthorizationAbsent != result.RequestCount {
			return result, errors.New("provider emulator observed an unexpected authorization header")
		}
	}
	if boolTarget(target, "requireCanceled", false) && result.Canceled == 0 {
		return result, errors.New("provider emulator did not observe a canceled request")
	}
	if err := requireExactProviderCounts("outcome", result.RequestCount, result.OutcomeCounts, targetIntegerMap(target, "expectedOutcomes")); err != nil {
		return result, err
	}
	if err := requireExactProviderCounts("status", result.RequestCount, result.StatusCounts, targetIntegerMap(target, "expectedStatuses")); err != nil {
		return result, err
	}
	if err := requireMinimumProviderCounts("outcome", result.OutcomeCounts, targetIntegerMap(target, "minimumOutcomes")); err != nil {
		return result, err
	}
	if err := requireMinimumProviderCounts("status", result.StatusCounts, targetIntegerMap(target, "minimumStatuses")); err != nil {
		return result, err
	}
	if err := requireMinimumProviderCounts("response model", result.ResponseModelCounts, targetIntegerMap(target, "minimumResponseModels")); err != nil {
		return result, err
	}
	for _, requiredID := range stringSliceTarget(target, "requiredProviderRequestIds") {
		if _, ok := providerRequestIDs[requiredID]; !ok {
			return result, fmt.Errorf("provider emulator did not observe required safe request ID %q", requiredID)
		}
	}
	if expected := stringSliceTarget(target, "expectedModels"); len(expected) > 0 {
		sort.Strings(expected)
		if strings.Join(expected, "\x00") != strings.Join(result.Models, "\x00") {
			return result, fmt.Errorf("provider emulator models %v, want %v", result.Models, expected)
		}
	}
	return result, nil
}

func targetIntegerMap(target map[string]any, key string) map[string]int {
	raw, ok := target[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]int, len(raw))
	for name, value := range raw {
		if parsed, ok := semanticIntTarget(value); ok {
			out[name] = parsed
		}
	}
	return out
}

func semanticIntTarget(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		if typed == float64(int(typed)) {
			return int(typed), true
		}
	}
	return 0, false
}

func requireMinimumProviderCounts(kind string, actual, minimum map[string]int) error {
	for name, count := range minimum {
		if actual[name] < count {
			return fmt.Errorf("provider emulator %s %q count %d, want at least %d", kind, name, actual[name], count)
		}
	}
	return nil
}

func requireExactProviderCounts(kind string, total int, actual, expected map[string]int) error {
	if len(expected) == 0 {
		return nil
	}
	expectedTotal := 0
	for name, count := range expected {
		expectedTotal += count
		if actual[name] != count {
			return fmt.Errorf("provider emulator %s %q count %d, want %d", kind, name, actual[name], count)
		}
	}
	if expectedTotal != total {
		return fmt.Errorf("provider emulator expected %s counts total %d, observed %d", kind, expectedTotal, total)
	}
	return nil
}
