package spec

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type semanticTargetValidator func(any) error

type semanticTargetField struct {
	required bool
	validate semanticTargetValidator
}

// ValidateSemanticEventTarget enforces the typed, fail-closed target contract
// for semantic reliability events. Unknown fields are rejected so assertion
// typos cannot silently weaken a scenario.
func ValidateSemanticEventTarget(eventType string, target map[string]any) error {
	fields, err := semanticTargetFields(eventType)
	if err != nil {
		return err
	}
	for key := range target {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("%s target field %q is not supported", eventType, key)
		}
	}
	for key, field := range fields {
		value, ok := target[key]
		if !ok {
			if field.required {
				return fmt.Errorf("%s requires target.%s", eventType, key)
			}
			continue
		}
		if err := field.validate(value); err != nil {
			return fmt.Errorf("%s target.%s: %w", eventType, key, err)
		}
	}
	if eventType == "semantic-rebuild" {
		if nested, ok := target["concurrentMutation"]; ok {
			mutation, _ := nested.(map[string]any)
			mutationType := "semantic-node-create"
			if raw, exists := mutation["eventType"]; exists {
				text, ok := raw.(string)
				if !ok || strings.TrimSpace(text) == "" {
					return fmt.Errorf("semantic-rebuild target.concurrentMutation.eventType must be a nonempty string")
				}
				mutationType = strings.TrimSpace(text)
			}
			if mutationType != "semantic-node-create" && mutationType != "semantic-node-update" && mutationType != "semantic-node-delete" {
				return fmt.Errorf("semantic-rebuild target.concurrentMutation.eventType %q is not supported", mutationType)
			}
			copy := make(map[string]any, len(mutation)-1)
			for key, value := range mutation {
				if key != "eventType" {
					copy[key] = value
				}
			}
			if err := ValidateSemanticEventTarget(mutationType, copy); err != nil {
				return fmt.Errorf("semantic-rebuild target.concurrentMutation: %w", err)
			}
		}
	}
	return nil
}

func semanticTargetFields(eventType string) (map[string]semanticTargetField, error) {
	optional := func(validate semanticTargetValidator) semanticTargetField {
		return semanticTargetField{validate: validate}
	}
	required := func(validate semanticTargetValidator) semanticTargetField {
		return semanticTargetField{required: true, validate: validate}
	}
	stringField := optional(nonEmptySemanticString)
	boolField := optional(semanticBool)
	nonNegativeInt := optional(semanticInteger(0, math.MaxInt32))
	captureFields := func(fields map[string]semanticTargetField, names ...string) map[string]semanticTargetField {
		for _, name := range names {
			fields[name] = stringField
		}
		return fields
	}

	switch eventType {
	case "semantic-rule-create":
		return map[string]semanticTargetField{"semantic": required(nonEmptySemanticString)}, nil
	case "semantic-node-create":
		return captureFields(map[string]semanticTargetField{
			"actorGroup": optional(nonEmptySemanticString), "nodeId": required(nonEmptySemanticString),
			"labels": optional(semanticStringList), "text": optional(semanticString), "properties": optional(semanticStringMap),
		}, "captureRevisionAs"), nil
	case "semantic-node-update":
		return captureFields(map[string]semanticTargetField{
			"actorGroup": optional(nonEmptySemanticString), "nodeId": required(nonEmptySemanticString),
			"text": optional(semanticString), "properties": optional(semanticStringMap),
		}, "captureRevisionAs"), nil
	case "semantic-node-delete":
		return captureFields(map[string]semanticTargetField{
			"actorGroup": optional(nonEmptySemanticString), "nodeId": required(nonEmptySemanticString),
		}, "captureRevisionAs"), nil
	case "semantic-transaction-negative":
		return map[string]semanticTargetField{
			"rollbackNodeId": required(nonEmptySemanticString), "failedNodeId": required(nonEmptySemanticString),
		}, nil
	case "semantic-backfill":
		return map[string]semanticTargetField{
			"force": boolField, "limit": optional(semanticInteger(1, 4096)), "nodeIds": optional(semanticStringList),
			"expectedSelected": nonNegativeInt, "minimumGenerated": nonNegativeInt,
		}, nil
	case "semantic-maintenance-await":
		return map[string]semanticTargetField{
			"timeout": optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
		}, nil
	case "semantic-search-assert":
		fields := map[string]semanticTargetField{
			"query": required(nonEmptySemanticString), "limit": optional(semanticInteger(1, 1000)),
			"minimumScore": optional(finiteSemanticNumber), "filterNodeLabels": optional(semanticStringList),
			"filterNodeIds": optional(semanticStringList), "filterProperties": optional(semanticStringMap),
			"expectFailureCode": stringField, "allPages": boolField, "allowWarnings": boolField,
			"expectedResultCount": nonNegativeInt, "requireProvenance": boolField, "requireActualRoute": boolField,
			"expectedNodeIds": optional(semanticStringList), "forbiddenNodeIds": optional(semanticStringList),
			"expectedScores": optional(semanticNumberList), "scoreTolerance": optional(nonNegativeSemanticNumber),
		}
		return captureFields(fields,
			"expectSourceRevisionFrom", "expectGenerationFrom", "expectRebuildEpochFrom", "expectConfigurationHashFrom",
			"rejectConfigurationHashFrom", "captureRecordAs", "captureConfigurationHashAs", "expectRecordFrom", "rejectRecordFrom",
		), nil
	case "semantic-unauthorized-search":
		return map[string]semanticTargetField{
			"actorGroup": required(nonEmptySemanticString), "requireProviderBudgetEvidence": boolField,
		}, nil
	case "semantic-rule-enable":
		return map[string]semanticTargetField{"enabled": required(semanticBool)}, nil
	case "semantic-rule-projection-update":
		return map[string]semanticTargetField{"includeProperties": required(semanticStringList)}, nil
	case "semantic-reliability-assert":
		fields := map[string]semanticTargetField{
			"expectRebuildRequired": boolField, "maximumUnresolvedGapCount": nonNegativeInt,
			"requireFreshAtLatest": boolField, "minimumSemanticGeneration": nonNegativeInt,
			"requireDeltaAfterPinned": boolField,
		}
		return captureFields(fields,
			"expectFreshRevisionFrom", "expectDeltaCatchupRevisionFrom", "captureGenerationAs", "captureRebuildEpochAs",
			"captureFreshRevisionAs", "capturePinnedRevisionAs", "captureDeltaCatchupRevisionAs",
		), nil
	case "semantic-provider-budget-reconcile":
		return map[string]semanticTargetField{
			"minimumAttempts": nonNegativeInt, "requireTerminal": boolField,
		}, nil
	case "semantic-rebuild":
		fields := map[string]semanticTargetField{
			"backfillLimit":         optional(semanticInteger(1, 100000)),
			"maintenanceBatchLimit": optional(semanticInteger(1, math.MaxInt32)),
			"maxCatchupIterations":  optional(semanticInteger(1, 10000)),
			"concurrentMutation":    optional(semanticStringMap),
		}
		return captureFields(fields, "captureGenerationAs", "captureRebuildEpochAs", "captureFreshRevisionAs", "capturePinnedRevisionAs", "captureDeltaCatchupRevisionAs"), nil
	default:
		return nil, fmt.Errorf("unsupported semantic event type %q", eventType)
	}
}

func semanticString(value any) error {
	if _, ok := value.(string); !ok {
		return fmt.Errorf("must be a string")
	}
	return nil
}

func nonEmptySemanticString(value any) error {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return fmt.Errorf("must be a nonempty string")
	}
	return nil
}

func semanticBool(value any) error {
	if _, ok := value.(bool); !ok {
		return fmt.Errorf("must be a boolean")
	}
	return nil
}

func semanticInteger(minimum, maximum int64) semanticTargetValidator {
	return func(value any) error {
		number, ok := semanticInt64(value)
		if !ok || number < minimum || number > maximum {
			return fmt.Errorf("must be an integer between %d and %d", minimum, maximum)
		}
		return nil
	}
}

func semanticInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number < math.MinInt64 || number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}

func finiteSemanticNumber(value any) error {
	number, ok := semanticFloat64(value)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
		return fmt.Errorf("must be a finite number")
	}
	return nil
}

func nonNegativeSemanticNumber(value any) error {
	if err := finiteSemanticNumber(value); err != nil {
		return err
	}
	number, _ := semanticFloat64(value)
	if number < 0 {
		return fmt.Errorf("must be nonnegative")
	}
	return nil
}

func semanticFloat64(value any) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}

func semanticStringList(value any) error {
	var values []string
	switch list := value.(type) {
	case []string:
		values = list
	case []any:
		values = make([]string, 0, len(list))
		for _, item := range list {
			text, ok := item.(string)
			if !ok {
				return fmt.Errorf("must contain only strings")
			}
			values = append(values, text)
		}
	default:
		return fmt.Errorf("must be a string list")
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("must not contain empty strings")
		}
	}
	return nil
}

func semanticNumberList(value any) error {
	list, ok := value.([]any)
	if !ok {
		return fmt.Errorf("must be a numeric list")
	}
	for _, item := range list {
		if err := finiteSemanticNumber(item); err != nil {
			return fmt.Errorf("must contain only finite numbers")
		}
	}
	return nil
}

func semanticStringMap(value any) error {
	mapping, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("must be an object")
	}
	for key := range mapping {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("must not contain an empty key")
		}
	}
	return nil
}

func positiveSemanticDuration(value any) error {
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a duration string")
	}
	duration, err := time.ParseDuration(strings.TrimSpace(text))
	if err != nil || duration <= 0 {
		return fmt.Errorf("must be a positive duration")
	}
	return nil
}
