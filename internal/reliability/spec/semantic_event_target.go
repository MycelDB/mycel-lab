package spec

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
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
	switch eventType {
	case "semantic-node-create", "semantic-node-update", "semantic-node-delete", "semantic-rule-create", "semantic-backfill", "semantic-maintenance-await", "semantic-search-assert", "semantic-reliability-assert", "semantic-rebuild", "semantic-work-assert", "semantic-route-create", "semantic-route-bind", "semantic-model-config-update":
		fields["scope"] = semanticTargetField{validate: semanticCaptureName}
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
	if eventType == "semantic-maintenance-await" {
		disabled := target["completion"] == "disabled-quiescent"
		for _, name := range []string{"expectGenerationFrom", "expectFreshRevisionFrom", "expectRevisionFrom"} {
			_, present := target[name]
			if present != disabled {
				return fmt.Errorf("semantic-maintenance-await %s is required only for disabled-quiescent completion", name)
			}
		}
	}
	if eventType == "semantic-model-config-update" {
		_, dimension := target["dimensions"]
		_, space := target["vectorSpaceKey"]
		if dimension == space {
			return fmt.Errorf("model update must independently change dimension or vector space")
		}
	}
	if eventType == "semantic-search-assert" {
		if _, configured := target["configuredRoute"]; configured && target["requireConfiguredRoute"] != true {
			return fmt.Errorf("configuredRoute requires requireConfiguredRoute")
		}
	}
	if eventType == "semantic-work-assert" && target["analyze"] == true {
		if _, preserving := target["expectWorkFrom"]; preserving {
			return fmt.Errorf("semantic-work-assert preservation checks must not analyze or reconstruct work")
		}
		if target["expectedStatus"] != "pending" {
			return fmt.Errorf("semantic-work-assert analysis is allowed only when capturing pending work")
		}
	}
	if eventType == "semantic-provider-emulator-assert" {
		if raw, ok := target["expectedRequestsPerInput"]; ok {
			counts := raw.([]any)
			// An exact per-input distribution prevents a shared input-digest
			// counter from masquerading as a per-work retry limit. Reject an
			// impossible cap before provisioning any resources.
			total, largest := int64(0), int64(0)
			for _, rawCount := range counts {
				count, _ := semanticInt64(rawCount)
				total += count
				if count > largest {
					largest = count
				}
			}
			minimum, hasMinimum := semanticInt64(target["minimumRequests"])
			maximum, hasMaximum := semanticInt64(target["maximumRequests"])
			cap, hasCap := semanticInt64(target["maximumAttemptsPerInput"])
			if !hasMinimum || !hasMaximum || minimum != total || maximum != total || !hasCap || cap != largest {
				return fmt.Errorf("semantic-provider-emulator-assert expectedRequestsPerInput requires exact request bounds equal to its sum and maximumAttemptsPerInput equal to its largest count")
			}
		}
	}
	if eventType == "semantic-fault-assert" {
		mode := target["disposition"]
		_, hasGate := target["gate"]
		_, hasDeadline := target["processTimeout"]
		if (mode == "cancel") != hasGate || (mode == "deadline") != hasDeadline {
			return fmt.Errorf("fault cancellation requires only a gate; deadline requires only processTimeout")
		}
		if _, maximum := target["maximumBackoff"]; maximum && mode != "retryable" {
			return fmt.Errorf("maximumBackoff requires retryable disposition")
		}
		if _, minimum := target["minimumBackoff"]; minimum && mode != "retryable" {
			return fmt.Errorf("minimumBackoff requires retryable disposition")
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
	case "semantic-domain-peer-create":
		return map[string]semanticTargetField{"name": required(semanticGate)}, nil
	case "semantic-route-create":
		return map[string]semanticTargetField{"name": required(semanticGate), "dimensions": required(semanticInteger(1, 65536)), "vectorSpaceKey": required(nonEmptySemanticString)}, nil
	case "semantic-route-bind":
		return map[string]semanticTargetField{"route": required(semanticGate)}, nil
	case "semantic-model-config-update":
		return map[string]semanticTargetField{"route": required(semanticGate), "dimensions": optional(semanticInteger(1, 65536)), "vectorSpaceKey": optional(nonEmptySemanticString)}, nil
	case "semantic-domain-isolation-assert":
		return captureFields(map[string]semanticTargetField{
			"peer": required(semanticGate), "nodeId": required(canonicalSemanticUUID), "primaryQuery": required(nonEmptySemanticString), "peerQuery": required(nonEmptySemanticString),
			"expectPrimaryRevisionFrom": required(semanticCaptureName), "expectPeerRevisionFrom": required(semanticCaptureName), "timeout": optional(positiveSemanticDuration),
		}, "capturePrimaryRecordAs", "capturePeerRecordAs", "expectPrimaryRecordFrom", "expectPeerRecordFrom"), nil
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
	case "semantic-work-assert":
		return map[string]semanticTargetField{
			"nodeId": required(canonicalSemanticUUID), "expectedStatus": required(semanticWorkStatus),
			"expectRevisionFrom": required(semanticCaptureName),
			"captureWorkAs":      optional(semanticCaptureName), "expectWorkFrom": optional(semanticCaptureName),
			"analyze": boolField, "expectBackgroundMaintenanceEnabled": boolField,
			"expectedAttemptCount": optional(semanticInteger(0, 4096)),
			"timeout":              optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
		}, nil
	case "semantic-ordering-assert":
		return map[string]semanticTargetField{
			"nodeId": required(canonicalSemanticUUID), "gate": required(semanticGate), "text": required(nonEmptySemanticString),
			"expectRevisionFrom": required(semanticCaptureName), "captureRevisionAs": required(semanticCaptureName), "captureRecordAs": required(semanticCaptureName),
			"timeout": optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
		}, nil
	case "semantic-fault-assert":
		return map[string]semanticTargetField{
			"nodeId": required(canonicalSemanticUUID), "expectRevisionFrom": required(semanticCaptureName), "disposition": required(semanticFaultDisposition),
			"gate": optional(semanticGate), "processTimeout": optional(positiveSemanticDuration), "minimumBackoff": optional(positiveSemanticDuration), "maximumBackoff": optional(positiveSemanticDuration),
			"timeout": optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
		}, nil
	case "semantic-maintenance-await":
		return map[string]semanticTargetField{
			"timeout": optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
			"completion":           optional(semanticMaintenanceCompletion),
			"expectGenerationFrom": optional(semanticCaptureName), "expectFreshRevisionFrom": optional(semanticCaptureName), "expectRevisionFrom": optional(semanticCaptureName),
		}, nil
	case "semantic-search-assert":
		fields := map[string]semanticTargetField{
			"query": required(nonEmptySemanticString), "limit": optional(semanticInteger(1, 1000)),
			"minimumScore": optional(finiteSemanticNumber), "filterNodeLabels": optional(semanticStringList),
			"filterNodeIds": optional(semanticStringList), "filterProperties": optional(semanticStringMap),
			"expectFailureCode": stringField, "allPages": boolField, "allowWarnings": boolField,
			"timeout": optional(positiveSemanticDuration), "pollInterval": optional(positiveSemanticDuration),
			"expectedResultCount": nonNegativeInt, "requireProvenance": boolField, "requireActualRoute": boolField,
			"broad": boolField, "requireConfiguredRoute": boolField, "configuredRoute": optional(semanticGate),
			"expectedDimensions": optional(semanticInteger(1, 65536)), "expectedVectorSpaceKey": stringField, "expectedPayloadText": stringField,
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
			"captureFreshRevisionAs", "capturePinnedRevisionAs", "captureDeltaCatchupRevisionAs", "requireGenerationAfter", "expectGenerationFrom",
		), nil
	case "semantic-provider-budget-reconcile":
		return map[string]semanticTargetField{
			"minimumAttempts": nonNegativeInt, "requireTerminal": boolField,
		}, nil
	case "semantic-provider-emulator-assert":
		return map[string]semanticTargetField{
			"minimumRequests": optional(semanticInteger(0, 100000)), "maximumRequests": optional(semanticInteger(0, 100000)),
			"maximumBatchItems": optional(semanticInteger(1, 1024)), "maximumAttemptsPerInput": optional(semanticInteger(1, 1024)),
			"expectedRequestsPerInput": optional(semanticPositiveIntegerList),
			"requireSingleItem":        boolField, "authorization": optional(semanticProviderAuthorization), "requireCanceled": boolField,
			"expectedOutcomes": optional(semanticIntegerMap), "expectedStatuses": optional(semanticIntegerMap),
			"minimumOutcomes": optional(semanticIntegerMap), "minimumStatuses": optional(semanticIntegerMap),
			"minimumResponseModels": optional(semanticIntegerMap), "requiredProviderRequestIds": optional(semanticStringList),
			"expectedModels": optional(semanticStringList),
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

func semanticGate(value any) error {
	name, ok := value.(string)
	if !ok || !provider.ValidEmulatorGate(name) {
		return fmt.Errorf("must be a bounded synthetic gate name")
	}
	return nil
}

func semanticMaintenanceCompletion(value any) error {
	if value != "indexed" && value != "disabled-quiescent" {
		return fmt.Errorf("must be indexed or disabled-quiescent")
	}
	return nil
}

func semanticFaultDisposition(value any) error {
	switch value {
	case "retryable", "permanent", "deadline", "cancel":
		return nil
	}
	return fmt.Errorf("must be retryable, permanent, deadline, or cancel")
}

func canonicalSemanticUUID(value any) error {
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a canonical nonzero UUID")
	}
	if !IsCanonicalSemanticUUID(text) {
		return fmt.Errorf("must be a canonical nonzero UUID")
	}
	return nil
}

// IsCanonicalSemanticUUID accepts lowercase, nonzero UUID text without adding
// a dependency or accepting UUID URNs, braces, aliases, or surrounding spaces.
func IsCanonicalSemanticUUID(text string) bool {
	if len(text) != 36 {
		return false
	}
	nonzero := false
	for index, char := range text {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
		if char != '0' {
			nonzero = true
		}
	}
	return nonzero
}

func semanticCaptureName(value any) error {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > 128 {
		return fmt.Errorf("must be a bounded capture name")
	}
	for _, char := range text {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.' {
			continue
		}
		return fmt.Errorf("must be a bounded capture name")
	}
	return nil
}

func semanticWorkStatus(value any) error {
	if value != "pending" && value != "complete" {
		return fmt.Errorf("must be pending or complete")
	}
	return nil
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

func semanticProviderAuthorization(value any) error {
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}
	switch strings.TrimSpace(text) {
	case "present", "absent", "":
		return nil
	default:
		return fmt.Errorf("must be present or absent")
	}
}

func semanticIntegerMap(value any) error {
	mapping, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("must be an object")
	}
	for key, item := range mapping {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("must not contain an empty key")
		}
		if err := semanticInteger(0, math.MaxInt32)(item); err != nil {
			return fmt.Errorf("value for %q: %w", key, err)
		}
	}
	return nil
}

// semanticPositiveIntegerList represents an exact, nonempty multiset of bounded
// per-input request counts. The individual input hashes never enter the spec.
func semanticPositiveIntegerList(value any) error {
	list, ok := value.([]any)
	if !ok || len(list) == 0 || len(list) > 1024 {
		return fmt.Errorf("must be a nonempty integer list with at most 1024 entries")
	}
	for _, item := range list {
		if err := semanticInteger(1, 1024)(item); err != nil {
			return fmt.Errorf("must contain only positive integers no greater than 1024: %w", err)
		}
	}
	return nil
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
