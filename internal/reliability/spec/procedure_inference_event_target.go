package spec

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type ProcedureInferenceScenarioSpec struct {
	TargetActorGroup string   `yaml:"targetActorGroup" json:"targetActorGroup"`
	AuditActorGroup  string   `yaml:"auditActorGroup" json:"auditActorGroup"`
	ProcedureID      string   `yaml:"procedureId,omitempty" json:"procedureId,omitempty"`
	BindingID        string   `yaml:"bindingId,omitempty" json:"bindingId,omitempty"`
	TargetLabels     []string `yaml:"targetLabels" json:"targetLabels"`
}

func (s *ProcedureInferenceScenarioSpec) setDefaults() {
	if s == nil {
		return
	}
	if strings.TrimSpace(s.ProcedureID) == "" {
		s.ProcedureID = "mycel-lab.procedure-inference"
	}
	if strings.TrimSpace(s.BindingID) == "" {
		s.BindingID = "mycel-lab.procedure-inference.graph-trigger"
	}
}

func (s ProcedureInferenceScenarioSpec) Validate(actorGroups map[string]bool, providerProfileRef string) error {
	if strings.TrimSpace(providerProfileRef) == "" {
		return errors.New("procedureInference requires providerProfileRef")
	}
	if !actorGroups[s.TargetActorGroup] {
		return fmt.Errorf("procedureInference targetActorGroup %q does not exist", s.TargetActorGroup)
	}
	if !actorGroups[s.AuditActorGroup] {
		return fmt.Errorf("procedureInference auditActorGroup %q does not exist", s.AuditActorGroup)
	}
	if s.TargetActorGroup == s.AuditActorGroup {
		return errors.New("procedureInference targetActorGroup and auditActorGroup must be distinct")
	}
	if strings.TrimSpace(s.ProcedureID) == "" || strings.TrimSpace(s.BindingID) == "" {
		return errors.New("procedureInference procedureId and bindingId are required")
	}
	if len(s.TargetLabels) == 0 {
		return errors.New("procedureInference targetLabels must not be empty")
	}
	for _, label := range s.TargetLabels {
		if strings.TrimSpace(label) == "" {
			return errors.New("procedureInference targetLabels must not contain empty values")
		}
	}
	return nil
}

// ValidateProcedureInferenceEventTarget rejects unknown or unbounded event
// controls before deployment. Graph content values are accepted only by the
// node-create event and are never copied into inference evidence.
func ValidateProcedureInferenceEventTarget(eventType string, target map[string]any) error {
	allowed := map[string]map[string]struct{}{
		"procedure-inference-setup": {
			"timeout": {},
		},
		"procedure-inference-node-create": {
			"nodeId": {}, "text": {}, "timeout": {},
		},
		"procedure-inference-await-terminal": {
			"timeout": {}, "interval": {}, "stableFor": {}, "minimumAttempts": {}, "maximumAttempts": {}, "requireSucceeded": {}, "requireChanges": {}, "checkpoint": {}, "compareCheckpoint": {},
		},
		"procedure-inference-control": {
			"action": {}, "timeout": {}, "checkpoint": {}, "compareCheckpoint": {},
		},
		"procedure-inference-capacity-diagnostics-assert": {
			"timeout": {}, "tail": {}, "requirePressure": {},
		},
		"procedure-inference-emulator-assert": {
			"minimumRequests": {}, "maximumRequests": {}, "requireSingleItem": {}, "maximumBatchItems": {},
			"maximumAttemptsPerInput": {}, "authorization": {}, "expectedModels": {}, "minimumOutcomes": {}, "minimumStatuses": {},
		},
	}
	fields, ok := allowed[eventType]
	if !ok {
		return fmt.Errorf("unsupported procedure inference event type %q", eventType)
	}
	for key := range target {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("%s target contains unsupported field %q", eventType, key)
		}
	}
	for _, key := range []string{"timeout", "interval", "stableFor"} {
		if raw, ok := target[key]; ok {
			value, ok := raw.(string)
			if !ok {
				return fmt.Errorf("%s target.%s must be a duration string", eventType, key)
			}
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed <= 0 {
				return fmt.Errorf("%s target.%s must be a positive duration", eventType, key)
			}
		}
	}
	switch eventType {
	case "procedure-inference-node-create":
		if strings.TrimSpace(fmt.Sprint(target["nodeId"])) == "" || strings.TrimSpace(fmt.Sprint(target["nodeId"])) == "<nil>" {
			return fmt.Errorf("%s target.nodeId is required", eventType)
		}
		text := strings.TrimSpace(fmt.Sprint(target["text"]))
		if text == "" || text == "<nil>" || len(text) > 4096 {
			return fmt.Errorf("%s target.text must contain 1-4096 bytes", eventType)
		}
	case "procedure-inference-await-terminal":
		for _, key := range []string{"checkpoint", "compareCheckpoint"} {
			if raw, exists := target[key]; exists {
				value, ok := raw.(string)
				if !ok || !validCheckpointName(value) {
					return fmt.Errorf("%s target.%s must be a safe checkpoint name", eventType, key)
				}
			}
		}
		minimum, ok := strictPositiveInteger(target["minimumAttempts"], 1)
		if !ok || minimum > 64 {
			return fmt.Errorf("%s target.minimumAttempts must be between 1 and 64", eventType)
		}
		maximum, ok := strictPositiveInteger(target["maximumAttempts"], 64)
		if !ok || maximum < minimum || maximum > 256 {
			return fmt.Errorf("%s target.maximumAttempts must be between minimumAttempts and 256", eventType)
		}
	case "procedure-inference-control":
		action := strings.TrimSpace(fmt.Sprint(target["action"]))
		if action != "enable" && action != "disable" {
			return fmt.Errorf("%s target.action must be enable or disable", eventType)
		}
		for _, key := range []string{"checkpoint", "compareCheckpoint"} {
			if raw, exists := target[key]; exists {
				value, ok := raw.(string)
				if !ok || !validCheckpointName(value) {
					return fmt.Errorf("%s target.%s must be a safe checkpoint name", eventType, key)
				}
			}
		}
		if _, checkpoint := target["checkpoint"]; checkpoint {
			if _, compare := target["compareCheckpoint"]; compare {
				return fmt.Errorf("%s target cannot set checkpoint and compareCheckpoint together", eventType)
			}
		}
	case "procedure-inference-emulator-assert":
		minimum, ok := optionalNonnegativeInteger(target["minimumRequests"], 0)
		if !ok || minimum > 100000 {
			return fmt.Errorf("%s target.minimumRequests must be between 0 and 100000", eventType)
		}
		maximum, ok := optionalNonnegativeInteger(target["maximumRequests"], 0)
		if !ok || maximum < minimum || maximum > 100000 {
			return fmt.Errorf("%s target.maximumRequests must be between minimumRequests and 100000", eventType)
		}
		for _, key := range []string{"maximumBatchItems", "maximumAttemptsPerInput"} {
			if raw, exists := target[key]; exists {
				value, ok := strictPositiveInteger(raw, 0)
				if !ok || value <= 0 || value > 100000 {
					return fmt.Errorf("%s target.%s must be between 1 and 100000", eventType, key)
				}
			}
		}
		if raw, exists := target["requireSingleItem"]; exists {
			if _, ok := raw.(bool); !ok {
				return fmt.Errorf("%s target.requireSingleItem must be boolean", eventType)
			}
		}
		if authorization := strings.TrimSpace(fmt.Sprint(target["authorization"])); authorization != "absent" && authorization != "present" {
			return fmt.Errorf("%s target.authorization must be absent or present", eventType)
		}
		if raw, exists := target["expectedModels"]; exists && !validNonemptyStringList(raw) {
			return fmt.Errorf("%s target.expectedModels must be a nonempty string list", eventType)
		}
		for _, key := range []string{"minimumOutcomes", "minimumStatuses"} {
			if raw, exists := target[key]; exists && !validBoundedCountMap(raw) {
				return fmt.Errorf("%s target.%s must be a nonempty map of bounded nonnegative integers", eventType, key)
			}
		}
	case "procedure-inference-capacity-diagnostics-assert":
		tail, ok := strictPositiveInteger(target["tail"], 5000)
		if !ok || tail > 20000 {
			return fmt.Errorf("%s target.tail must be between 1 and 20000", eventType)
		}
		if raw, exists := target["requirePressure"]; exists {
			if _, ok := raw.(bool); !ok {
				return fmt.Errorf("%s target.requirePressure must be boolean", eventType)
			}
		}
	}
	return nil
}

func validCheckpointName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func validNonemptyStringList(raw any) bool {
	values, ok := raw.([]any)
	if !ok || len(values) == 0 || len(values) > 256 {
		return false
	}
	for _, value := range values {
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" || len(text) > 256 {
			return false
		}
	}
	return true
}

func validBoundedCountMap(raw any) bool {
	values, ok := raw.(map[string]any)
	if !ok || len(values) == 0 || len(values) > 256 {
		return false
	}
	for key, rawValue := range values {
		if strings.TrimSpace(key) == "" || len(key) > 128 {
			return false
		}
		value, ok := strictNonnegativeInteger(rawValue)
		if !ok || value > 100000 {
			return false
		}
	}
	return true
}

func strictNonnegativeInteger(raw any) (int, bool) {
	switch value := raw.(type) {
	case int:
		return value, value >= 0
	case int64:
		return int(value), value >= 0
	case float64:
		parsed := int(value)
		return parsed, value == float64(parsed) && parsed >= 0
	default:
		return 0, false
	}
}

func optionalNonnegativeInteger(raw any, fallback int) (int, bool) {
	if raw == nil {
		return fallback, true
	}
	return strictNonnegativeInteger(raw)
}

func strictPositiveInteger(raw any, fallback int) (int, bool) {
	if raw == nil {
		return fallback, true
	}
	switch value := raw.(type) {
	case int:
		return value, value > 0
	case int64:
		return int(value), value > 0
	case float64:
		parsed := int(value)
		return parsed, value == float64(parsed) && parsed > 0
	default:
		return 0, false
	}
}
