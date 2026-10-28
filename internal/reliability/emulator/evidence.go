package emulator

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// DecodeObservationEvidence decodes and validates bounded sanitized protocol
// envelopes. Empty evidence is valid for startup capture; callers responsible
// for final evidence must separately require a nonempty result.
func DecodeObservationEvidence(raw string) ([]Observation, error) {
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	observations := make([]Observation, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := rejectDuplicateObservationFields(line, len(observations)+1); err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.DisallowUnknownFields()
		var observation Observation
		if err := decoder.Decode(&observation); err != nil {
			return nil, fmt.Errorf("decode provider emulator observation %d", len(observations)+1)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("provider emulator observation %d has invalid trailing content", len(observations)+1)
		}
		observations = append(observations, observation)
		if len(observations) > 100000 {
			return nil, errors.New("provider emulator observation ceiling exceeded")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("scan provider emulator observations failed")
	}
	sort.Slice(observations, func(i, j int) bool { return observations[i].Sequence < observations[j].Sequence })
	attempts := map[string]int{}
	for index, observation := range observations {
		if err := validateObservation(observation, index, attempts[observation.InputSHA256]); err != nil {
			return nil, err
		}
		attempts[observation.InputSHA256] = observation.Attempt
	}
	return observations, nil
}

var canonicalObservationFields = map[string]struct{}{
	"time": {}, "method": {}, "path": {}, "contentLength": {}, "capability": {},
	"model": {}, "responseModel": {}, "providerRequestId": {}, "sequence": {}, "attempt": {},
	"batchItemCount": {}, "bodySHA256": {}, "inputSHA256": {}, "authorizationPresent": {},
	"authorizationScheme": {}, "outcome": {}, "status": {}, "canceled": {}, "durationMilliseconds": {},
}

func rejectDuplicateObservationFields(line string, sequence int) error {
	decoder := json.NewDecoder(strings.NewReader(line))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return fmt.Errorf("provider emulator observation %d is not a JSON object", sequence)
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("provider emulator observation %d has an invalid field", sequence)
		}
		name, ok := token.(string)
		if !ok {
			return fmt.Errorf("provider emulator observation %d has an invalid field name", sequence)
		}
		if _, canonical := canonicalObservationFields[name]; !canonical {
			return fmt.Errorf("provider emulator observation %d has a noncanonical field", sequence)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("provider emulator observation %d has a duplicate field", sequence)
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("provider emulator observation %d has an invalid field value", sequence)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return fmt.Errorf("provider emulator observation %d has an invalid object boundary", sequence)
	}
	return nil
}

func validateObservation(observation Observation, index, previousAttempt int) error {
	if observation.Sequence != index+1 {
		return fmt.Errorf("provider emulator observation %d has invalid sequence", index+1)
	}
	validProtocol := observation.Method == "POST" && ((observation.Path == "/v1/embeddings" && observation.Capability == "embedding") || (observation.Path == "/v1/chat/completions" && observation.Capability == "generation"))
	if !validProtocol {
		return fmt.Errorf("provider emulator observation %d has unexpected protocol envelope", observation.Sequence)
	}
	if observation.ContentLength <= 0 || !validObservationDigest(observation.BodySHA256) || !validObservationDigest(observation.InputSHA256) {
		return fmt.Errorf("provider emulator observation %d has incomplete bounded digest evidence", observation.Sequence)
	}
	if !validSafeProviderIdentity(observation.Model, 256) || !validSafeProviderIdentity(observation.ResponseModel, 256) || !validSafeProviderIdentity(observation.ProviderRequestID, 128) {
		return fmt.Errorf("provider emulator observation %d has unsafe response identity evidence", observation.Sequence)
	}
	if observation.AuthorizationScheme != "" && observation.AuthorizationScheme != "Bearer" && observation.AuthorizationScheme != "Basic" && observation.AuthorizationScheme != "Other" {
		return fmt.Errorf("provider emulator observation %d has unsafe authorization scheme evidence", observation.Sequence)
	}
	if observation.AuthorizationPresent != (observation.AuthorizationScheme != "") {
		return fmt.Errorf("provider emulator observation %d has inconsistent authorization evidence", observation.Sequence)
	}
	if observation.BatchItemCount <= 0 || observation.BatchItemCount > 1024 {
		return fmt.Errorf("provider emulator observation %d has invalid batch item count", observation.Sequence)
	}
	if observation.Attempt <= 0 || observation.Attempt != previousAttempt+1 {
		return fmt.Errorf("provider emulator observation %d has invalid attempt sequence", observation.Sequence)
	}
	switch observation.Outcome {
	case "success", "http-error", "refusal", "malformed-json", "truncated-json", "wrong-dimensions", "canceled":
	default:
		return fmt.Errorf("provider emulator observation %d has unsupported outcome", observation.Sequence)
	}
	if observation.Status < 100 || observation.Status > 599 {
		return fmt.Errorf("provider emulator observation %d has invalid status", observation.Sequence)
	}
	return nil
}

func validObservationDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func validSafeProviderIdentity(value string, maximum int) bool {
	if value == "" {
		return true
	}
	if len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '.', '_', ':', '/', '-':
			continue
		default:
			return false
		}
	}
	return true
}
