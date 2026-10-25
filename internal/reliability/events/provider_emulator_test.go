package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

func TestSemanticProviderEmulatorAssertRequiresExactSafeEvidence(t *testing.T) {
	observations := []emulator.Observation{
		{Time: time.Now().UTC(), Method: "POST", Path: "/v1/embeddings", ContentLength: 42, Capability: "embedding", Model: "embedding-v1", Sequence: 1, Attempt: 1, BatchItemCount: 1, BodySHA256: strings.Repeat("a", 64), InputSHA256: strings.Repeat("b", 64), Outcome: "http-error", Status: 429, ProviderRequestID: "retry-1"},
		{Time: time.Now().UTC(), Method: "POST", Path: "/v1/embeddings", ContentLength: 42, Capability: "embedding", Model: "embedding-v1", ResponseModel: "route-b", Sequence: 2, Attempt: 2, BatchItemCount: 1, BodySHA256: strings.Repeat("c", 64), InputSHA256: strings.Repeat("b", 64), Outcome: "success", Status: 200, ProviderRequestID: "success-2"},
	}
	var lines strings.Builder
	for _, observation := range observations {
		raw, _ := json.Marshal(observation)
		lines.Write(raw)
		lines.WriteByte('\n')
	}
	driver := &emulatorObservationDriver{output: lines.String()}
	result, err := executeSemanticProviderEmulatorAssert(context.Background(), driver, env.Environment{Metadata: map[string]string{"providerEmulator": "true"}}, map[string]any{
		"minimumRequests": 2, "maximumRequests": 2, "requireSingleItem": true, "authorization": "absent",
		"maximumAttemptsPerInput": 2, "expectedModels": []any{"embedding-v1"},
		"minimumResponseModels": map[string]any{"route-b": 1}, "requiredProviderRequestIds": []any{"retry-1", "success-2"},
		"expectedOutcomes": map[string]any{"http-error": 1, "success": 1}, "expectedStatuses": map[string]any{"429": 1, "200": 1},
	})
	if err != nil {
		t.Fatalf("executeSemanticProviderEmulatorAssert() error = %v", err)
	}
	if result.RequestCount != 2 || result.MaximumAttempts != 2 || result.MaximumBatchItems != 1 || result.AuthorizationAbsent != 2 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDecodeProviderEmulatorObservationsRejectsUnknownOrNonContiguousEvidence(t *testing.T) {
	if _, err := emulator.DecodeObservationEvidence(`{"sequence":1,"unexpected":"leak"}`); err == nil {
		t.Fatal("DecodeObservationEvidence() accepted unknown field")
	}
	if _, err := emulator.DecodeObservationEvidence(`{"sequence":1} {}`); err == nil {
		t.Fatal("DecodeObservationEvidence() accepted trailing JSON")
	}
	observation := emulator.Observation{Method: "POST", Path: "/v1/embeddings", ContentLength: 1, Capability: "embedding", Model: "m", Sequence: 2, Attempt: 1, BatchItemCount: 1, BodySHA256: strings.Repeat("a", 64), InputSHA256: strings.Repeat("b", 64), Outcome: "success", Status: 200}
	raw, _ := json.Marshal(observation)
	driver := &emulatorObservationDriver{output: string(raw)}
	if _, err := executeSemanticProviderEmulatorAssert(context.Background(), driver, env.Environment{}, map[string]any{}); err == nil || !strings.Contains(err.Error(), "sequence") {
		t.Fatalf("executeSemanticProviderEmulatorAssert() error = %v", err)
	}
}

func TestProviderEmulatorObservationsAreValidatedInAssignedSequenceOrder(t *testing.T) {
	first := emulator.Observation{Method: "POST", Path: "/v1/embeddings", ContentLength: 1, Capability: "embedding", Model: "m", Sequence: 1, Attempt: 1, BatchItemCount: 1, BodySHA256: strings.Repeat("a", 64), InputSHA256: strings.Repeat("b", 64), Outcome: "success", Status: 200}
	second := first
	second.Sequence = 2
	second.Attempt = 2
	second.BodySHA256 = strings.Repeat("c", 64)
	firstRaw, _ := json.Marshal(first)
	secondRaw, _ := json.Marshal(second)
	driver := &emulatorObservationDriver{output: string(secondRaw) + "\n" + string(firstRaw) + "\n"}
	result, err := executeSemanticProviderEmulatorAssert(context.Background(), driver, env.Environment{}, map[string]any{"minimumRequests": 2, "maximumRequests": 2})
	if err != nil {
		t.Fatalf("executeSemanticProviderEmulatorAssert() error = %v", err)
	}
	if result.FirstSequence != 1 || result.LastSequence != 2 || result.MaximumAttempts != 2 {
		t.Fatalf("result = %+v", result)
	}
}

type emulatorObservationDriver struct {
	recordingDriver
	output string
}

func (d *emulatorObservationDriver) ProviderEmulatorObservations(context.Context, env.Environment) (env.ExecResult, error) {
	return env.ExecResult{Stdout: d.output}, nil
}
