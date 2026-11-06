package events

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

// The emulator's attempt is cumulative by input digest, not a retry counter.
// Search and work may legitimately submit the same input independently.
func TestSemanticProviderEmulatorExactInputDistribution(t *testing.T) {
	cases := []struct {
		name       string
		inputs     string
		want       []any
		maximum    int
		firstFault string
		faultHTTP  int
		wantError  string
	}{
		{name: "seven controlled requests across two inputs", inputs: "aaabbba", want: []any{3, 4}, maximum: 4},
		{name: "three fault work and search requests", inputs: "aaa", want: []any{3}, maximum: 3, firstFault: "http-error", faultHTTP: 429},
		{name: "extra repeat of one input rejects distribution", inputs: "aaaaabb", want: []any{3, 4}, maximum: 4, wantError: "maximum attempts"},
		{name: "unexpected third input rejects distribution", inputs: "aaabbcc", want: []any{3, 4}, maximum: 4, wantError: "requests per input"},
		{name: "wrong groups at same total reject distribution", inputs: "aabbbbb", want: []any{3, 4}, maximum: 4, wantError: "maximum attempts"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			attempts := map[rune]int{}
			outcomes := map[string]any{"success": len(test.inputs)}
			statuses := map[string]any{"200": len(test.inputs)}
			if test.firstFault != "" {
				outcomes["success"], outcomes[test.firstFault] = len(test.inputs)-1, 1
				statuses["200"], statuses[strconv.Itoa(test.faultHTTP)] = len(test.inputs)-1, 1
			}
			var evidence strings.Builder
			for sequence, input := range test.inputs {
				attempts[input]++
				outcome, status := "success", 200
				if sequence == 0 && test.firstFault != "" {
					outcome, status = test.firstFault, test.faultHTTP
				}
				observation := emulator.Observation{
					Method: "POST", Path: "/v1/embeddings", ContentLength: 1, Capability: "embedding",
					Model: "embedding-v1", ResponseModel: "embedding-v1", Sequence: sequence + 1,
					Attempt: attempts[input], BatchItemCount: 1, BodySHA256: strings.Repeat("b", 64),
					InputSHA256: strings.Repeat(string(input), 64), Outcome: outcome, Status: status,
					ProviderRequestID: "safe-request-" + string(rune('a'+sequence)),
				}
				raw, err := json.Marshal(observation)
				if err != nil {
					t.Fatal(err)
				}
				evidence.Write(raw)
				evidence.WriteByte('\n')
			}
			result, err := executeSemanticProviderEmulatorAssert(context.Background(),
				&emulatorObservationDriver{output: evidence.String()}, env.Environment{}, map[string]any{
					"minimumRequests": len(test.inputs), "maximumRequests": len(test.inputs),
					"maximumAttemptsPerInput": test.maximum, "expectedRequestsPerInput": test.want,
					"requireSingleItem": true, "authorization": "absent",
					"expectedModels":   []any{"embedding-v1"},
					"expectedOutcomes": outcomes,
					"expectedStatuses": statuses,
				})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error=%v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.RequestCount != len(test.inputs) || !reflect.DeepEqual(result.RequestsPerInput, ints(test.want)) || result.MaximumAttempts != test.maximum {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func ints(values []any) []int {
	out := make([]int, len(values))
	for i, value := range values {
		out[i] = value.(int)
	}
	return out
}
